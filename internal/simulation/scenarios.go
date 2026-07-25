package simulation

import (
	"time"

	"github.com/DistributedClocks/vectorclock-system/internal/process"
)

// ScenarioStep is a single step in a pre-built scenario.
type ScenarioStep struct {
	Delay   time.Duration
	Action  func(*Simulation) error
	Narrate string
}

// Scenario is a pre-built simulation scenario.
type Scenario struct {
	Name        string
	Description string
	Steps       []ScenarioStep
}

// AllScenarios returns all available scenarios.
func AllScenarios() []Scenario {
	return []Scenario{
		basicLamport(),
		concurrentWrites(),
		causalViolation(),
		causalDelivery(),
		snapshot3P(),
		falseConflict(),
		matrixGC(),
		partitionAndHeal(),
	}
}

// falseConflict demonstrates a false conflict in plain vector clocks:
// P1 writes key "x" (v1), P2 reads (replication) then writes "x" with
// context {P1:1}. In plain VVs this is a false conflict; DVV resolves it.
func falseConflict() Scenario {
	return Scenario{
		Name:        "FalseConflict",
		Description: "VV false conflict: P1->P2 replication triggers false conflict in plain VCs but not DVV",
		Steps: []ScenarioStep{
			{
				Narrate: "Spawning P1, P2 with Vector clocks",
				Action: func(s *Simulation) error {
					for _, id := range []string{"P1", "P2"} {
						if err := s.SpawnProcess(process.ProcessConfig{
							ID:        id,
							ClockType: process.ClockVector,
						}); err != nil {
							return err
						}
					}
					return nil
				},
			},
			{Delay: 50 * time.Millisecond, Narrate: "P1 writes key x = v1", Action: func(s *Simulation) error {
				return s.InternalEvent("P1")
			}},
			{Delay: 50 * time.Millisecond, Narrate: "P1 sends to P2 (replication)", Action: func(s *Simulation) error {
				return s.SendMessage("P1", "P2", "replicate: x=v1")
			}},
			{Delay: 100 * time.Millisecond, Narrate: "P2 writes key x = v2 (causally after v1)", Action: func(s *Simulation) error {
				return s.InternalEvent("P2")
			}},
			{Delay: 50 * time.Millisecond, Narrate: "P2 sends to P1 (replication back)", Action: func(s *Simulation) error {
				return s.SendMessage("P2", "P1", "replicate: x=v2")
			}},
		},
	}
}

// matrixGC demonstrates matrix clock garbage collection convergence.
// 4 processes exchange messages in a ring; MinKnowledge grows.
func matrixGC() Scenario {
	return Scenario{
		Name:        "MatrixGC",
		Description: "4-process matrix clock: MinKnowledge grows through exchange rounds",
		Steps: []ScenarioStep{
			{
				Narrate: "Spawning P1-P4 with Matrix clocks",
				Action: func(s *Simulation) error {
					for _, id := range []string{"P1", "P2", "P3", "P4"} {
						if err := s.SpawnProcess(process.ProcessConfig{
							ID:        id,
							ClockType: process.ClockMatrix,
						}); err != nil {
							return err
						}
					}
					return nil
				},
			},
			{Delay: 30 * time.Millisecond, Narrate: "Round 1: P1→P2, P2→P3, P3→P4, P4→P1", Action: func(s *Simulation) error {
				_ = s.SendMessage("P1", "P2", "r1")
				_ = s.SendMessage("P2", "P3", "r1")
				_ = s.SendMessage("P3", "P4", "r1")
				return s.SendMessage("P4", "P1", "r1")
			}},
			{Delay: 30 * time.Millisecond, Narrate: "Round 2: P1→P2, P2→P3, P3→P4, P4→P1", Action: func(s *Simulation) error {
				_ = s.SendMessage("P1", "P2", "r2")
				_ = s.SendMessage("P2", "P3", "r2")
				_ = s.SendMessage("P3", "P4", "r2")
				return s.SendMessage("P4", "P1", "r2")
			}},
			{Delay: 30 * time.Millisecond, Narrate: "Round 3: P1→P2, P2→P3, P3→P4, P4→P1", Action: func(s *Simulation) error {
				_ = s.SendMessage("P1", "P2", "r3")
				_ = s.SendMessage("P2", "P3", "r3")
				_ = s.SendMessage("P3", "P4", "r3")
				return s.SendMessage("P4", "P1", "r3")
			}},
		},
	}
}

// partitionAndHeal demonstrates network partition and convergence after heal.
// 4 processes: split into {P1,P2} and {P3,P4}, messages cross groups are
// dropped, then heal and verify convergence.
func partitionAndHeal() Scenario {
	return Scenario{
		Name:        "PartitionAndHeal",
		Description: "4 processes: partition {P1,P2}|{P3,P4}, isolate, heal, converge",
		Steps: []ScenarioStep{
			{
				Narrate: "Spawning P1-P4 with Vector clocks",
				Action: func(s *Simulation) error {
					for _, id := range []string{"P1", "P2", "P3", "P4"} {
						if err := s.SpawnProcess(process.ProcessConfig{
							ID:        id,
							ClockType: process.ClockVector,
						}); err != nil {
							return err
						}
					}
					return nil
				},
			},
			{Delay: 30 * time.Millisecond, Narrate: "Partition: {P1,P2} | {P3,P4} — cross-group messages dropped", Action: func(s *Simulation) error {
				s.GetTransport().SetPartition([][]string{{"P1", "P2"}, {"P3", "P4"}})
				return nil
			}},
			{Delay: 50 * time.Millisecond, Narrate: "P1→P3 (dropped by partition)", Action: func(s *Simulation) error {
				return s.SendMessage("P1", "P3", "cross-partition")
			}},
			{Delay: 30 * time.Millisecond, Narrate: "P1→P2 (in-group, delivered)", Action: func(s *Simulation) error {
				return s.SendMessage("P1", "P2", "in-partition")
			}},
			{Delay: 30 * time.Millisecond, Narrate: "P3→P4 (in-group, delivered)", Action: func(s *Simulation) error {
				return s.SendMessage("P3", "P4", "in-partition")
			}},
			{Delay: 50 * time.Millisecond, Narrate: "Heal: remove partition", Action: func(s *Simulation) error {
				s.GetTransport().SetPartition(nil)
				return nil
			}},
			{Delay: 50 * time.Millisecond, Narrate: "P1→P3 (now delivered after heal)", Action: func(s *Simulation) error {
				return s.SendMessage("P1", "P3", "post-heal")
			}},
		},
	}
}

// basicLamport: 3 processes, 5 events showing Lamport total order.
func basicLamport() Scenario {
	return Scenario{
		Name:        "BasicLamport",
		Description: "3 processes, 5 events demonstrating Lamport total order",
		Steps: []ScenarioStep{
			{
				Narrate: "Spawning P1, P2, P3 with Lamport clocks",
				Action: func(s *Simulation) error {
					for _, id := range []string{"P1", "P2", "P3"} {
						if err := s.SpawnProcess(process.ProcessConfig{
							ID:        id,
							ClockType: process.ClockLamport,
						}); err != nil {
							return err
						}
					}
					return nil
				},
			},
			{Delay: 100 * time.Millisecond, Narrate: "P1 internal event", Action: func(s *Simulation) error {
				return s.InternalEvent("P1")
			}},
			{Delay: 100 * time.Millisecond, Narrate: "P1 sends to P2", Action: func(s *Simulation) error {
				return s.SendMessage("P1", "P2", "hello")
			}},
			{Delay: 150 * time.Millisecond, Narrate: "P2 receives and sends to P3", Action: func(s *Simulation) error {
				return s.SendMessage("P2", "P3", "world")
			}},
			{Delay: 100 * time.Millisecond, Narrate: "P3 internal event", Action: func(s *Simulation) error {
				return s.InternalEvent("P3")
			}},
		},
	}
}

// concurrentWrites: 2 processes write concurrently — VC detects ∥.
func concurrentWrites() Scenario {
	return Scenario{
		Name:        "ConcurrentWrites",
		Description: "2 processes write concurrently — vector clock detects concurrent events",
		Steps: []ScenarioStep{
			{
				Narrate: "Spawning P1, P2 with Vector clocks",
				Action: func(s *Simulation) error {
					for _, id := range []string{"P1", "P2"} {
						if err := s.SpawnProcess(process.ProcessConfig{
							ID:        id,
							ClockType: process.ClockVector,
						}); err != nil {
							return err
						}
					}
					return nil
				},
			},
			{Delay: 100 * time.Millisecond, Narrate: "P1 internal event (concurrent with P2)", Action: func(s *Simulation) error {
				return s.InternalEvent("P1")
			}},
			{Delay: 0, Narrate: "P2 internal event (concurrent with P1)", Action: func(s *Simulation) error {
				return s.InternalEvent("P2")
			}},
		},
	}
}

// causalViolation: immediate delivery shows out-of-order receive.
func causalViolation() Scenario {
	return Scenario{
		Name:        "CausalViolation",
		Description: "Immediate delivery shows causal violation: P3 gets reply before question",
		Steps: []ScenarioStep{
			{
				Narrate: "Spawning with IMMEDIATE delivery (no hold-back queue)",
				Action: func(s *Simulation) error {
					for _, id := range []string{"P1", "P2", "P3"} {
						if err := s.SpawnProcess(process.ProcessConfig{
							ID:           id,
							ClockType:    process.ClockVector,
							DeliveryMode: process.ImmediateDelivery,
						}); err != nil {
							return err
						}
					}
					return nil
				},
			},
			{Delay: 50 * time.Millisecond, Narrate: "P1 sends question to P2", Action: func(s *Simulation) error {
				return s.SendMessage("P1", "P2", "question")
			}},
			{Delay: 50 * time.Millisecond, Narrate: "P2 replies to P3 (causally after question)", Action: func(s *Simulation) error {
				return s.SendMessage("P2", "P3", "answer")
			}},
		},
	}
}

// causalDelivery: hold-back queue enforces correct order.
func causalDelivery() Scenario {
	return Scenario{
		Name:        "CausalDelivery",
		Description: "Causal delivery: hold-back queue ensures P3 gets question before answer",
		Steps: []ScenarioStep{
			{
				Narrate: "Spawning with CAUSAL delivery (hold-back queue active)",
				Action: func(s *Simulation) error {
					for _, id := range []string{"P1", "P2", "P3"} {
						if err := s.SpawnProcess(process.ProcessConfig{
							ID:           id,
							ClockType:    process.ClockVector,
							DeliveryMode: process.CausalDelivery,
						}); err != nil {
							return err
						}
					}
					return nil
				},
			},
			{Delay: 50 * time.Millisecond, Narrate: "P1 sends question to P2", Action: func(s *Simulation) error {
				return s.SendMessage("P1", "P2", "question")
			}},
			{Delay: 50 * time.Millisecond, Narrate: "P2 replies to P3", Action: func(s *Simulation) error {
				return s.SendMessage("P2", "P3", "answer")
			}},
		},
	}
}

// snapshot3P: 3-process Chandy-Lamport step by step.
func snapshot3P() Scenario {
	return Scenario{
		Name:        "Snapshot3P",
		Description: "3-process Chandy-Lamport: marker propagation animated step by step",
		Steps: []ScenarioStep{
			{
				Narrate: "Spawning 3 processes with vector clocks",
				Action: func(s *Simulation) error {
					for _, id := range []string{"P1", "P2", "P3"} {
						if err := s.SpawnProcess(process.ProcessConfig{
							ID:        id,
							ClockType: process.ClockVector,
						}); err != nil {
							return err
						}
					}
					return nil
				},
			},
			{Delay: 100 * time.Millisecond, Narrate: "P1 sends to P2 (message in flight)", Action: func(s *Simulation) error {
				return s.SendMessage("P1", "P2", "data")
			}},
			{Delay: 50 * time.Millisecond, Narrate: "P1 initiates global snapshot", Action: func(s *Simulation) error {
				_, err := s.TriggerSnapshot("P1")
				return err
			}},
		},
	}
}
