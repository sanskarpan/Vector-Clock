package simulation

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/DistributedClocks/vectorclock-system/internal/causality"
	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
	"github.com/DistributedClocks/vectorclock-system/internal/events"
	"github.com/DistributedClocks/vectorclock-system/internal/process"
	"github.com/DistributedClocks/vectorclock-system/internal/snapshot"
)

// SimConfig is the top-level simulation configuration.
type SimConfig struct {
	ClockType    process.ClockType
	DeliveryMode process.DeliveryMode
	Channels     string // "full_mesh" | "ring"
}

// SimulationState is the full serializable state for REST/WebSocket.
type SimulationState struct {
	Processes  []process.ProcessState `json:"processes"`
	EventCount int                    `json:"eventCount"`
	Config     SimConfig              `json:"config"`
}

// Simulation orchestrates N processes with a shared transport and event bus.
//
// Concurrency: sim.mu protects the process map. Bus and transport are
// independently thread-safe.
type Simulation struct {
	mu          sync.RWMutex
	processes   map[string]*process.Process
	transport   *SimTransport
	bus         *events.EventBus
	snapshots   *snapshot.SnapshotCoordinator
	config      SimConfig
	eventCount  atomic.Int64
	causalGraph *causality.CausalGraph

	log *zap.Logger
}

// New creates a Simulation with the given config. Unsupported channel
// topologies are normalised to full_mesh (logged as a warning when a logger
// is provided via SetLogger).
func New(cfg SimConfig) *Simulation {
	if cfg.Channels != "" && cfg.Channels != "full_mesh" {
		// Logged at the gateway layer; here we silently normalise.
		cfg.Channels = "full_mesh"
	}
	bus := events.NewEventBus()
	xport := NewSimTransport()

	cg := causality.NewCausalGraph()

	s := &Simulation{
		processes:   make(map[string]*process.Process),
		transport:   xport,
		bus:         bus,
		config:      cfg,
		causalGraph: cg,
	}

	s.snapshots = snapshot.NewCoordinator(
		func(from, to, snapshotID string) {
			marker := &process.Message{
				ID:         fmt.Sprintf("marker-%s-%s-%s", snapshotID, from, to),
				From:       from,
				To:         to,
				IsMarker:   true,
				SnapshotID: snapshotID,
			}
			if err := xport.Send(from, to, marker); err != nil {
				s.logSendFailure("snapshot marker send", from, to, err)
			}
			bus.Publish(events.Event{
				Type:      events.EvtMarkerSent,
				ProcessID: from,
				GlobalSeq: bus.NextSeq(),
				Snapshot:  &events.SnapshotPayload{SnapshotID: snapshotID, ProcessID: from},
			})
		},
		func(pid string) interface{} {
			s.mu.RLock()
			defer s.mu.RUnlock()
			if p, ok := s.processes[pid]; ok {
				return p.Snapshot()
			}
			return nil
		},
		func(snapshotID string, gs *snapshot.GlobalSnapshot) {
			bus.Publish(events.Event{
				Type:      events.EvtSnapshotComplete,
				GlobalSeq: bus.NextSeq(),
				Snapshot:  &events.SnapshotPayload{SnapshotID: snapshotID, Complete: true},
			})
		},
	)

	cgCh := bus.Subscribe([]events.EventType{
		events.EvtSend, events.EvtReceive, events.EvtInternalEvent,
	})
	go s.handleCausalGraphEvents(cgCh)
	return s
}

// SetLogger attaches a structured logger. Optional but recommended.
func (s *Simulation) SetLogger(l *zap.Logger) { s.log = l }

func (s *Simulation) logSendFailure(op, from, to string, err error) {
	if s.log == nil {
		return
	}
	s.log.Warn(op+" failed",
		zap.String("from", from),
		zap.String("to", to),
		zap.Error(err))
}

// Bus returns the shared event bus.
func (s *Simulation) Bus() *events.EventBus { return s.bus }

// GetCausalGraph returns the internally-maintained causal graph.
// The graph is populated from bus events emitted by processes.
func (s *Simulation) GetCausalGraph() *causality.CausalGraph {
	return s.causalGraph
}

// handleCausalGraphEvents subscribes to bus events and populates the
// causal graph with send/receive/internal events.
func (s *Simulation) handleCausalGraphEvents(ch chan events.Event) {
	defer func() {
		if r := recover(); r != nil {
			if s.log != nil {
				s.log.Error("handleCausalGraphEvents: panic recovered",
					zap.Any("panic", r))
			}
		}
	}()
	for {
		select {
		case e, ok := <-ch:
			if !ok {
				return
			}
			ev := &causality.Event{
				ID:        e.ID,
				ProcessID: e.ProcessID,
				LocalSeq:  e.LocalSeq,
			}
			switch e.Type {
			case events.EvtInternalEvent:
				ev.Type = causality.EventInternal
			case events.EvtSend:
				ev.Type = causality.EventSend
				if e.Message != nil {
					ev.MessageID = e.Message.ID
				}
			case events.EvtReceive:
				ev.Type = causality.EventReceive
				if e.Message != nil {
					ev.MessageID = e.Message.ID
				}
			}
			s.causalGraph.AddEvent(ev)
			if e.Type == events.EvtReceive && e.Message != nil && !e.Message.IsMarker {
				s.causalGraph.AddEdge(e.Message.ID+"-send", e.ID)
			}
		case <-s.bus.Done():
			return
		}
	}
}

// SpawnProcess creates and starts a new process.
//
// Enforces MaxSimulationProcesses atomically: a SpawnProcess that would
// exceed the limit returns an error. Without this, an unauthenticated
// caller could exhaust memory and goroutines by spawning thousands of
// processes (each backed by an O(N²) matrix clock).
func (s *Simulation) SpawnProcess(cfg process.ProcessConfig) error {
	if cfg.ID == "" {
		return fmt.Errorf("process id is required")
	}
	if len(cfg.ID) > 64 {
		return fmt.Errorf("process id too long")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.processes[cfg.ID]; exists {
		return fmt.Errorf("process %s already exists", cfg.ID)
	}
	if len(s.processes) >= MaxSimulationProcesses {
		return fmt.Errorf("process limit reached (max %d); reset simulation to add more",
			MaxSimulationProcesses)
	}

	if cfg.ClockType == "" {
		cfg.ClockType = s.config.ClockType
	}
	if cfg.DeliveryMode == "" {
		cfg.DeliveryMode = s.config.DeliveryMode
	}

	allPIDs := make([]string, 0, len(s.processes)+1)
	for pid := range s.processes {
		allPIDs = append(allPIDs, pid)
	}
	allPIDs = append(allPIDs, cfg.ID)
	cfg.AllPIDs = allPIDs

	p := process.New(cfg, s.transport, s.bus)
	s.processes[cfg.ID] = p

	pid := cfg.ID

	// Wire the synchronous marker callback. The snapshot coordinator is
	// notified with the process's state captured AT the instant of marker
	// arrival, before any subsequent message processing — this is what
	// makes the Chandy-Lamport cut property (Chandy & Lamport 1985) hold.
	p.OnMarker = func(from, snapshotID string, localState process.ProcessState) {
		s.snapshots.OnMarkerReceived(pid, from, snapshotID, localState)
	}

	s.transport.RegisterProcess(pid, func(m *process.Message) {
		if !m.IsMarker {
			// Record the message for ALL active snapshots where the channel
			// is being recorded (each snapshot has independent channel state).
			for _, snapID := range s.snapshots.GetActiveSnapshotIDs(pid) {
				senderVC := vector.Copy(m.SentVC)
				s.snapshots.RecordMessage(snapID, pid, m.From, &snapshot.Message{
					ID:       m.ID,
					From:     m.From,
					To:       m.To,
					Data:     m.Data,
					SenderVC: senderVC,
				})
			}
		}
		p.Deliver(m)
	})

	for pid := range s.processes {
		if pid == cfg.ID {
			continue
		}
		s.transport.RegisterChannel(cfg.ID, pid)
		s.transport.RegisterChannel(pid, cfg.ID)
	}

	peers := make([]string, 0, len(s.processes)-1)
	for pid := range s.processes {
		if pid != cfg.ID {
			peers = append(peers, pid)
		}
	}
	s.snapshots.RegisterProcess(cfg.ID, peers)

	for pid, ep := range s.processes {
		if pid != cfg.ID {
			ep.AddPeer(cfg.ID)
		}
	}

	p.Start()
	return nil
}

// KillProcess stops and removes a process.
func (s *Simulation) KillProcess(id string) error {
	s.mu.Lock()
	p, ok := s.processes[id]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("process %s not found", id)
	}
	delete(s.processes, id)
	s.mu.Unlock()
	// Deregister before stopping so any active snapshot can still complete
	// without waiting forever for a process that will never report.
	s.snapshots.DeregisterProcess(id)
	p.Stop()
	return nil
}

// SendMessage sends a message between two processes.
func (s *Simulation) SendMessage(from, to string, data interface{}) error {
	s.mu.RLock()
	p, ok := s.processes[from]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("process %s not found", from)
	}
	return p.Send(to, data)
}

// BroadcastMessage broadcasts a message from a process to all peers.
func (s *Simulation) BroadcastMessage(from string, data interface{}) error {
	s.mu.RLock()
	p, ok := s.processes[from]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("process %s not found", from)
	}
	return p.Broadcast(data)
}

// InternalEvent triggers a local event on a process.
func (s *Simulation) InternalEvent(pid string) error {
	s.mu.RLock()
	p, ok := s.processes[pid]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("process %s not found", pid)
	}
	p.InternalEvent()
	return nil
}

// TriggerSnapshot initiates a Chandy-Lamport snapshot from a process.
func (s *Simulation) TriggerSnapshot(initiatorID string) (string, error) {
	s.mu.RLock()
	_, ok := s.processes[initiatorID]
	s.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("process %s not found", initiatorID)
	}

	snapID, err := s.snapshots.InitiateSnapshot(initiatorID)
	if err != nil {
		return "", fmt.Errorf("snapshot: %w", err)
	}
	s.bus.Publish(events.Event{
		Type:      events.EvtSnapshotStart,
		ProcessID: initiatorID,
		GlobalSeq: s.bus.NextSeq(),
		Snapshot:  &events.SnapshotPayload{SnapshotID: snapID, ProcessID: initiatorID},
	})
	return snapID, nil
}

// GetSnapshot returns a completed snapshot.
func (s *Simulation) GetSnapshot(snapID string) *snapshot.GlobalSnapshot {
	return s.snapshots.GetSnapshot(snapID)
}

// GetState returns the full serializable simulation state.
func (s *Simulation) GetState() SimulationState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state := SimulationState{
		Processes:  make([]process.ProcessState, 0, len(s.processes)),
		EventCount: int(s.eventCount.Load()),
		Config:     s.config,
	}
	for _, p := range s.processes {
		state.Processes = append(state.Processes, p.Snapshot())
	}
	return state
}

// GetProcess returns a process by ID.
func (s *Simulation) GetProcess(pid string) *process.Process {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.processes[pid]
}

// InjectDelay sets message delivery delay on a channel.
func (s *Simulation) InjectDelay(from, to string, delayMs int64) {
	s.transport.InjectDelay(from, to, time.Duration(delayMs)*time.Millisecond)
}

// GetTransport exposes the transport for fault injection.
func (s *Simulation) GetTransport() *SimTransport { return s.transport }

// InjectDrop causes the next message on channel from→to to be dropped.
func (s *Simulation) InjectDrop(from, to string) {
	s.transport.InjectDrop(from, to)
}

// SetDeliveryMode updates the global config and propagates to all running processes.
func (s *Simulation) SetDeliveryMode(mode process.DeliveryMode) {
	s.mu.Lock()
	s.config.DeliveryMode = mode
	procs := make([]*process.Process, 0, len(s.processes))
	for _, p := range s.processes {
		procs = append(procs, p)
	}
	s.mu.Unlock()
	for _, p := range procs {
		p.SetDeliveryMode(mode)
	}
}

// SetPartition creates a network partition between groups. Pass nil to heal.
func (s *Simulation) SetPartition(groups [][]string) {
	s.transport.SetPartition(groups)
}

// Stop shuts down the simulation: stops each process, drains transport
// forward goroutines, and stops the event bus. Idempotent.
func (s *Simulation) Stop() {
	s.mu.Lock()
	procs := make([]*process.Process, 0, len(s.processes))
	for _, p := range s.processes {
		procs = append(procs, p)
	}
	s.mu.Unlock()
	for _, p := range procs {
		p.Stop()
	}
	s.bus.Stop()
	s.transport.Close()
}
