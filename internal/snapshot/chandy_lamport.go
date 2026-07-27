// Package snapshot implements the Chandy-Lamport global snapshot algorithm
// (Chandy & Lamport 1985) with real FIFO channel recording and marker propagation.
package snapshot

import (
	"fmt"
	"sync"
	"time"
)

// SnapshotRole tracks a process's role in an ongoing snapshot.
type SnapshotRole int

const (
	NotParticipating SnapshotRole = iota // process has not yet taken part in this snapshot
	Initiator                            // sent markers, recording all incoming channels
	Participant                          // received first marker, sent markers, recording remaining channels
	Done                                 // all channels finalized
)

// ChannelState records messages captured on a channel during the snapshot.
type ChannelState struct {
	From      string
	Messages  []*Message // in-transit messages captured on this channel
	Finalized bool
}

// Message is a simplified message for snapshot purposes.
type Message struct {
	ID         string
	From       string
	To         string
	Data       interface{}
	IsMarker   bool
	SnapshotID string
	// SenderVC is the sender's vector clock at the time the message was
	// sent. Used by Verify to check the Chandy-Lamport cut property:
	// the sender's snapshot must reflect that the message has been sent,
	// i.e. its own counter is >= SenderVC[From].
	SenderVC map[string]uint64
}

// LocalSnapshot is a process's contribution to the global snapshot.
type LocalSnapshot struct {
	ProcessID     string
	LocalState    interface{}              // process state at snapshot time (VC, KV, etc.)
	RecordedChans map[string]*ChannelState // from→ channel state
	Finalized     bool
	SnapshotID    string
	SnapshotTime  time.Time
}

// GlobalSnapshot aggregates all local snapshots.
type GlobalSnapshot struct {
	SnapshotID  string
	InitiatorID string
	StartTime   time.Time
	LocalStates map[string]*LocalSnapshot
	Complete    bool
	mu          sync.RWMutex
}

// RLockForTest exposes the read lock for test assertions.
func (gs *GlobalSnapshot) RLockForTest() { gs.mu.RLock() }

// RUnlockForTest exposes the read unlock for test assertions.
func (gs *GlobalSnapshot) RUnlockForTest() { gs.mu.RUnlock() }

// Done returns whether the snapshot is complete (thread-safe).
func (gs *GlobalSnapshot) Done() bool {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.Complete
}

// IsComplete returns true when all processes have reported and all channels finalized.
func (gs *GlobalSnapshot) IsComplete(expectedProcesses []string) bool {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	for _, pid := range expectedProcesses {
		ls, ok := gs.LocalStates[pid]
		if !ok || !ls.Finalized {
			return false
		}
	}
	return true
}

// procSnapState is the per-snapshot state for a single process.
type procSnapState struct {
	role          SnapshotRole
	localState    interface{}
	recordedChans map[string]*ChannelState
}

// processSnapshot manages snapshot state for a process, supporting multiple concurrent snapshots.
type processSnapshot struct {
	mu      sync.Mutex
	peers   []string                  // all peers of this process
	perSnap map[string]*procSnapState // snapshotID → state
}

// SnapshotCoordinator manages multiple concurrent snapshots across all processes.
type SnapshotCoordinator struct {
	mu        sync.RWMutex
	snapshots map[string]*GlobalSnapshot

	// Per-process snapshot state
	procStates map[string]*processSnapshot

	// Callbacks
	onComplete     func(snapshotID string, gs *GlobalSnapshot)
	onSendMarker   func(from, to, snapshotID string)
	onCaptureState func(pid string) interface{}
}

// NewCoordinator creates a SnapshotCoordinator.
func NewCoordinator(
	onSendMarker func(from, to, snapshotID string),
	onCaptureState func(pid string) interface{},
	onComplete func(snapshotID string, gs *GlobalSnapshot),
) *SnapshotCoordinator {
	return &SnapshotCoordinator{
		snapshots:      make(map[string]*GlobalSnapshot),
		procStates:     make(map[string]*processSnapshot),
		onSendMarker:   onSendMarker,
		onCaptureState: onCaptureState,
		onComplete:     onComplete,
	}
}

// RegisterProcess registers a process with its peers for snapshot participation.
// It also updates all existing processes to include pid as a peer.
func (c *SnapshotCoordinator) RegisterProcess(pid string, peers []string) {
	c.mu.Lock()
	c.procStates[pid] = &processSnapshot{
		perSnap: make(map[string]*procSnapState),
		peers:   peers,
	}
	// Collect existing process states to update (avoid holding c.mu while locking ps.mu)
	toUpdate := make([]*processSnapshot, 0, len(c.procStates))
	for existingPID, ps := range c.procStates {
		if existingPID != pid {
			toUpdate = append(toUpdate, ps)
		}
	}
	c.mu.Unlock()

	// Update existing processes' peer lists without holding c.mu
	for _, ps := range toUpdate {
		ps.mu.Lock()
		found := false
		for _, p := range ps.peers {
			if p == pid {
				found = true
				break
			}
		}
		if !found {
			ps.peers = append(ps.peers, pid)
		}
		ps.mu.Unlock()
	}
}

// InitiateSnapshot starts a new snapshot from the given process.
// Returns the snapshot ID.
func (c *SnapshotCoordinator) InitiateSnapshot(initiatorID string) (string, error) {
	c.mu.Lock()
	snapshotID := fmt.Sprintf("snap-%d", time.Now().UnixNano())
	gs := &GlobalSnapshot{
		SnapshotID:  snapshotID,
		InitiatorID: initiatorID,
		StartTime:   time.Now(),
		LocalStates: make(map[string]*LocalSnapshot),
	}
	c.snapshots[snapshotID] = gs
	c.mu.Unlock()

	ps := c.getProcessState(initiatorID)
	if ps == nil {
		return "", fmt.Errorf("process %s not registered", initiatorID)
	}

	// Capture initiator state BEFORE acquiring ps.mu. onCaptureState calls
	// back into the process (p.Snapshot → p.mu.RLock). If we held ps.mu here,
	// we would deadlock with handleMessage which holds p.mu.Lock and then tries
	// to acquire ps.mu inside OnMarkerReceived.
	localState := c.onCaptureState(initiatorID)

	ps.mu.Lock()
	state := &procSnapState{
		role:          Initiator,
		localState:    localState,
		recordedChans: make(map[string]*ChannelState),
	}
	for _, peer := range ps.peers {
		state.recordedChans[peer] = &ChannelState{From: peer}
	}
	ps.perSnap[snapshotID] = state
	peers := ps.peers
	ps.mu.Unlock()

	// Send markers to all outgoing channels
	for _, peer := range peers {
		c.onSendMarker(initiatorID, peer, snapshotID)
	}

	return snapshotID, nil
}

// OnMarkerReceived processes a received marker message at a process.
// capturedState is the process's state captured atomically at the instant the
// marker arrived (before any subsequent clock tick). Passing it in avoids a
// second p.Snapshot() call inside the coordinator, which would race with
// concurrent sends on the process (and would also re-acquire p.mu, deadlocking
// when called from handleMessage which already holds p.mu.Lock).
func (c *SnapshotCoordinator) OnMarkerReceived(pid, from, snapshotID string, capturedState interface{}) {
	ps := c.getProcessState(pid)
	if ps == nil {
		return
	}

	ps.mu.Lock()

	state, exists := ps.perSnap[snapshotID]
	if !exists {
		state = &procSnapState{
			role:          NotParticipating,
			recordedChans: make(map[string]*ChannelState),
		}
		ps.perSnap[snapshotID] = state
	}

	switch state.role {
	case NotParticipating:
		// FIRST marker: record state, forward markers, mark this channel empty
		state.role = Participant
		state.localState = capturedState // use pre-captured state; never call onCaptureState here

		// Channel from sender is empty (no messages between snapshot points)
		state.recordedChans[from] = &ChannelState{From: from, Finalized: true}

		// Start recording all other incoming channels
		for _, peer := range ps.peers {
			if peer != from {
				if state.recordedChans[peer] == nil {
					state.recordedChans[peer] = &ChannelState{From: peer}
				}
			}
		}

		peers := ps.peers
		ps.mu.Unlock()

		// Forward markers on ALL outgoing channels (Chandy-Lamport §4).
		// This includes the channel back to the sender: the P_j→P_i directed
		// channel is independent of P_i→P_j. P_i needs P_j's marker on
		// P_j→P_i to finalize that channel in P_i's own RecordedChans.
		for _, peer := range peers {
			c.onSendMarker(pid, peer, snapshotID)
		}

		c.checkFinalized(snapshotID, pid)

	case Participant, Initiator:
		// SUBSEQUENT marker: finalize this channel
		if cs, ok := state.recordedChans[from]; ok {
			cs.Finalized = true
		} else {
			state.recordedChans[from] = &ChannelState{From: from, Finalized: true}
		}

		// Check if all channels done
		allDone := true
		for _, peer := range ps.peers {
			if cs := state.recordedChans[peer]; cs == nil || !cs.Finalized {
				allDone = false
				break
			}
		}
		if allDone {
			state.role = Done
		}
		ps.mu.Unlock()

		c.checkFinalized(snapshotID, pid)

	default:
		ps.mu.Unlock()
	}
}

// RecordMessage buffers a non-marker message on a channel being recorded.
func (c *SnapshotCoordinator) RecordMessage(snapshotID, pid, from string, m *Message) {
	ps := c.getProcessState(pid)
	if ps == nil {
		return
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	state, ok := ps.perSnap[snapshotID]
	if !ok {
		return
	}
	cs := state.recordedChans[from]
	if cs == nil || cs.Finalized {
		return // channel not being recorded or already done
	}
	cs.Messages = append(cs.Messages, m)
}

// GetSnapshot returns a completed global snapshot.
func (c *SnapshotCoordinator) GetSnapshot(snapshotID string) *GlobalSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshots[snapshotID]
}

// SnapshotDTO is a deep-copied, JSON-safe view of a snapshot.
// GlobalSnapshot contains a RWMutex and a map that is concurrently
// mutated by the coordinator. Serialize only the DTO to avoid the
// JSON encoder racing with the coordinator.
type SnapshotDTO struct {
	SnapshotID  string                       `json:"snapshotId"`
	InitiatorID string                       `json:"initiatorId"`
	StartTime   time.Time                    `json:"startTime"`
	Complete    bool                         `json:"complete"`
	LocalStates map[string]*LocalSnapshotDTO `json:"localStates"`
}

// LocalSnapshotDTO is the JSON-safe view of a single process's snapshot.
type LocalSnapshotDTO struct {
	ProcessID     string                 `json:"processId"`
	LocalState    interface{}            `json:"localState"`
	RecordedChans map[string]*ChannelDTO `json:"recordedChans"`
	Finalized     bool                   `json:"finalized"`
	SnapshotID    string                 `json:"snapshotId"`
	SnapshotTime  time.Time              `json:"snapshotTime"`
}

// ChannelDTO is the JSON-safe view of a channel state.
type ChannelDTO struct {
	From      string     `json:"from"`
	Messages  []*Message `json:"messages"`
	Finalized bool       `json:"finalized"`
}

// ToDTO returns a deep-copied, lock-free JSON-safe view of this snapshot.
// The coordinator's lock is held for the duration of the copy so the
// returned DTO is safe to serialize without further synchronization.
func (gs *GlobalSnapshot) ToDTO() *SnapshotDTO {
	if gs == nil {
		return nil
	}
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	dto := &SnapshotDTO{
		SnapshotID:  gs.SnapshotID,
		InitiatorID: gs.InitiatorID,
		StartTime:   gs.StartTime,
		Complete:    gs.Complete,
		LocalStates: make(map[string]*LocalSnapshotDTO, len(gs.LocalStates)),
	}
	for pid, ls := range gs.LocalStates {
		dto.LocalStates[pid] = ls.toDTO()
	}
	return dto
}

func (ls *LocalSnapshot) toDTO() *LocalSnapshotDTO {
	if ls == nil {
		return nil
	}
	chans := make(map[string]*ChannelDTO, len(ls.RecordedChans))
	for k, v := range ls.RecordedChans {
		msgs := append([]*Message(nil), v.Messages...)
		chans[k] = &ChannelDTO{From: v.From, Messages: msgs, Finalized: v.Finalized}
	}
	return &LocalSnapshotDTO{
		ProcessID:     ls.ProcessID,
		LocalState:    ls.LocalState,
		RecordedChans: chans,
		Finalized:     ls.Finalized,
		SnapshotID:    ls.SnapshotID,
		SnapshotTime:  ls.SnapshotTime,
	}
}

// checkFinalized assembles the local snapshot and checks if the global snapshot is complete.
func (c *SnapshotCoordinator) checkFinalized(snapshotID, pid string) {
	ps := c.getProcessState(pid)
	if ps == nil {
		return
	}

	ps.mu.Lock()
	state, snapExists := ps.perSnap[snapshotID]
	if !snapExists {
		ps.mu.Unlock()
		return
	}
	if state.role != Done {
		// Check if all channels are finalized. The initiator participates
		// too — its recordedChans are populated for every peer at
		// InitiateSnapshot time, and each peer's marker back to the
		// initiator finalizes one of them.
		allDone := true
		for _, peer := range ps.peers {
			if cs := state.recordedChans[peer]; cs == nil || !cs.Finalized {
				allDone = false
				break
			}
		}
		if !allDone {
			ps.mu.Unlock()
			return
		}
		state.role = Done
	}

	ls := &LocalSnapshot{
		ProcessID:     pid,
		LocalState:    state.localState,
		RecordedChans: copyChannelStates(state.recordedChans),
		Finalized:     true,
		SnapshotID:    snapshotID,
		SnapshotTime:  time.Now(),
	}
	ps.mu.Unlock()

	c.mu.Lock()
	gs := c.snapshots[snapshotID]
	if gs == nil {
		c.mu.Unlock()
		return
	}
	gs.mu.Lock()
	gs.LocalStates[pid] = ls
	gs.mu.Unlock()

	// Check if all registered processes have finalized
	allPIDs := make([]string, 0, len(c.procStates))
	for p := range c.procStates {
		allPIDs = append(allPIDs, p)
	}
	c.mu.Unlock()

	if gs.IsComplete(allPIDs) {
		gs.mu.Lock()
		gs.Complete = true
		gs.mu.Unlock()
		if c.onComplete != nil {
			c.onComplete(snapshotID, gs)
		}
	}
}

// DeregisterProcess removes a process from the coordinator so that ongoing
// snapshots no longer wait for its contribution. Peer lists of remaining
// processes are updated to exclude the removed process.
// Lock order matches RegisterProcess: hold c.mu to mutate procStates, release
// it, then lock each ps.mu individually (never nest c.mu inside ps.mu).
func (c *SnapshotCoordinator) DeregisterProcess(pid string) {
	c.mu.Lock()
	delete(c.procStates, pid)
	toUpdate := make([]*processSnapshot, 0, len(c.procStates))
	for _, ps := range c.procStates {
		toUpdate = append(toUpdate, ps)
	}
	c.mu.Unlock()

	for _, ps := range toUpdate {
		ps.mu.Lock()
		filtered := ps.peers[:0:0]
		for _, p := range ps.peers {
			if p != pid {
				filtered = append(filtered, p)
			}
		}
		ps.peers = filtered
		ps.mu.Unlock()
	}
}

func (c *SnapshotCoordinator) getProcessState(pid string) *processSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.procStates[pid]
}

// GetActiveSnapshotIDs returns all snapshot IDs currently active for pid.
func (c *SnapshotCoordinator) GetActiveSnapshotIDs(pid string) []string {
	ps := c.getProcessState(pid)
	if ps == nil {
		return nil
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	var ids []string
	for sid, state := range ps.perSnap {
		if state.role != NotParticipating && state.role != Done {
			ids = append(ids, sid)
		}
	}
	return ids
}

func copyChannelStates(m map[string]*ChannelState) map[string]*ChannelState {
	result := make(map[string]*ChannelState, len(m))
	for k, v := range m {
		msgs := make([]*Message, len(v.Messages))
		copy(msgs, v.Messages)
		result[k] = &ChannelState{From: v.From, Messages: msgs, Finalized: v.Finalized}
	}
	return result
}
