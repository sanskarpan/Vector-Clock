package process

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/lamport"
	"github.com/DistributedClocks/vectorclock-system/internal/clock/matrix"
	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
	"github.com/DistributedClocks/vectorclock-system/internal/events"
)

// ClockType selects which clock algorithm the process uses.
type ClockType string

const (
	ClockLamport ClockType = "lamport"
	ClockVector  ClockType = "vector"
	ClockMatrix  ClockType = "matrix"
)

// DeliveryMode controls how messages are delivered.
type DeliveryMode string

const (
	ImmediateDelivery  DeliveryMode = "immediate"
	CausalDelivery     DeliveryMode = "causal"
	TotalOrderDelivery DeliveryMode = "total_order"
)

// ProcessStatus is the lifecycle state of a process.
type ProcessStatus string

const (
	StatusRunning      ProcessStatus = "running"
	StatusSnapshotting ProcessStatus = "snapshotting"
	StatusDead         ProcessStatus = "dead"
)

// Transport is the message-passing abstraction (implemented by SimTransport).
type Transport interface {
	Send(from, to string, m *Message) error
}

// Message is a point-to-point message between processes.
// It carries the sender's clock stamps so the receiver can apply the
// appropriate clock update rule (LC2, VC3, MC3) on delivery.
type Message struct {
	ID         string
	From       string
	To         string
	Data       interface{}
	SentVC     vector.VectorClock
	SentMatrix matrix.MatrixClock // full matrix stamp for MC3
	SentLC     uint64
	SentAt     time.Time
	IsMarker   bool
	SnapshotID string
}

// ProcessConfig is the configuration for spawning a process.
// All fields except ID are optional; ClockType and DeliveryMode default
// to the parent simulation's settings when empty.
type ProcessConfig struct {
	ID           string
	ClockType    ClockType
	DeliveryMode DeliveryMode
	Peers        []string // connected peer process IDs
	AllPIDs      []string // all process IDs (for vector/matrix clock initialization)
}

// ProcessState is a serializable snapshot of process state, safe to marshal
// to JSON and send over REST/WebSocket. It is a value type (no pointers).
type ProcessState struct {
	ID           string                       `json:"id"`
	ClockType    ClockType                    `json:"clockType"`
	Delivery     DeliveryMode                 `json:"deliveryMode"`
	LamportClock uint64                       `json:"lamportClock"`
	VectorClock  map[string]uint64            `json:"vectorClock,omitempty"`
	MatrixClock  map[string]map[string]uint64 `json:"matrixClock,omitempty"`
	Peers        []string                     `json:"peers"`
	EventCount   int                          `json:"eventCount"`
	HeldMessages int                          `json:"heldMessages"`
	Status       ProcessStatus                `json:"status"`
}

// GetVectorClock satisfies the snapshot package's clockGetter interface,
// allowing the snapshot verifier to extract the vector clock for cut-property
// consistency checks without creating an import cycle.
func (ps ProcessState) GetVectorClock() map[string]uint64 {
	return ps.VectorClock
}

// Process is a simulated distributed system process.
//
// Locking contract:
//   - p.mu protects all clock state, cfg.Peers, and status.
//   - emitEventLocked must be called with p.mu held (at least read).
//   - emitEvent acquires p.mu.RLock internally; do not call while holding p.mu.
//   - msgSeq is an independent atomic counter for message IDs (not event sequence).
//   - localSeq is the per-event sequence counter, incremented only in emitEventLocked.
//   - OnMarker is an optional synchronous callback invoked the instant a
//     snapshot marker is observed. The callback receives the process's
//     state captured AT THE INSTANT of the marker, before any subsequent
//     message processing. This is required for correct Chandy-Lamport
//     snapshot semantics (Chandy & Lamport 1985).
type Process struct {
	cfg   ProcessConfig
	bus   *events.EventBus
	xport Transport

	mu           sync.RWMutex
	lamportClock *lamport.LamportClock
	vectorClock  vector.VectorClock
	matrixClock  matrix.MatrixClock
	status       ProcessStatus
	msgSeq       atomic.Uint64 // per-process message counter (H13: separate from event seq)
	localSeq     atomic.Uint64 // per-process event sequence counter

	inbound chan *Message // messages waiting to be processed
	hbq     *HoldBackQueue

	ctx    context.Context
	cancel context.CancelFunc

	// OnMarker is invoked synchronously when a snapshot marker is
	// received. The callback captures the process's local state at the
	// exact instant of the marker, before any further message processing.
	// Used by the Chandy-Lamport coordinator to record a causally-
	// consistent local snapshot.
	OnMarker func(from, snapshotID string, localState ProcessState)
}

// New creates a new Process. Call Start() to begin processing messages.
//
// New does NOT start the goroutine; callers must call Start() explicitly
// after wiring up any required callbacks.
func New(cfg ProcessConfig, xport Transport, bus *events.EventBus) *Process {
	ctx, cancel := context.WithCancel(context.Background())
	p := &Process{
		cfg:     cfg,
		bus:     bus,
		xport:   xport,
		status:  StatusRunning,
		inbound: make(chan *Message, 256),
		hbq:     &HoldBackQueue{},
		ctx:     ctx,
		cancel:  cancel,
	}
	switch cfg.ClockType {
	case ClockLamport:
		p.lamportClock = lamport.New(cfg.ID)
	case ClockVector:
		p.vectorClock = vector.New(cfg.AllPIDs)
	case ClockMatrix:
		p.matrixClock = matrix.New(cfg.AllPIDs)
	}
	return p
}

// Start launches the process message-processing goroutine and emits EvtProcessSpawned.
func (p *Process) Start() {
	go p.run()
	p.emitEvent(events.EvtProcessSpawned, nil)
}

// Stop gracefully shuts down the process.
func (p *Process) Stop() {
	p.mu.Lock()
	p.status = StatusDead
	p.mu.Unlock()
	p.cancel()
	p.emitEvent(events.EvtProcessKilled, nil)
}

// Deliver pushes a message into the process's inbound channel.
func (p *Process) Deliver(m *Message) {
	select {
	case p.inbound <- m:
	case <-p.ctx.Done():
	}
}

// InternalEvent triggers a local event (ticks the clock).
func (p *Process) InternalEvent() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tickClock()
	p.emitEventLocked(events.EvtInternalEvent, nil)
}

// Send sends a message to another process.
func (p *Process) Send(to string, data interface{}) error {
	p.mu.Lock()
	// H13: use separate msgSeq for message IDs — localSeq is only for event sequence.
	msgID := fmt.Sprintf("%s-m%d", p.cfg.ID, p.msgSeq.Add(1))
	lc, vc, mc := p.sendClock()
	msg := &Message{
		ID:         msgID,
		From:       p.cfg.ID,
		To:         to,
		Data:       data,
		SentVC:     vc,
		SentMatrix: mc,
		SentLC:     lc,
		SentAt:     time.Now(),
	}
	p.emitEventLocked(events.EvtSend, &events.MessagePayload{
		ID:     msgID,
		From:   p.cfg.ID,
		To:     to,
		Data:   data,
		SentVC: vc,
		SentLC: lc,
	})
	p.mu.Unlock()

	return p.xport.Send(p.cfg.ID, to, msg)
}

// Broadcast sends a message to all peers.
func (p *Process) Broadcast(data interface{}) error {
	for _, peer := range p.cfg.Peers {
		if err := p.Send(peer, data); err != nil {
			return fmt.Errorf("broadcast to %s: %w", peer, err)
		}
	}
	return nil
}

// Snapshot returns the current serializable state.
func (p *Process) Snapshot() ProcessState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.snapshotLocked()
}

// snapshotLocked reads the current state. Caller must hold p.mu (any mode).
func (p *Process) snapshotLocked() ProcessState {
	state := ProcessState{
		ID:           p.cfg.ID,
		ClockType:    p.cfg.ClockType,
		Delivery:     p.cfg.DeliveryMode,
		Peers:        p.cfg.Peers,
		EventCount:   clampUint64ToInt(p.localSeq.Load()),
		HeldMessages: p.hbq.Len(),
		Status:       p.status,
	}
	if p.lamportClock != nil {
		state.LamportClock = p.lamportClock.Value()
	}
	if p.vectorClock != nil {
		state.VectorClock = vector.Copy(p.vectorClock)
	}
	if p.matrixClock != nil {
		state.MatrixClock = matrix.DeepCopy(p.matrixClock)
	}
	return state
}

// ID returns the process identifier.
func (p *Process) ID() string { return p.cfg.ID }

// AddPeer adds a peer process ID (for dynamic membership).
func (p *Process) AddPeer(pid string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, existing := range p.cfg.Peers {
		if existing == pid {
			return
		}
	}
	p.cfg.Peers = append(p.cfg.Peers, pid)
}

// ── internal ─────────────────────────────────────────────────────────────────

func (p *Process) run() {
	// Per-goroutine panic recovery: a single malformed message must never
	// crash the entire simulation backend. On panic we log (no logger here)
	// and continue, marking the process as StatusDead so callers can detect.
	defer func() {
		if r := recover(); r != nil {
			p.mu.Lock()
			p.status = StatusDead
			p.mu.Unlock()
		}
	}()
	for {
		select {
		case <-p.ctx.Done():
			return
		case msg := <-p.inbound:
			p.handleMessage(msg)
		}
	}
}

func (p *Process) handleMessage(msg *Message) {
	// A panic in any one delivery path must not terminate the run goroutine.
	// We catch, mark the process dead, and re-publish the panic as an
	// observable bus event so operators can see what happened.
	defer func() {
		if r := recover(); r != nil {
			p.mu.Lock()
			p.status = StatusDead
			p.mu.Unlock()
			p.bus.Publish(events.Event{
				Type:      events.EvtInternalEvent,
				ProcessID: p.cfg.ID,
				GlobalSeq: p.bus.NextSeq(),
				Timestamp: time.Now(),
				Message: &events.MessagePayload{
					ID:   fmt.Sprintf("panic-%v", r),
					From: p.cfg.ID,
					To:   p.cfg.ID,
					Data: r,
				},
			})
		}
	}()
	if msg.IsMarker {
		// Capture state and forward the snapshot marker while holding p.mu.Lock().
		// Holding the write lock through the onSendMarker call ensures that any
		// concurrent p.Send() (from an external goroutine) cannot enqueue an
		// application message into the transport channel BEFORE the marker.
		// Without this, a concurrent send could tick the clock past the snapshot
		// value and put a message in the channel first, violating the Chandy-Lamport
		// cut property (sender counter < message sender-counter).
		//
		// OnMarker must NOT call p.Snapshot() internally (it would re-acquire p.mu
		// and deadlock). The coordinator receives the pre-captured state instead.
		if p.OnMarker != nil {
			p.mu.Lock()
			localState := p.snapshotLocked()
			p.OnMarker(msg.From, msg.SnapshotID, localState)
			p.mu.Unlock()
		}
		p.emitEvent(events.EvtMarkerReceived, &events.MessagePayload{
			ID:         msg.ID,
			From:       msg.From,
			To:         msg.To,
			IsMarker:   true,
			SnapshotID: msg.SnapshotID,
		})
		return
	}

	switch p.cfg.DeliveryMode {
	case ImmediateDelivery:
		p.deliverImmediate(msg)
	case CausalDelivery:
		p.deliverCausal(msg)
	case TotalOrderDelivery:
		// H6: Lamport's total-order broadcast is not yet implemented.
		// Fall back to causal delivery so behaviour is at least causally consistent.
		p.deliverCausal(msg)
	default:
		p.deliverImmediate(msg)
	}
}

func (p *Process) deliverImmediate(msg *Message) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.receiveClock(msg)
	p.emitEventLocked(events.EvtReceive, &events.MessagePayload{
		ID:     msg.ID,
		From:   msg.From,
		To:     msg.To,
		Data:   msg.Data,
		SentVC: msg.SentVC,
		SentLC: msg.SentLC,
	})
}

func (p *Process) deliverCausal(msg *Message) {
	p.mu.Lock()
	localVC := vector.Copy(p.vectorClock)
	p.mu.Unlock()

	if canDeliver(msg.SentVC, msg.From, localVC) {
		p.mu.Lock()
		// H14: merge full causal prefix using vector.Receive (VC3 rule).
		p.vectorClock = vector.Receive(p.vectorClock, msg.SentVC, p.cfg.ID)
		p.emitEventLocked(events.EvtReceive, &events.MessagePayload{
			ID:     msg.ID,
			From:   msg.From,
			To:     msg.To,
			Data:   msg.Data,
			SentVC: msg.SentVC,
		})
		// Try to deliver any held messages
		delivered, newVC := p.hbq.TryDeliver(vector.Copy(p.vectorClock), p.cfg.ID)
		p.vectorClock = newVC
		p.mu.Unlock()

		for _, dm := range delivered {
			p.emitEvent(events.EvtDelivered, &events.MessagePayload{
				ID:   dm.ID,
				From: dm.From,
				To:   dm.To,
				Data: dm.Data,
			})
		}
	} else {
		// Hold message
		p.hbq.Enqueue(msg, msg.SentVC)
		p.emitEvent(events.EvtHeld, &events.MessagePayload{
			ID:     msg.ID,
			From:   msg.From,
			To:     msg.To,
			SentVC: msg.SentVC,
		})
	}
}

// tickClock advances the clock for an internal event. Caller holds p.mu.
func (p *Process) tickClock() {
	switch p.cfg.ClockType {
	case ClockLamport:
		p.lamportClock.Tick()
	case ClockVector:
		p.vectorClock = vector.Tick(p.vectorClock, p.cfg.ID)
	case ClockMatrix:
		p.matrixClock = matrix.Tick(p.matrixClock, p.cfg.ID)
	}
}

// sendClock advances clock for send and returns (lc, vc, mc) stamp. Caller holds p.mu.
// H2b: matrix clock now returns the full sender-row matrix stamp (not stamp["self"]).
func (p *Process) sendClock() (uint64, vector.VectorClock, matrix.MatrixClock) {
	switch p.cfg.ClockType {
	case ClockLamport:
		lc := p.lamportClock.Send()
		return lc, nil, nil
	case ClockVector:
		var stamp vector.VectorClock
		p.vectorClock, stamp = vector.Send(p.vectorClock, p.cfg.ID)
		return 0, stamp, nil
	case ClockMatrix:
		var stamp matrix.MatrixClock
		p.matrixClock, stamp = matrix.Send(p.matrixClock, p.cfg.ID)
		return 0, nil, stamp
	}
	return 0, nil, nil
}

// receiveClock updates clock on message receipt. Caller holds p.mu.
// H2a: matrix clock now properly calls matrix.Receive (MC3 rule).
func (p *Process) receiveClock(msg *Message) {
	switch p.cfg.ClockType {
	case ClockLamport:
		p.lamportClock.Receive(msg.SentLC)
	case ClockVector:
		p.vectorClock = vector.Receive(p.vectorClock, msg.SentVC, p.cfg.ID)
	case ClockMatrix:
		p.matrixClock = matrix.Receive(p.matrixClock, msg.SentMatrix, p.cfg.ID, msg.From)
	}
}

// emitEvent creates and publishes an event with current clock state.
// Must NOT be called while holding p.mu (it acquires RLock internally).
func (p *Process) emitEvent(t events.EventType, msg *events.MessagePayload) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.emitEventLocked(t, msg)
}

// emitEventLocked publishes an event. Caller must hold p.mu (at least RLock).
func (p *Process) emitEventLocked(t events.EventType, msg *events.MessagePayload) {
	seq := p.localSeq.Add(1)
	e := events.Event{
		ID:        fmt.Sprintf("%s-e%d", p.cfg.ID, seq),
		Type:      t,
		ProcessID: p.cfg.ID,
		Timestamp: time.Now(),
		LocalSeq:  seq,
		GlobalSeq: p.bus.NextSeq(),
		Message:   msg,
	}
	if p.lamportClock != nil {
		lc := p.lamportClock.Value()
		e.LamportClock = &lc
	}
	if p.vectorClock != nil {
		e.VectorClock = vector.Copy(p.vectorClock)
	}
	if p.matrixClock != nil {
		e.MatrixClock = matrix.DeepCopy(p.matrixClock)
	}
	p.bus.Publish(e)
}

// clampUint64ToInt converts a uint64 to int with a defensive cap. Real
// per-process event counts can never reach int64-max in any plausible
// deployment, so we cap at that bound to make the conversion safe and
// explicit.
func clampUint64ToInt(v uint64) int {
	const maxInt = int(^uint(0) >> 1)
	if v > uint64(maxInt) {
		return maxInt
	}
	return int(v)
}
