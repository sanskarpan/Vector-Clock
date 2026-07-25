package simulation

import (
	"fmt"
	"sync"
	"time"

	"github.com/DistributedClocks/vectorclock-system/internal/process"
)

// ChannelKey identifies a directed channel between two processes.
type ChannelKey struct{ From, To string }

// SimTransport provides FIFO message channels with fault injection.
//
// Lifecycle: callers must invoke Close() at shutdown to stop the per-channel
// forward goroutines and the delay goroutines. Without Close() those
// goroutines leak until process exit.
type SimTransport struct {
	mu       sync.RWMutex
	channels map[ChannelKey]chan *process.Message

	// Fault injection state
	delays   map[ChannelKey]time.Duration
	dropNext map[ChannelKey]bool

	// Reorder injection: when set, the next message on the channel is
	// buffered; the subsequent message is sent first, then the buffered one.
	reorderBuf map[ChannelKey]*process.Message

	// Partition support: if non-nil, messages crossing groups are dropped.
	partitionGroups [][]string

	// Per-process delivery target (set by RegisterProcess)
	deliver map[string]func(*process.Message)

	// Lifecycle.
	closed   chan struct{}
	closeOne sync.Once
}

func NewSimTransport() *SimTransport {
	return &SimTransport{
		channels:   make(map[ChannelKey]chan *process.Message),
		delays:     make(map[ChannelKey]time.Duration),
		dropNext:   make(map[ChannelKey]bool),
		reorderBuf: make(map[ChannelKey]*process.Message),
		deliver:    make(map[string]func(*process.Message)),
		closed:     make(chan struct{}),
	}
}

// RegisterProcess registers a process and its delivery function.
func (t *SimTransport) RegisterProcess(pid string, deliverFn func(*process.Message)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deliver[pid] = deliverFn
}

// RegisterChannel creates a buffered FIFO channel between from and to.
func (t *SimTransport) RegisterChannel(from, to string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := ChannelKey{from, to}
	if t.channels[key] == nil {
		ch := make(chan *process.Message, ChannelBufferSize)
		t.channels[key] = ch
		go t.forward(key, ch)
	}
}

// Send enqueues a message on the channel from→to. Returns an error if the
// channel is full, has been closed, or has no route.
//
// Concurrency: Send reads channel pointers under RLock, then sends outside
// the lock. Close() never closes the per-direction channels; it only
// signals via `closed`. This avoids the "send on closed channel" panic.
func (t *SimTransport) Send(from, to string, m *process.Message) error {
	t.mu.RLock()
	key := ChannelKey{from, to}
	ch := t.channels[key]
	drop := t.dropNext[key]
	delay := t.delays[key]
	closed := t.closed
	groups := t.partitionGroups
	reorderMsg := t.reorderBuf[key]
	t.mu.RUnlock()

	select {
	case <-closed:
		return fmt.Errorf("transport closed")
	default:
	}

	if ch == nil {
		return fmt.Errorf("no channel %s→%s", from, to)
	}

	// Partition check: if groups are active and from/to are in different
	// groups, silently drop.
	if len(groups) > 0 && !inSameGroup(groups, from, to) {
		return nil
	}

	// Drop injection: checked before reorder so drop takes priority.
	if drop {
		t.mu.Lock()
		t.dropNext[key] = false
		t.mu.Unlock()
		return nil
	}

	// Reorder injection: if a buffered message exists, send the current
	// one first then the buffered one. Otherwise, buffer this message.
	if reorderMsg != nil {
		t.mu.Lock()
		t.reorderBuf[key] = nil
		t.mu.Unlock()
		if err := t.sendToChannel(ch, closed, m); err != nil {
			return err
		}
		return t.sendToChannel(ch, closed, reorderMsg)
	}

	// Buffer this message for reorder on next send.
	// Only activate when a reorder was actually injected for this key.
	if _, reorderActive := t.reorderBuf[key]; reorderActive {
		t.mu.Lock()
		t.reorderBuf[key] = m
		t.mu.Unlock()
		return nil
	}

	// Delay injection
	if delay > 0 {
		go t.sendDelayed(ch, m, closed, delay)
		return nil
	}

	return t.sendToChannel(ch, closed, m)
}

// sendToChannel performs a non-blocking send with closed-guard.
func (t *SimTransport) sendToChannel(ch chan *process.Message, closed chan struct{}, m *process.Message) error {
	select {
	case <-closed:
		return fmt.Errorf("transport closed")
	default:
	}
	select {
	case ch <- m:
		return nil
	case <-closed:
		return fmt.Errorf("transport closed")
	default:
		return fmt.Errorf("channel full")
	}
}

// sendDelayed handles a delayed message send in a goroutine.
func (t *SimTransport) sendDelayed(ch chan *process.Message, m *process.Message, closed chan struct{}, delay time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			_ = r
		}
	}()
	select {
	case <-closed:
		return
	case <-time.After(delay):
		select {
		case <-closed:
		default:
			select {
			case ch <- m:
			case <-closed:
			}
		}
	}
}

// forward drains a channel and invokes the registered delivery function.
func (t *SimTransport) forward(key ChannelKey, ch chan *process.Message) {
	t.mu.RLock()
	deliverFn := t.deliver[key.To]
	t.mu.RUnlock()

	defer func() {
		if r := recover(); r != nil {
		}
	}()
	for {
		select {
		case <-t.closed:
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			if deliverFn != nil {
				func() {
					defer func() { _ = recover() }()
					deliverFn(m)
				}()
			}
		}
	}
}

// InjectDelay sets a delivery delay on channel from→to.
func (t *SimTransport) InjectDelay(from, to string, d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.delays[ChannelKey{from, to}] = d
}

// InjectDrop causes the next message on channel from→to to be dropped.
func (t *SimTransport) InjectDrop(from, to string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dropNext[ChannelKey{from, to}] = true
}

// InjectReorder causes the next two messages on from→to to be swapped.
func (t *SimTransport) InjectReorder(from, to string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Setting reorderBuf to non-nil marker. The next message will be
	// buffered; the message after that will be sent first.
	if t.reorderBuf == nil {
		t.reorderBuf = make(map[ChannelKey]*process.Message)
	}
	t.reorderBuf[ChannelKey{from, to}] = nil
}

// SetPartition partitions processes into groups. Messages crossing groups
// are silently dropped. Call with nil or empty to remove the partition.
func (t *SimTransport) SetPartition(groups [][]string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(groups) == 0 {
		t.partitionGroups = nil
		return
	}
	t.partitionGroups = groups
}

// ClearFaults removes all injected faults.
func (t *SimTransport) ClearFaults() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.delays = make(map[ChannelKey]time.Duration)
	t.dropNext = make(map[ChannelKey]bool)
	t.reorderBuf = make(map[ChannelKey]*process.Message)
	t.partitionGroups = nil
}

// Close stops all forward goroutines. Idempotent.
func (t *SimTransport) Close() {
	t.closeOne.Do(func() {
		t.mu.Lock()
		close(t.closed)
		t.mu.Unlock()
	})
}

// Done returns a channel closed when the transport has been Close()d.
func (t *SimTransport) Done() <-chan struct{} { return t.closed }

// ChannelCount returns the number of registered channels.
func (t *SimTransport) ChannelCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.channels)
}

// inSameGroup checks if from and to are in the same partition group.
func inSameGroup(groups [][]string, from, to string) bool {
	for _, g := range groups {
		hasFrom, hasTo := false, false
		for _, p := range g {
			if p == from {
				hasFrom = true
			}
			if p == to {
				hasTo = true
			}
		}
		if hasFrom && hasTo {
			return true
		}
	}
	return false
}
