package events

import (
	"log"
	"sync"
	"sync/atomic"
)

const historySize = 1000

// EventBus is a pub/sub bus for distributed system events.
// Publish is non-blocking; full buffer drops events and increments DropCount.
type EventBus struct {
	publish chan Event

	mu   sync.RWMutex
	subs map[chan Event]map[EventType]bool

	// Ring buffer history for replay
	history [historySize]Event
	histIdx int
	histLen int // how many entries filled (up to historySize)

	globalSeq atomic.Uint64

	// M4: drop counter for monitoring
	dropCount  atomic.Uint64
	dropLogged atomic.Uint64 // last logged drop count (for rate limiting)

	done     chan struct{}
	stopOnce sync.Once
}

// NewEventBus creates and starts an EventBus.
func NewEventBus() *EventBus {
	b := &EventBus{
		publish: make(chan Event, 4096),
		subs:    make(map[chan Event]map[EventType]bool),
		done:    make(chan struct{}),
	}
	go b.loop()
	return b
}

// NextSeq returns the next monotonic global sequence number.
func (b *EventBus) NextSeq() uint64 {
	return b.globalSeq.Add(1)
}

// Publish enqueues an event. Non-blocking: drops and increments DropCount if buffer full.
func (b *EventBus) Publish(e Event) {
	select {
	case b.publish <- e:
	default:
		n := b.dropCount.Add(1)
		// M4: log a warning at most once per 100 drops to avoid log spam.
		if prev := b.dropLogged.Load(); n-prev >= 100 {
			if b.dropLogged.CompareAndSwap(prev, n) {
				log.Printf("[EventBus] WARN: dropped %d events (buffer full)", n)
			}
		}
	}
}

// DropCount returns the total number of dropped events since startup.
func (b *EventBus) DropCount() uint64 {
	return b.dropCount.Load()
}

// Subscribe returns a channel that receives events of the given types.
// Pass nil to subscribe to all event types.
func (b *EventBus) Subscribe(types []EventType) chan Event {
	ch := make(chan Event, 256)
	typeSet := make(map[EventType]bool, len(types))
	for _, t := range types {
		typeSet[t] = true
	}
	b.mu.Lock()
	b.subs[ch] = typeSet
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes a subscriber channel.
func (b *EventBus) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	delete(b.subs, ch)
	b.mu.Unlock()
}

// History returns the last N events from the ring buffer.
func (b *EventBus) History() []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.histLen == 0 {
		return nil
	}
	result := make([]Event, b.histLen)
	if b.histLen < historySize {
		copy(result, b.history[:b.histLen])
	} else {
		// Wrap-around: start from histIdx (oldest)
		start := b.histIdx
		for i := 0; i < historySize; i++ {
			result[i] = b.history[(start+i)%historySize]
		}
	}
	return result
}

// Done returns a channel that is closed when the bus is stopped.
func (b *EventBus) Done() <-chan struct{} {
	return b.done
}

// Stop shuts down the event bus loop. Idempotent.
func (b *EventBus) Stop() {
	b.stopOnce.Do(func() { close(b.done) })
}

func (b *EventBus) loop() {
	for {
		select {
		case <-b.done:
			return
		case e := <-b.publish:
			b.store(e)
			b.fanout(e)
		}
	}
}

func (b *EventBus) store(e Event) {
	b.mu.Lock()
	b.history[b.histIdx] = e
	b.histIdx = (b.histIdx + 1) % historySize
	if b.histLen < historySize {
		b.histLen++
	}
	b.mu.Unlock()
}

func (b *EventBus) fanout(e Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch, types := range b.subs {
		if len(types) == 0 || types[e.Type] {
			select {
			case ch <- e:
			default:
				// M4: subscriber buffer full — drop silently (sub-channel drops
				// are separate from publish-buffer drops).
			}
		}
	}
}
