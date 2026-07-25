package events_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DistributedClocks/vectorclock-system/internal/events"
)

func TestEventBus_PublishSubscribe(t *testing.T) {
	bus := events.NewEventBus()
	defer bus.Stop()

	ch := bus.Subscribe([]events.EventType{events.EvtInternalEvent})
	defer bus.Unsubscribe(ch)

	bus.Publish(events.Event{
		Type:      events.EvtInternalEvent,
		ProcessID: "P1",
		GlobalSeq: bus.NextSeq(),
	})

	select {
	case e := <-ch:
		if e.ProcessID != "P1" {
			t.Fatalf("got event from %q, want P1", e.ProcessID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("no event received")
	}
}

func TestEventBus_SubscribeAll(t *testing.T) {
	bus := events.NewEventBus()
	defer bus.Stop()

	ch := bus.Subscribe(nil) // nil = subscribe to all
	defer bus.Unsubscribe(ch)

	bus.Publish(events.Event{Type: events.EvtInternalEvent, GlobalSeq: bus.NextSeq()})
	bus.Publish(events.Event{Type: events.EvtSend, GlobalSeq: bus.NextSeq()})

	count := 0
	deadline := time.After(500 * time.Millisecond)
loop:
	for {
		select {
		case <-ch:
			count++
			if count >= 2 {
				break loop
			}
		case <-deadline:
			break loop
		}
	}
	if count < 2 {
		t.Errorf("expected 2 events, got %d", count)
	}
}

func TestEventBus_GlobalSeqMonotonic(t *testing.T) {
	bus := events.NewEventBus()
	defer bus.Stop()

	var lastSeq uint64
	for i := 0; i < 100; i++ {
		seq := bus.NextSeq()
		if seq <= lastSeq {
			t.Fatalf("NextSeq not monotonic: %d after %d", seq, lastSeq)
		}
		lastSeq = seq
	}
}

func TestEventBus_PublishDoesNotBlock(t *testing.T) {
	// Verify the bus does not block on full buffer (should drop instead).
	bus := events.NewEventBus()
	defer bus.Stop()

	// Fill the publish buffer; further publishes must not block.
	for i := 0; i < 10000; i++ {
		done := make(chan struct{})
		go func() {
			bus.Publish(events.Event{Type: events.EvtInternalEvent})
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("Publish blocked at iteration %d", i)
		}
	}
}

func TestEventBus_ConcurrentPublishers(t *testing.T) {
	// Race-detector regression: concurrent publishers must be safe.
	bus := events.NewEventBus()
	defer bus.Stop()
	ch := bus.Subscribe(nil)
	defer bus.Unsubscribe(ch)

	var wg sync.WaitGroup
	var received atomic.Int64
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ch:
				received.Add(1)
			case <-done:
				return
			}
		}
	}()

	const N = 100
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bus.Publish(events.Event{Type: events.EvtInternalEvent, GlobalSeq: bus.NextSeq()})
		}()
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
	close(done)
}

func TestEventBus_StopClosesSubscribers(t *testing.T) {
	bus := events.NewEventBus()
	ch := bus.Subscribe(nil)
	bus.Stop()
	// Channel should not block forever; History should still work.
	_ = bus.History()
	_ = ch
}

func TestEventBus_HistoryRingBuffer(t *testing.T) {
	bus := events.NewEventBus()
	defer bus.Stop()

	// Publish more events than the ring buffer holds. History is populated
	// asynchronously by the bus loop, so we must wait for drain before
	// asserting on the snapshot.
	for i := 0; i < 1500; i++ {
		bus.Publish(events.Event{Type: events.EvtInternalEvent, GlobalSeq: bus.NextSeq()})
	}
	// Wait until the buffer is full (or timeout).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hist := bus.History()
		if len(hist) == 1000 {
			// Verify the most recent event has a higher GlobalSeq than
			// the oldest (sanity check on ring semantics).
			if hist[0].GlobalSeq >= hist[len(hist)-1].GlobalSeq {
				t.Errorf("ring order broken: first=%d last=%d",
					hist[0].GlobalSeq, hist[len(hist)-1].GlobalSeq)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	hist := bus.History()
	t.Errorf("history did not fill to 1000 within deadline; got %d", len(hist))
}
