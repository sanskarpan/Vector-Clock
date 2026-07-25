// Package lamport implements Lamport scalar logical clocks per Lamport 1978.
package lamport

import "sync"

// LamportClock is a thread-safe scalar logical clock.
type LamportClock struct {
	mu    sync.Mutex
	value uint64
	pid   string
}

// New creates a Lamport clock for process pid, starting at 0.
func New(pid string) *LamportClock {
	return &LamportClock{pid: pid}
}

// Tick increments the clock (LC1: internal event). Returns new value.
func (c *LamportClock) Tick() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value++
	return c.value
}

// Send increments before send (LC2). Same rule as Tick, named separately.
func (c *LamportClock) Send() uint64 {
	return c.Tick()
}

// Receive updates on message receipt (LC3): c = max(c, ts) + 1.
func (c *LamportClock) Receive(msgTimestamp uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if msgTimestamp > c.value {
		c.value = msgTimestamp
	}
	c.value++
	return c.value
}

// Value returns the current clock value without incrementing.
func (c *LamportClock) Value() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

// PID returns the process ID this clock belongs to.
func (c *LamportClock) PID() string {
	return c.pid
}

// TotalOrder compares two (timestamp, pid) pairs for deterministic total order.
// Returns -1 if (ts1,pid1) < (ts2,pid2), 0 if equal, 1 if greater.
func TotalOrder(ts1 uint64, pid1 string, ts2 uint64, pid2 string) int {
	if ts1 != ts2 {
		if ts1 < ts2 {
			return -1
		}
		return 1
	}
	// Tie-break by pid lexicographic order
	if pid1 < pid2 {
		return -1
	}
	if pid1 > pid2 {
		return 1
	}
	return 0
}
