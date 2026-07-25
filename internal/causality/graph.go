// Package causality implements the happened-before relation (Lamport 1978),
// consistent cuts (Mattern 1987), and causal graph queries.
package causality

import "sync"

// maxBFSDepth caps the depth of traversal queries. Prevents unbounded CPU
// on adversarial inputs (e.g. caller passes an event ID that triggers a
// full-graph BFS in a long-running simulation).
const maxBFSDepth = 10_000

// EventType classifies what kind of event occurred.
type EventType string

const (
	EventInternal EventType = "internal"
	EventSend     EventType = "send"
	EventReceive  EventType = "receive"
)

// Event is a single event in a distributed execution.
type Event struct {
	ID        string
	ProcessID string
	Type      EventType
	LocalSeq  uint64 // per-process sequence number
	MessageID string // for send/receive pairing
}

// CausalGraph tracks events and happened-before edges (directed acyclic graph).
type CausalGraph struct {
	mu     sync.RWMutex
	events map[string]*Event
	edges  map[string]map[string]bool // from → set of to (happened-before edges)
}

// NewCausalGraph creates an empty causal graph.
func NewCausalGraph() *CausalGraph {
	return &CausalGraph{
		events: make(map[string]*Event),
		edges:  make(map[string]map[string]bool),
	}
}

// AddEvent registers an event in the graph.
func (g *CausalGraph) AddEvent(e *Event) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.events[e.ID] = e
	if g.edges[e.ID] == nil {
		g.edges[e.ID] = make(map[string]bool)
	}
}

// AddEdge adds a directed happened-before edge: from → to.
func (g *CausalGraph) AddEdge(from, to string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.edges[from] == nil {
		g.edges[from] = make(map[string]bool)
	}
	g.edges[from][to] = true
}

// HappenedBefore returns true if there is a directed path from aID to bID
// (i.e., a → b, transitively). BFS is bounded by maxBFSDepth to prevent
// pathological long traversals.
func (g *CausalGraph) HappenedBefore(aID, bID string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if aID == bID {
		return false // irreflexive
	}

	visited := make(map[string]bool)
	queue := []string{aID}
	depth := 0
	for len(queue) > 0 && depth < maxBFSDepth {
		cur := queue[0]
		queue = queue[1:]
		if cur == bID {
			return true
		}
		if visited[cur] {
			continue
		}
		visited[cur] = true
		depth++
		for next := range g.edges[cur] {
			if !visited[next] {
				queue = append(queue, next)
			}
		}
	}
	return false
}

// Concurrent returns true iff neither a→b nor b→a.
func (g *CausalGraph) Concurrent(aID, bID string) bool {
	return !g.HappenedBefore(aID, bID) && !g.HappenedBefore(bID, aID)
}

// CausalHistory returns all events that happened before id (its ancestors).
func (g *CausalGraph) CausalHistory(id string) []*Event {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var result []*Event
	visited := make(map[string]bool)
	// Reverse BFS: find all nodes that have a path to id.
	// Build reverse edge map on the fly.
	reverse := make(map[string][]string)
	for from, tos := range g.edges {
		for to := range tos {
			reverse[to] = append(reverse[to], from)
		}
	}

	queue := reverse[id]
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if visited[cur] {
			continue
		}
		visited[cur] = true
		if e, ok := g.events[cur]; ok {
			result = append(result, e)
		}
		queue = append(queue, reverse[cur]...)
	}
	return result
}

// TopologicalSort returns all events in a topological (causal) order.
// Uses Kahn's algorithm.
func (g *CausalGraph) TopologicalSort() []*Event {
	g.mu.RLock()
	defer g.mu.RUnlock()

	inDegree := make(map[string]int, len(g.events))
	for id := range g.events {
		inDegree[id] = 0
	}
	for from, tos := range g.edges {
		if _, ok := g.events[from]; !ok {
			continue
		}
		for to := range tos {
			if _, ok := g.events[to]; ok {
				inDegree[to]++
			}
		}
	}

	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	var sorted []*Event
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if e, ok := g.events[cur]; ok {
			sorted = append(sorted, e)
		}
		for next := range g.edges[cur] {
			if _, ok := g.events[next]; !ok {
				continue
			}
			inDegree[next]--
			if inDegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	return sorted
}

// Events returns a copy of all events.
func (g *CausalGraph) Events() map[string]*Event {
	g.mu.RLock()
	defer g.mu.RUnlock()
	result := make(map[string]*Event, len(g.events))
	for k, v := range g.events {
		result[k] = v
	}
	return result
}
