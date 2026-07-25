// Package events defines all event types flowing through the system event bus.
package events

import "time"

// EventType identifies the kind of distributed system event.
type EventType string

const (
	EvtInternalEvent    EventType = "internal_event"
	EvtSend             EventType = "send"
	EvtReceive          EventType = "receive"
	EvtHeld             EventType = "message_held"
	EvtDelivered        EventType = "message_delivered"
	EvtClockUpdate      EventType = "clock_update"
	EvtSnapshotStart    EventType = "snapshot_start"
	EvtMarkerSent       EventType = "marker_sent"
	EvtMarkerReceived   EventType = "marker_received"
	EvtLocalSnapshot    EventType = "local_snapshot"
	EvtSnapshotComplete EventType = "snapshot_complete"
	EvtConflict         EventType = "conflict"
	EvtResolved         EventType = "resolved"
	EvtProcessSpawned   EventType = "process_spawned"
	EvtProcessKilled    EventType = "process_killed"
	EvtScenarioStep     EventType = "scenario_step"
)

// MessagePayload carries message detail in events.
type MessagePayload struct {
	ID         string            `json:"id"`
	From       string            `json:"from"`
	To         string            `json:"to"`
	Data       interface{}       `json:"data,omitempty"`
	SentVC     map[string]uint64 `json:"sentVc,omitempty"`
	SentLC     uint64            `json:"sentLc,omitempty"`
	IsMarker   bool              `json:"isMarker,omitempty"`
	SnapshotID string            `json:"snapshotId,omitempty"`
}

// SnapshotPayload carries snapshot detail in events.
type SnapshotPayload struct {
	SnapshotID string `json:"snapshotId"`
	ProcessID  string `json:"processId,omitempty"`
	Complete   bool   `json:"complete,omitempty"`
}

// ConflictPayload carries conflict detail in events.
type ConflictPayload struct {
	Key      string      `json:"key"`
	Siblings interface{} `json:"siblings,omitempty"`
}

// Event is the canonical event structure flowing through the system.
type Event struct {
	ID        string    `json:"id"`
	Type      EventType `json:"type"`
	ProcessID string    `json:"processId"`
	Timestamp time.Time `json:"timestamp"`
	LocalSeq  uint64    `json:"localSeq"`
	GlobalSeq uint64    `json:"globalSeq"`

	// Clock state at time of event
	LamportClock *uint64                      `json:"lamportClock,omitempty"`
	VectorClock  map[string]uint64            `json:"vectorClock,omitempty"`
	MatrixClock  map[string]map[string]uint64 `json:"matrixClock,omitempty"`

	Message   *MessagePayload  `json:"message,omitempty"`
	Snapshot  *SnapshotPayload `json:"snapshot,omitempty"`
	Conflict  *ConflictPayload `json:"conflict,omitempty"`
	Narration string           `json:"narration,omitempty"`
}
