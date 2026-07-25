// TypeScript types mirroring Go event/state types
export type ClockType = 'lamport' | 'vector' | 'matrix'
export type DeliveryMode = 'immediate' | 'causal' | 'total_order'
export type ProcessStatus = 'running' | 'snapshotting' | 'dead'

export type EventType =
  | 'internal_event'
  | 'send'
  | 'receive'
  | 'message_held'
  | 'message_delivered'
  | 'clock_update'
  | 'snapshot_start'
  | 'marker_sent'
  | 'marker_received'
  | 'local_snapshot'
  | 'snapshot_complete'
  | 'conflict'
  | 'resolved'
  | 'process_spawned'
  | 'process_killed'
  | 'scenario_step'

export type VectorClock = Record<string, number>
export type MatrixClock = Record<string, VectorClock>

export interface MessagePayload {
  id: string
  from: string
  to: string
  data?: unknown
  sentVc?: VectorClock
  sentLc?: number
  isMarker?: boolean
  snapshotId?: string
}

export interface SnapshotPayload {
  snapshotId: string
  processId?: string
  complete?: boolean
}

export interface ConflictPayload {
  key: string
  siblings?: unknown
}

// Main event type (avoids collision with DOM Event)
export interface DHTEvent {
  id: string
  type: EventType
  processId: string
  timestamp: string
  localSeq: number
  globalSeq: number
  // X1/X4: Go *uint64 serializes as null (not undefined) when nil; accept both.
  lamportClock?: number | null
  vectorClock?: VectorClock | null
  matrixClock?: MatrixClock | null
  message?: MessagePayload
  snapshot?: SnapshotPayload
  conflict?: ConflictPayload
  narration?: string
}

export interface ProcessState {
  id: string
  clockType: ClockType
  deliveryMode: DeliveryMode
  lamportClock: number
  vectorClock?: VectorClock
  matrixClock?: MatrixClock
  peers: string[]
  eventCount: number
  heldMessages: number
  status: ProcessStatus
}

export interface SimConfig {
  clockType: ClockType
  deliveryMode: DeliveryMode
  channels: string
}

export interface SimulationState {
  processes: Map<string, ProcessState>
  events: DHTEvent[]
  activeSnapshot: string | null
  config: SimConfig
}

export interface DiagramLayout {
  laneY: Map<string, number>
  positions: Map<string, { x: number; y: number }>
  processOrder: string[]
}
