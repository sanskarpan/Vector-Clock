// Nano store for simulation state
import { atom } from 'nanostores'
import type { SimulationState, ProcessState, DHTEvent, SimConfig } from '../api/types'

const defaultConfig: SimConfig = {
  clockType: 'vector',
  deliveryMode: 'causal',
  channels: 'full_mesh'
}

const defaultState: SimulationState = {
  processes: new Map(),
  events: [],
  activeSnapshot: null,
  config: defaultConfig
}

export const simulationStore = atom<SimulationState>(defaultState)

// isValidEvent performs basic structural validation on a WebSocket message.
// M15: prevents crashes from malformed server messages.
export function isValidEvent(obj: unknown): obj is DHTEvent {
  if (!obj || typeof obj !== 'object') return false
  const e = obj as Record<string, unknown>
  return typeof e['id'] === 'string' && typeof e['type'] === 'string' && typeof e['processId'] === 'string'
}

// Apply a server event to the store
export function applyEvent(event: DHTEvent): void {
  const state = simulationStore.get()
  const newState = { ...state, events: [...state.events, event] }

  // Update process state based on event
  if (event.type === 'process_spawned') {
    const newProcesses = new Map(state.processes)
    const pid = event.processId
    newProcesses.set(pid, {
      id: pid,
      clockType: state.config.clockType,
      deliveryMode: state.config.deliveryMode,
      // X2: lamportClock may be null from Go *uint64 — normalise to 0.
      lamportClock: event.lamportClock ?? 0,
      // X2: vectorClock may be null before first clock event — use existing or empty map.
      vectorClock: event.vectorClock ?? undefined,
      matrixClock: event.matrixClock ?? undefined,
      peers: [],
      eventCount: 0,
      heldMessages: 0,
      status: 'running'
    })
    newState.processes = newProcesses
  } else if (event.type === 'process_killed') {
    const newProcesses = new Map(state.processes)
    const existing = newProcesses.get(event.processId)
    if (existing) {
      newProcesses.set(event.processId, { ...existing, status: 'dead' })
    }
    newState.processes = newProcesses
  } else if (event.type === 'snapshot_start' && event.snapshot?.snapshotId) {
    newState.activeSnapshot = event.snapshot.snapshotId
  } else if (event.type === 'snapshot_complete') {
    newState.activeSnapshot = null
  } else if (event.vectorClock != null || event.lamportClock != null) {
    // Update clock state for the process
    const newProcesses = new Map(state.processes)
    const existing = newProcesses.get(event.processId)
    if (existing) {
      newProcesses.set(event.processId, {
        ...existing,
        lamportClock: event.lamportClock ?? existing.lamportClock,
        vectorClock: event.vectorClock ?? existing.vectorClock,
        matrixClock: event.matrixClock ?? existing.matrixClock,
        eventCount: existing.eventCount + 1,
        heldMessages: event.type === 'message_held'
          ? existing.heldMessages + 1
          : event.type === 'message_delivered'
            ? Math.max(0, existing.heldMessages - 1)
            : existing.heldMessages
      })
    }
    newState.processes = newProcesses
  }

  simulationStore.set(newState)
}

// Load initial state from REST API
export async function loadInitialState(): Promise<void> {
  try {
    const res = await fetch('/api/simulation/state')
    if (!res.ok) return
    // M16: validate shape of REST response before casting.
    const raw = await res.json()
    if (!raw || typeof raw !== 'object') return
    const data = raw as { processes?: unknown[]; config?: Partial<SimConfig> }

    const processes = new Map<string, ProcessState>()
    if (Array.isArray(data.processes)) {
      for (const p of data.processes) {
        if (p && typeof p === 'object') {
          const ps = p as ProcessState
          if (typeof ps.id === 'string') {
            processes.set(ps.id, ps)
          }
        }
      }
    }

    const cfg: SimConfig = {
      clockType: (data.config?.clockType as SimConfig['clockType']) ?? defaultConfig.clockType,
      deliveryMode: (data.config?.deliveryMode as SimConfig['deliveryMode']) ?? defaultConfig.deliveryMode,
      channels: data.config?.channels ?? defaultConfig.channels
    }

    simulationStore.set({
      processes,
      events: [],
      activeSnapshot: null,
      config: cfg
    })
  } catch {
    // Server not yet available
  }
}
