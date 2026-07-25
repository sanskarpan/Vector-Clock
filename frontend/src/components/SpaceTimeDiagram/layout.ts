import type { DHTEvent, ProcessState, DiagramLayout, ClockType } from '../../api/types'

export interface CausalHistory {
  causes: Set<string>
  effects: Set<string>
}

const LANE_HEIGHT = 80       // pixels between process lanes
const TIME_UNIT = 60         // pixels per logical clock unit
const MIN_SPACING = TIME_UNIT // minimum horizontal gap between events on same lane

interface LayoutConfig {
  clockType: ClockType
}

export function computeLayout(
  events: DHTEvent[],
  processes: ProcessState[],
  config: LayoutConfig
): DiagramLayout {
  const processOrder = processes.map(p => p.id).sort()

  // Map processID → Y coordinate (center of process lane)
  const laneY = new Map<string, number>()
  processOrder.forEach((pid, i) => {
    laneY.set(pid, 50 + i * LANE_HEIGHT)
  })

  // Map eventID → (x, y) position
  const positions = new Map<string, { x: number; y: number }>()

  // Per-process: track last X position to enforce minimum spacing
  const lastX = new Map<string, number>()
  processOrder.forEach(p => lastX.set(p, 60))

  // Sort events by globalSeq for consistent processing
  const sorted = [...events].sort((a, b) => a.globalSeq - b.globalSeq)

  for (const e of sorted) {
    const y = laneY.get(e.processId) ?? (laneY.size + 1) * LANE_HEIGHT

    let x: number
    const clockType = config.clockType
    if (clockType === 'lamport' && e.lamportClock != null) {
      x = 60 + e.lamportClock * TIME_UNIT
    } else if (clockType === 'vector' && e.vectorClock) {
      // Use own component of vector clock for X position
      x = 60 + (e.vectorClock[e.processId] ?? 0) * TIME_UNIT
    } else {
      // Fallback: use localSeq × TIME_UNIT
      x = 60 + e.localSeq * TIME_UNIT
    }

    // Enforce minimum spacing on same process lane
    const minX = (lastX.get(e.processId) ?? 60) + MIN_SPACING
    x = Math.max(x, minX)
    lastX.set(e.processId, x)

    positions.set(e.id, { x, y })
  }

  return { laneY, positions, processOrder }
}

export function computeCausalHistory(eventId: string, events: DHTEvent[]): CausalHistory {
  const target = events.find(e => e.id === eventId)
  if (!target) return { causes: new Set(), effects: new Set() }

  const causes = new Set<string>()
  const effects = new Set<string>()

  for (const e of events) {
    if (e.id === eventId) continue
    if (e.processId === target.processId) {
      if (e.globalSeq < target.globalSeq) {
        causes.add(e.id)
      } else if (e.globalSeq > target.globalSeq) {
        effects.add(e.id)
      }
    }
  }

  return { causes, effects }
}
