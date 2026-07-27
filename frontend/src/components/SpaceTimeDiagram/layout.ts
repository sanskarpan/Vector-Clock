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
  const byId = new Map<string, DHTEvent>()
  for (const e of events) byId.set(e.id, e)

  // Build: msgId → send event, msgId → receive event
  const sendByMsg = new Map<string, DHTEvent>()
  const recvByMsg = new Map<string, DHTEvent>()
  for (const e of events) {
    if (!e.message) continue
    if (e.type === 'send') sendByMsg.set(e.message.id, e)
    if (e.type === 'receive' || e.type === 'message_delivered') recvByMsg.set(e.message.id, e)
  }

  // Predecessors of an event = same-process events earlier + the send for any receive
  function predecessors(e: DHTEvent): string[] {
    const preds: string[] = []
    for (const other of events) {
      if (other.processId === e.processId && other.globalSeq < e.globalSeq) {
        preds.push(other.id)
      }
    }
    if ((e.type === 'receive' || e.type === 'message_delivered') && e.message) {
      const send = sendByMsg.get(e.message.id)
      if (send) preds.push(send.id)
    }
    return preds
  }

  // Successors of an event = same-process events later + the receive for any send
  function successors(e: DHTEvent): string[] {
    const succs: string[] = []
    for (const other of events) {
      if (other.processId === e.processId && other.globalSeq > e.globalSeq) {
        succs.push(other.id)
      }
    }
    if (e.type === 'send' && e.message) {
      const recv = recvByMsg.get(e.message.id)
      if (recv) succs.push(recv.id)
    }
    return succs
  }

  const target = byId.get(eventId)
  if (!target) return { causes: new Set(), effects: new Set() }

  // BFS backward to find all causal ancestors
  const causes = new Set<string>()
  const queue: string[] = predecessors(target)
  while (queue.length > 0) {
    const cur = queue.pop()!
    if (cur === eventId || causes.has(cur)) continue
    causes.add(cur)
    const e = byId.get(cur)
    if (e) queue.push(...predecessors(e))
  }

  // BFS forward to find all causal effects
  const effects = new Set<string>()
  const fqueue: string[] = successors(target)
  while (fqueue.length > 0) {
    const cur = fqueue.pop()!
    if (cur === eventId || effects.has(cur)) continue
    effects.add(cur)
    const e = byId.get(cur)
    if (e) fqueue.push(...successors(e))
  }

  return { causes, effects }
}
