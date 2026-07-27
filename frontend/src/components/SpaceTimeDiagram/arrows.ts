import * as d3 from 'd3'
import type { DHTEvent, DiagramLayout } from '../../api/types'

const EVENT_RADIUS = 8

export function renderArrows(
  g: d3.Selection<SVGGElement, unknown, null, undefined>,
  events: DHTEvent[],
  layout: DiagramLayout,
  animatedArrowIds?: Set<string>
): void {
  const byId = new Map<string, DHTEvent>()
  for (const e of events) byId.set(e.id, e)

  const sendByMsgId = new Map<string, DHTEvent>()
  const recvByMsgId = new Map<string, DHTEvent>()

  for (const e of events) {
    if (!e.message) continue
    if (e.type === 'send') sendByMsgId.set(e.message.id, e)
    if (e.type === 'receive' || e.type === 'message_delivered') {
      recvByMsgId.set(e.message.id, e)
    }
  }

  const arrowData: Array<{
    sx: number; sy: number
    tx: number; ty: number
    msgId: string
    isMarker: boolean
  }> = []

  for (const [msgId, sendEvent] of sendByMsgId) {
    const recvEvent = recvByMsgId.get(msgId)
    if (!recvEvent) continue

    const sp = layout.positions.get(sendEvent.id)
    const rp = layout.positions.get(recvEvent.id)
    if (!sp || !rp) continue

    arrowData.push({
      sx: sp.x, sy: sp.y,
      tx: rp.x, ty: rp.y,
      msgId,
      isMarker: sendEvent.message?.isMarker ?? false
    })
  }

  const svg = g.node()?.ownerSVGElement
  if (svg) {
    const svgSel = d3.select(svg) as unknown as d3.Selection<SVGSVGElement, unknown, null, undefined>
    let defs = svgSel.select<SVGDefsElement>('defs')
    if (defs.empty()) defs = svgSel.insert('defs', ':first-child')

    const markerId = 'arrow-head'
    if (defs.select(`#${markerId}`).empty()) {
      defs.append('marker')
        .attr('id', markerId)
        .attr('markerWidth', 8)
        .attr('markerHeight', 6)
        .attr('refX', 6)
        .attr('refY', 3)
        .attr('orient', 'auto')
        .append('polygon')
        .attr('points', '0 0, 8 3, 0 6')
        .attr('fill', '#64748b')

      defs.append('marker')
        .attr('id', 'arrow-head-marker')
        .attr('markerWidth', 8)
        .attr('markerHeight', 6)
        .attr('refX', 6)
        .attr('refY', 3)
        .attr('orient', 'auto')
        .append('polygon')
        .attr('points', '0 0, 8 3, 0 6')
        .attr('fill', '#a855f7')
    }
  }

  const arrows = g.selectAll<SVGPathElement, typeof arrowData[0]>('path.msg-arrow')
    .data(arrowData, d => d.msgId)

  const enter = arrows.enter()
    .append('path')
    .attr('class', d => `msg-arrow ${d.isMarker ? 'marker-arrow' : ''}`)
    .attr('data-msg-id', d => d.msgId)
    .attr('fill', 'none')
    .attr('stroke', d => d.isMarker ? '#a855f7' : '#64748b')
    .attr('stroke-width', d => d.isMarker ? 1.5 : 1)
    .attr('stroke-dasharray', d => d.isMarker ? '4,3' : 'none')
    .attr('opacity', 0.7)
    .attr('marker-end', d => d.isMarker ? 'url(#arrow-head-marker)' : 'url(#arrow-head)')

  // Transit animation: animate from source to dest on new arrows
  enter
    .attr('d', d => {
      const start = `${d.sx},${d.sy}`
      return `M ${d.sx} ${d.sy} L ${d.sx} ${d.sy}`
    })
    .transition()
    .duration(500)
    .ease(d3.easeQuad)
    .attr('d', d => {
      if (animatedArrowIds && !animatedArrowIds.has(d.msgId)) {
        animatedArrowIds.add(d.msgId)
      }
      return cubicPath(d.sx, d.sy, d.tx, d.ty)
    })

  // Flash detection: highlight arrow when message is delivered
  for (const e of events) {
    if ((e.type === 'message_delivered' || e.type === 'receive') && e.message) {
      const existing = g.select<SVGPathElement>(`path.msg-arrow[data-msg-id="${e.message.id}"]`)
      if (!existing.empty()) {
        existing
          .interrupt()
          .attr('stroke', '#facc15')
          .attr('stroke-width', 2.5)
          .transition()
          .duration(600)
          .ease(d3.easeQuad)
          .attr('stroke', d => d.isMarker ? '#a855f7' : '#64748b')
          .attr('stroke-width', d => d.isMarker ? 1.5 : 1)
      }
    }
  }

  enter.merge(arrows as unknown as d3.Selection<SVGPathElement, typeof arrowData[0], SVGGElement, unknown>)
    .attr('d', d => cubicPath(d.sx, d.sy, d.tx, d.ty))
    .attr('data-msg-id', d => d.msgId)

  arrows.exit().remove()
}

function cubicPath(sx: number, sy: number, tx: number, ty: number): string {
  const dx = tx - sx
  const dy = ty - sy
  const len = Math.sqrt(dx * dx + dy * dy) || 1
  const ux = dx / len
  const uy = dy / len

  const x1 = sx + ux * EVENT_RADIUS
  const y1 = sy + uy * EVENT_RADIUS
  const x2 = tx - ux * EVENT_RADIUS
  const y2 = ty - uy * EVENT_RADIUS

  const bow = Math.abs(ty - sy) < 5 ? 20 : 0
  const cx1 = x1 + (x2 - x1) * 0.4
  const cy1 = y1 - bow
  const cx2 = x1 + (x2 - x1) * 0.6
  const cy2 = y2 - bow

  return `M ${x1} ${y1} C ${cx1} ${cy1}, ${cx2} ${cy2}, ${x2} ${y2}`
}