// Render clock value labels near each event node
import type * as d3 from 'd3'
import type { DHTEvent, DiagramLayout, ClockType } from '../../api/types'

export function renderClockLabels(
  g: d3.Selection<SVGGElement, unknown, null, undefined>,
  events: DHTEvent[],
  layout: DiagramLayout,
  clockType: ClockType
): void {
  const labelData = events
    .filter(e => layout.positions.has(e.id))
    .map(e => {
      const pos = layout.positions.get(e.id)!
      const label = formatClock(e, clockType)
      return { id: e.id, x: pos.x, y: pos.y, label }
    })
    .filter(d => d.label !== '')

  const labels = g.selectAll<SVGTextElement, typeof labelData[0]>('text.clock-label')
    .data(labelData, d => d.id)

  labels.enter()
    .append('text')
    .attr('class', 'clock-label')
    .attr('text-anchor', 'middle')
    .attr('font-size', '9px')
    .attr('fill', '#94a3b8')
    .attr('pointer-events', 'none')
    .attr('x', d => d.x)
    .attr('y', d => d.y - 14)
    .text(d => d.label)
    .merge(labels as unknown as d3.Selection<SVGTextElement, typeof labelData[0], SVGGElement, unknown>)
    .attr('x', d => d.x)
    .attr('y', d => d.y - 14)
    .text(d => d.label)

  labels.exit().remove()
}

function formatClock(e: DHTEvent, clockType: ClockType): string {
  if (clockType === 'lamport' && e.lamportClock != null) {
    return String(e.lamportClock)
  }
  if (clockType === 'vector' && e.vectorClock) {
    const vals = Object.entries(e.vectorClock)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([, v]) => v)
    return '[' + vals.join(',') + ']'
  }
  if (clockType === 'matrix' && e.matrixClock) {
    // Only show own row for brevity
    const row = e.matrixClock[e.processId]
    if (row) {
      const vals = Object.entries(row)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([, v]) => v)
      return '[' + vals.join(',') + ']'
    }
  }
  return ''
}
