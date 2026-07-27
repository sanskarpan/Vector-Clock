// D3-powered Space-Time Diagram: process lanes, event circles, message arrows
import * as d3 from 'd3'
import type { DHTEvent, SimulationState, DiagramLayout, ClockType } from '../../api/types'
import { computeLayout, computeCausalHistory } from './layout'
import { renderArrows } from './arrows'
import { renderClockLabels } from './clocks'

const EVENT_RADIUS = 8
const LANE_HEIGHT = 80
const LABEL_WIDTH = 60

// Color map by event type
const EVENT_COLORS: Record<string, string> = {
  internal_event:      '#3b82f6',  // blue
  send:                '#22c55e',  // green
  receive:             '#f59e0b',  // amber
  message_held:        '#ef4444',  // red
  message_delivered:   '#22c55e',  // green
  snapshot_start:      '#a855f7',  // purple
  marker_sent:         '#a855f7',  // purple
  marker_received:     '#c084fc',  // light purple
  local_snapshot:      '#818cf8',  // indigo
  snapshot_complete:   '#6366f1',  // indigo
  conflict:            '#f97316',  // orange
  resolved:            '#84cc16',  // lime
  process_spawned:     '#06b6d4',  // cyan
  process_killed:      '#94a3b8',  // slate
  clock_update:        '#64748b',  // slate (dimmed)
}

function eventColor(type: string): string {
  return EVENT_COLORS[type] ?? '#64748b'
}

export class SpaceTimeDiagram {
  private svg: d3.Selection<SVGSVGElement, unknown, null, undefined>
  private zoomGroup: d3.Selection<SVGGElement, unknown, null, undefined>
  private laneGroup: d3.Selection<SVGGElement, unknown, null, undefined>
  private arrowGroup: d3.Selection<SVGGElement, unknown, null, undefined>
  private eventGroup: d3.Selection<SVGGElement, unknown, null, undefined>
  private labelGroup: d3.Selection<SVGGElement, unknown, null, undefined>
  private clockGroup: d3.Selection<SVGGElement, unknown, null, undefined>
  private cutGroup: d3.Selection<SVGGElement, unknown, null, undefined>
  private tooltip: d3.Selection<HTMLDivElement, unknown, null, undefined>
  private zoom: d3.ZoomBehavior<SVGSVGElement, unknown>
  private width = 800
  private height = 500

  // Cut line state
  private cutData: { processId: string; localSeq: number }[] | null = null
  private cutVisible = false

  // Causal highlight state
  private selectedEventId: string | null = null

  // Timeline scrubber state
  private scrubberContainer: HTMLDivElement
  private scrubber: HTMLInputElement
  private stepLabel: HTMLSpanElement
  private playBtn: HTMLButtonElement
  private currentStep = 0
  private totalSteps = 0
  private playing = false
  private playInterval: ReturnType<typeof setInterval> | null = null

  // Track which arrow IDs have been animated (transit animation runs once)
  private animatedArrowIds = new Set<string>()

  constructor(container: HTMLElement) {
    this.svg = d3.select(container)
      .append('svg')
      .attr('width', '100%')
      .attr('height', '100%')
      .style('background', '#0f172a')

    const ro = new ResizeObserver(entries => {
      for (const entry of entries) {
        this.width = entry.contentRect.width
        this.height = entry.contentRect.height
      }
    })
    ro.observe(container)
    this.width = container.clientWidth
    this.height = container.clientHeight

    this.zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.2, 5])
      .on('zoom', (event: d3.D3ZoomEvent<SVGSVGElement, unknown>) => {
        this.zoomGroup.attr('transform', event.transform.toString())
      })

    this.svg.call(this.zoom)

    this.zoomGroup = this.svg.append('g').attr('class', 'zoom-root')
    this.laneGroup = this.zoomGroup.append('g').attr('class', 'lanes')
    this.arrowGroup = this.zoomGroup.append('g').attr('class', 'arrows')
    this.cutGroup = this.zoomGroup.append('g').attr('class', 'cut-line').style('display', 'none')
    this.eventGroup = this.zoomGroup.append('g').attr('class', 'events')
    this.labelGroup = this.zoomGroup.append('g').attr('class', 'process-labels')
    this.clockGroup = this.zoomGroup.append('g').attr('class', 'clocks')

    this.tooltip = d3.select(container)
      .append('div')
      .attr('class', 'diagram-tooltip')
      .style('position', 'absolute')
      .style('pointer-events', 'none')
      .style('background', '#1e293b')
      .style('border', '1px solid #334155')
      .style('border-radius', '6px')
      .style('padding', '8px 12px')
      .style('font-size', '11px')
      .style('color', '#e2e8f0')
      .style('max-width', '260px')
      .style('line-height', '1.6')
      .style('opacity', '0')
      .style('transition', 'opacity 0.1s')
      .style('z-index', '10')

    // ── Cut line toggle button ──
    const toolbar = document.getElementById('diagram-toolbar')
    if (toolbar) {
      const cutBtn = document.createElement('button')
      cutBtn.id = 'btn-show-cut'
      cutBtn.className = 'px-3 py-1 bg-red-800 hover:bg-red-700 rounded text-xs text-red-200'
      cutBtn.textContent = 'Show Cut'
      cutBtn.addEventListener('click', () => {
        this.cutVisible = !this.cutVisible
        cutBtn.textContent = this.cutVisible ? 'Hide Cut' : 'Show Cut'
        cutBtn.className = this.cutVisible
          ? 'px-3 py-1 bg-red-700 hover:bg-red-600 rounded text-xs text-red-100'
          : 'px-3 py-1 bg-red-800 hover:bg-red-700 rounded text-xs text-red-200'
        this._updateCutVisibility()
      })
      toolbar.appendChild(cutBtn)
    }

    // ── Timeline scrubber ──
    this.scrubberContainer = document.createElement('div')
    this.scrubberContainer.className = 'flex items-center gap-3 px-4 py-2 border-t border-slate-700 bg-slate-800/50 text-xs'
    this.scrubberContainer.style.display = 'none'

    const playLabel = document.createElement('span')
    playLabel.className = 'text-slate-400'
    playLabel.textContent = 'Step:'
    this.scrubberContainer.appendChild(playLabel)

    this.playBtn = document.createElement('button')
    this.playBtn.id = 'btn-play-scrubber'
    this.playBtn.className = 'px-2 py-1 bg-slate-700 hover:bg-slate-600 rounded text-xs text-slate-300'
    this.playBtn.textContent = '\u25B6'
    this.playBtn.addEventListener('click', () => this._togglePlay())
    this.scrubberContainer.appendChild(this.playBtn)

    this.scrubber = document.createElement('input')
    this.scrubber.type = 'range'
    this.scrubber.min = '0'
    this.scrubber.max = '0'
    this.scrubber.value = '0'
    this.scrubber.className = 'flex-1 accent-blue-500'
    this.scrubber.addEventListener('input', () => {
      this.currentStep = parseInt(this.scrubber.value)
      this.stepLabel.textContent = `${this.currentStep} / ${this.totalSteps}`
    })
    this.scrubberContainer.appendChild(this.scrubber)

    this.stepLabel = document.createElement('span')
    this.stepLabel.className = 'text-slate-400 w-20 text-right'
    this.stepLabel.textContent = '0 / 0'
    this.scrubberContainer.appendChild(this.stepLabel)

    const diagramParent = container.parentElement
    if (diagramParent) {
      diagramParent.appendChild(this.scrubberContainer)
    }
  }

  update(state: SimulationState, events: DHTEvent[]): void {
    const processes = Array.from(state.processes.values())
    const layout = computeLayout(events, processes, { clockType: state.config.clockType })
    this._render(events, layout, state.config.clockType)
  }

  private _render(events: DHTEvent[], layout: DiagramLayout, clockType: ClockType): void {
    this._renderLanes(layout)
    this._renderProcessLabels(layout)
    renderArrows(this.arrowGroup, events, layout, this.animatedArrowIds)
    // Update scrubber BEFORE _renderEvents so currentStep reflects the new
    // maximum before the event filter runs (otherwise new events at globalSeq N
    // are filtered out on the render cycle they arrive because currentStep
    // was still at N-1 when the filter executes).
    this._updateScrubber(events)
    this._renderEvents(events, layout)
    renderClockLabels(this.clockGroup, events, layout, clockType)
    this._renderCutLine(layout)
  }

  private _renderLanes(layout: DiagramLayout): void {
    // Compute total width needed
    const maxX = this._maxX(layout)

    const laneData = layout.processOrder.map(pid => ({
      pid,
      y: layout.laneY.get(pid) ?? 0
    }))

    const lines = this.laneGroup.selectAll<SVGLineElement, typeof laneData[0]>('line.lane')
      .data(laneData, d => d.pid)

    lines.enter()
      .append('line')
      .attr('class', 'lane')
      .attr('stroke', '#1e293b')
      .attr('stroke-width', 1)
      .merge(lines as unknown as d3.Selection<SVGLineElement, typeof laneData[0], SVGGElement, unknown>)
      .attr('x1', LABEL_WIDTH)
      .attr('y1', d => d.y)
      .attr('x2', Math.max(maxX + 120, this.width))
      .attr('y2', d => d.y)

    lines.exit().remove()

    // Lane background bands
    const bands = this.laneGroup.selectAll<SVGRectElement, typeof laneData[0]>('rect.lane-band')
      .data(laneData, d => d.pid)

    bands.enter()
      .append('rect')
      .attr('class', 'lane-band')
      .attr('fill', (_d, i) => i % 2 === 0 ? '#0f172a' : '#111827')
      .attr('opacity', 0.5)
      .merge(bands as unknown as d3.Selection<SVGRectElement, typeof laneData[0], SVGGElement, unknown>)
      .attr('x', 0)
      .attr('y', d => d.y - LANE_HEIGHT / 2)
      .attr('width', Math.max(maxX + 120, this.width))
      .attr('height', LANE_HEIGHT)

    bands.exit().remove()
  }

  private _renderProcessLabels(layout: DiagramLayout): void {
    const labelData = layout.processOrder.map(pid => ({
      pid,
      y: layout.laneY.get(pid) ?? 0
    }))

    const labels = this.labelGroup.selectAll<SVGTextElement, typeof labelData[0]>('text.proc-label')
      .data(labelData, d => d.pid)

    labels.enter()
      .append('text')
      .attr('class', 'proc-label')
      .attr('font-size', '12px')
      .attr('font-weight', '600')
      .attr('fill', '#94a3b8')
      .attr('text-anchor', 'end')
      .merge(labels as unknown as d3.Selection<SVGTextElement, typeof labelData[0], SVGGElement, unknown>)
      .attr('x', LABEL_WIDTH - 8)
      .attr('y', d => d.y + 4)
      .text(d => d.pid)

    labels.exit().remove()
  }

  private _renderEvents(events: DHTEvent[], layout: DiagramLayout): void {
    // Filter by scrubber step
    const filtered = this.totalSteps > 0
      ? events.filter(e => e.globalSeq <= this.currentStep)
      : events

    const nodeData = filtered.filter(e => layout.positions.has(e.id))

    const circles = this.eventGroup.selectAll<SVGCircleElement, DHTEvent>('circle.event-node')
      .data(nodeData, d => d.id)

    const entered = circles.enter()
      .append('circle')
      .attr('class', 'event-node')
      .attr('r', 0)
      .attr('stroke', '#0f172a')
      .attr('stroke-width', 1.5)
      .attr('cursor', 'pointer')

    entered
      .transition()
      .duration(300)
      .attr('r', EVENT_RADIUS)

    entered.merge(circles as unknown as d3.Selection<SVGCircleElement, DHTEvent, SVGGElement, unknown>)
      .attr('cx', d => layout.positions.get(d.id)!.x)
      .attr('cy', d => layout.positions.get(d.id)!.y)
      .attr('fill', d => {
        if (this.selectedEventId === d.id) return '#fbbf24'
        return eventColor(d.type)
      })
      .attr('opacity', d => {
        if (!this.selectedEventId) return 1
        if (this.selectedEventId === d.id) return 1
        const hist = this._causalHistory
        if (!hist) return 0.3
        if (hist.causes.has(d.id) || hist.effects.has(d.id)) return 1
        return 0.3
      })
      .attr('r', d => this.selectedEventId === d.id ? EVENT_RADIUS * 1.6 : EVENT_RADIUS)
      .on('mouseover', null)
      .on('mouseout', null)
      .on('mouseover', (event: MouseEvent, d: DHTEvent) => {
        this._showTooltip(event, d, layout)
        if (this.selectedEventId !== d.id) {
          d3.select(event.currentTarget as SVGCircleElement)
            .transition().duration(100).attr('r', EVENT_RADIUS * 1.4)
        }
      })
      .on('mouseout', (event: MouseEvent) => {
        this.tooltip.style('opacity', '0')
        if (this.selectedEventId !== d3.select(event.currentTarget as SVGCircleElement).datum()?.id) {
          d3.select(event.currentTarget as SVGCircleElement)
            .transition().duration(100).attr('r', EVENT_RADIUS)
        }
      })
      .on('click', (_event: MouseEvent, d: DHTEvent) => {
        this._selectEvent(d, events, layout)
      })

    circles.exit()
      .transition().duration(200).attr('r', 0)
      .remove()
  }

  private _causalHistory: { causes: Set<string>; effects: Set<string> } | null = null

  private _selectEvent(event: DHTEvent, allEvents: DHTEvent[], layout: DiagramLayout): void {
    if (this.selectedEventId === event.id) {
      this.selectedEventId = null
      this._causalHistory = null
      this._renderEvents(allEvents, layout)
      this.tooltip.style('opacity', '0')
      return
    }
    this.selectedEventId = event.id
    this._causalHistory = computeCausalHistory(event.id, allEvents)
    this._renderEvents(allEvents, layout)
    this._showTooltip({ clientX: 0, clientY: 0 } as MouseEvent, event, layout)
  }

  private _showTooltip(mouseEvent: MouseEvent, event: DHTEvent, layout: DiagramLayout): void {
    const pos = layout.positions.get(event.id)
    const clock = this._clockStr(event)
    const lines = [
      `<b>${event.type}</b> — ${event.processId}`,
      `seq: ${event.globalSeq} | local: ${event.localSeq}`,
      clock ? `clock: ${clock}` : '',
      event.narration ? `<i>${event.narration}</i>` : '',
      event.message ? `msg: ${event.message.id.slice(0, 8)}…` : '',
    ].filter(Boolean).join('<br/>')

    const svgRect = (this.svg.node() as SVGSVGElement).getBoundingClientRect()
    const mx = mouseEvent.clientX - svgRect.left + 12
    const my = mouseEvent.clientY - svgRect.top - 10

    this.tooltip
      .html(lines)
      .style('left', mx + 'px')
      .style('top', my + 'px')
      .style('opacity', '1')

    void pos // suppress unused warning
  }

  private _clockStr(e: DHTEvent): string {
    if (e.lamportClock != null) return String(e.lamportClock)
    if (e.vectorClock) {
      const vals = Object.entries(e.vectorClock).sort(([a],[b])=>a.localeCompare(b)).map(([,v])=>v)
      return '[' + vals.join(',') + ']'
    }
    return ''
  }

  private _maxX(layout: DiagramLayout): number {
    let max = LABEL_WIDTH
    for (const pos of layout.positions.values()) {
      if (pos.x > max) max = pos.x
    }
    return max
  }

  setCutData(cut: { processId: string; localSeq: number }[] | null): void {
    this.cutData = cut
  }

  setCutVisible(visible: boolean): void {
    this.cutVisible = visible
    this._updateCutVisibility()
    const btn = document.getElementById('btn-show-cut')
    if (btn) {
      btn.textContent = visible ? 'Hide Cut' : 'Show Cut'
      btn.className = visible
        ? 'px-3 py-1 bg-red-700 hover:bg-red-600 rounded text-xs text-red-100'
        : 'px-3 py-1 bg-red-800 hover:bg-red-700 rounded text-xs text-red-200'
    }
  }

  setScrubberStep(step: number): void {
    this.currentStep = Math.min(step, this.totalSteps)
    this.scrubber.value = String(this.currentStep)
    this.stepLabel.textContent = `${this.currentStep} / ${this.totalSteps}`
  }

  private _renderCutLine(layout: DiagramLayout): void {
    if (!this.cutData || !this.cutVisible) {
      this.cutGroup.style('display', 'none')
      return
    }
    this.cutGroup.style('display', null)

    const maxX = this._maxX(layout)

    // For each cut entry, find the y position and draw a vertical line
    const cutPoints = this.cutData.map(c => {
      const y = layout.laneY.get(c.processId) ?? 0
      // Find x position from the last event on this process with localSeq <= cut localSeq
      let x = 60
      for (const [id, pos] of layout.positions) {
        if (pos.y === y && id.startsWith(c.processId)) {
          x = Math.max(x, pos.x)
        }
      }
      return { x, y, pid: c.processId }
    })

    const lineData = this.cutGroup.selectAll<SVGLineElement, typeof cutPoints[0]>('line.cut-line-seg')
      .data(cutPoints, d => d.pid)

    lineData.enter()
      .append('line')
      .attr('class', 'cut-line-seg')
      .attr('stroke', '#ef4444')
      .attr('stroke-width', 2)
      .attr('stroke-dasharray', '6,3')
      .attr('opacity', 0.8)
      .merge(lineData as unknown as d3.Selection<SVGLineElement, typeof cutPoints[0], SVGGElement, unknown>)
      .attr('x1', d => d.x)
      .attr('y1', d => d.y - 40)
      .attr('x2', d => d.x)
      .attr('y2', d => d.y + 40)

    lineData.exit().remove()
  }

  private _updateCutVisibility(): void {
    this.cutGroup.style('display', this.cutVisible && this.cutData ? null : 'none')
  }

  private _updateScrubber(events: DHTEvent[]): void {
    const maxStep = events.length > 0 ? Math.max(...events.map(e => e.globalSeq)) : 0
    if (maxStep !== this.totalSteps) {
      // If the user was at the live end (or diagram was empty), keep them there.
      // This ensures new events are immediately visible rather than hidden behind
      // a scrubber stuck at step 0.
      const wasAtEnd = this.currentStep === this.totalSteps
      this.totalSteps = maxStep
      this.scrubber.max = String(maxStep)
      if (wasAtEnd || this.currentStep > maxStep) {
        this.currentStep = maxStep
        this.scrubber.value = String(maxStep)
      }
      this.stepLabel.textContent = `${this.currentStep} / ${this.totalSteps}`
      this.scrubberContainer.style.display = maxStep > 0 ? 'flex' : 'none'
    }
  }

  private _togglePlay(): void {
    if (this.playing) {
      this.playing = false
      this.playBtn.textContent = '\u25B6'
      if (this.playInterval) {
        clearInterval(this.playInterval)
        this.playInterval = null
      }
      return
    }

    if (this.currentStep >= this.totalSteps) {
      this.currentStep = 0
      this.scrubber.value = '0'
      this.stepLabel.textContent = `0 / ${this.totalSteps}`
    }

    this.playing = true
    this.playBtn.textContent = '\u23F8'
    this.playInterval = setInterval(() => {
      if (this.currentStep < this.totalSteps) {
        this.currentStep++
        this.scrubber.value = String(this.currentStep)
        this.stepLabel.textContent = `${this.currentStep} / ${this.totalSteps}`
      } else {
        this._togglePlay()
      }
    }, 500)
  }
}
