// App bootstrap — connects stores, mounts components, starts WebSocket
import { simulationStore, loadInitialState } from './stores/simulation'
import type { DHTEvent, SimulationState } from './api/types'
import { SpaceTimeDiagram } from './components/SpaceTimeDiagram/index'
import { ClockInspector } from './components/ClockInspector/index'
import { CausalDeliveryMonitor } from './components/CausalDelivery/index'
import { ConflictDash } from './components/ConflictDash/index'
import { SnapshotViewer } from './components/SnapshotViewer/index'
import { ScenarioPanel } from './components/ScenarioPanel/index'
import { TheoryCards } from './components/TheoryCards/index'
import { wsConnect } from './ws/socket'
import { spawnProcess, resetSimulation } from './api/client'

// Mount components
const diagramContainer = document.getElementById('space-time-diagram')!
const diagram = new SpaceTimeDiagram(diagramContainer)

const clockInspector = new ClockInspector(
  document.getElementById('clock-inspector-content')!
)

const deliveryMonitor = new CausalDeliveryMonitor(
  document.getElementById('hold-back-queue')!
)

const conflictDash = new ConflictDash(
  document.getElementById('conflict-dash')!
)

const snapshotViewer = new SnapshotViewer(
  document.getElementById('snapshot-viewer')!
)

const scenarioPanel = new ScenarioPanel(
  document.getElementById('scenario-panel')!
)

const theoryCards = new TheoryCards(
  document.getElementById('theory-cards')!
)

// Track processed event IDs to avoid re-processing on every store update
const processedEventIds = new Set<string>()

function processSpecialEvents(events: DHTEvent[]): void {
  for (const event of events) {
    if (processedEventIds.has(event.id)) continue
    processedEventIds.add(event.id)

    if (event.type === 'conflict' && event.conflict) {
      conflictDash.onConflict(event.conflict.key, (event.conflict as any).siblings ?? [])
    }
    if (event.type === 'resolved' && event.conflict) {
      conflictDash.onResolve(event.conflict.key, (event.conflict as any).winner ?? {})
    }
    if (event.type === 'snapshot_complete') {
      const snapId = event.snapshot?.snapshotId ?? event.id
      snapshotViewer.onSnapshotComplete(snapId, (event as any).processes ?? [])
    }
  }
}

// Subscribe to store changes
simulationStore.subscribe(state => {
  diagram.update(state, state.events)
  deliveryMonitor.update(state)
  renderProcessList(state)
  processSpecialEvents(state.events)
})

// ── Toolbar state ────────────────────────────────────────────────────────────

// H17: read from actual DOM select elements instead of a plain object.
const clockTypeSelect = document.getElementById('clock-type-select') as HTMLSelectElement | null
const deliveryModeSelect = document.getElementById('delivery-mode-select') as HTMLSelectElement | null

function getClockType(): string {
  return clockTypeSelect?.value ?? 'vector'
}

function getDeliveryMode(): string {
  return deliveryModeSelect?.value ?? 'causal'
}

// H16: wire up clock-type and delivery-mode change handlers.
clockTypeSelect?.addEventListener('change', () => {
  // Changing clock type only affects future spawned processes.
  // Show user feedback:
  const indicator = document.getElementById('config-status')
  if (indicator) {
    indicator.textContent = `Clock: ${getClockType()} · Mode: ${getDeliveryMode()}`
  }
})

deliveryModeSelect?.addEventListener('change', () => {
  const indicator = document.getElementById('config-status')
  if (indicator) {
    indicator.textContent = `Clock: ${getClockType()} · Mode: ${getDeliveryMode()}`
  }
})

// ── Button handlers ──────────────────────────────────────────────────────────

document.getElementById('btn-internal-event')?.addEventListener('click', async () => {
  const pid = promptPid()
  if (!pid) return
  // H9: check res.ok before parsing
  const res = await fetch('/api/processes/' + pid + '/event', { method: 'POST' })
  if (!res.ok) console.error('internal event failed:', res.status)
})

document.getElementById('btn-send-message')?.addEventListener('click', async () => {
  const from = promptPid('From process:')
  const to = promptPid('To process:')
  if (!from || !to) return
  // H9: check res.ok
  const res = await fetch('/api/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ from, to, data: 'msg-' + Date.now() })
  })
  if (!res.ok) console.error('send message failed:', res.status)
})

document.getElementById('btn-snapshot')?.addEventListener('click', async () => {
  const pid = promptPid('Initiator:')
  if (!pid) return
  // H9: check res.ok
  const res = await fetch('/api/processes/' + pid + '/snapshot', { method: 'POST' })
  if (!res.ok) console.error('snapshot failed:', res.status)
})

document.getElementById('btn-reset')?.addEventListener('click', async () => {
  try {
    await resetSimulation(3, getClockType(), getDeliveryMode())
    await loadInitialState()
  } catch (err) {
    console.error('reset failed:', err)
  }
})

document.getElementById('btn-add-process')?.addEventListener('click', async () => {
  const id = prompt('Process ID (e.g. P4):')
  if (!id) return
  try {
    await spawnProcess(id, getClockType(), getDeliveryMode())
  } catch (err) {
    console.error('spawn failed:', err)
  }
})

// Keyboard shortcuts
document.addEventListener('keydown', (e) => {
  if (e.target instanceof HTMLInputElement || e.target instanceof HTMLSelectElement) return
  switch (e.key.toLowerCase()) {
    case 'i': document.getElementById('btn-internal-event')?.click(); break
    case 's': document.getElementById('btn-send-message')?.click(); break
    case 'n': document.getElementById('btn-snapshot')?.click(); break
  }
})

function promptPid(msg = 'Process ID:'): string | null {
  return prompt(msg)
}

// M14: use SimulationState directly instead of complex conditional inferred type.
function renderProcessList(state: SimulationState): void {
  const container = document.getElementById('process-list')!
  container.innerHTML = ''
  for (const [id, process] of state.processes) {
    const el = document.createElement('div')
    el.className = 'flex items-center justify-between p-2 bg-slate-800 rounded text-xs'
    const vc = process.vectorClock
      ? Object.entries(process.vectorClock).sort(([a],[b])=>a.localeCompare(b)).map(([,v])=>v).join(',')
      : (process.lamportClock ?? '?')

    // M11 (partial): avoid raw innerHTML for dynamic values; use textContent for each span.
    const idSpan = document.createElement('span')
    idSpan.className = 'font-mono font-bold text-slate-200'
    idSpan.textContent = id

    const vcSpan = document.createElement('span')
    vcSpan.className = 'text-slate-400'
    vcSpan.textContent = `[${vc}]`

    const statusSpan = document.createElement('span')
    statusSpan.className = `text-xs px-1 rounded ${process.status === 'running' ? 'bg-green-900 text-green-300' : 'bg-red-900 text-red-300'}`
    statusSpan.textContent = process.status

    el.appendChild(idSpan)
    el.appendChild(vcSpan)
    el.appendChild(statusSpan)
    el.addEventListener('click', () => clockInspector.show(id, process, state.processes))
    container.appendChild(el)
  }
}

// Connect WebSocket and load initial state
wsConnect('/ws')
loadInitialState()
