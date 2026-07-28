import { showToast } from '../Toast/index'
import { simulationStore } from '../../stores/simulation'

function esc(s: string | number | undefined | null): string {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

interface FaultState {
  delays: Record<string, number>
  drops: string[]
  partition: string[][] | null
}

export class NetworkFaultsPanel {
  private container: HTMLElement
  private faults: FaultState = { delays: {}, drops: [], partition: null }
  private pollTimer: ReturnType<typeof setInterval> | null = null

  constructor(container: HTMLElement) {
    this.container = container
    this._render()
    this.container.addEventListener('click', e => { void this._onClick(e) })
    this.container.addEventListener('change', e => { void this._onChange(e) })
    // Poll fault state every 2s — cheap GET, needed since faults can change via scenarios too
    this.pollTimer = setInterval(() => { void this._fetchFaults() }, 2000)
    void this._fetchFaults()
  }

  destroy(): void {
    if (this.pollTimer !== null) clearInterval(this.pollTimer)
  }

  // Called by store subscribe to re-render process list when processes change
  refresh(): void {
    this._render()
  }

  private async _fetchFaults(): Promise<void> {
    try {
      const res = await fetch('/api/faults')
      if (!res.ok) return
      this.faults = await res.json() as FaultState
      this._render()
    } catch { /* backend not yet ready */ }
  }

  private _render(): void {
    const state = simulationStore.get()
    const pids = [...state.processes.keys()].sort()

    let html = ''

    // ── Per-pair channel controls ─────────────────────────────────────────────
    html += '<div class="text-xs text-slate-500 font-semibold uppercase tracking-wide mb-2">Channels</div>'

    if (pids.length < 2) {
      html += '<div class="text-xs text-slate-600 mb-3">Spawn at least 2 processes</div>'
    } else {
      for (const from of pids) {
        for (const to of pids) {
          if (from === to) continue
          const key = `${from}→${to}`
          const delayMs = this.faults.delays[key] ?? 0
          const hasDrop = this.faults.drops.includes(key)

          html += `<div class="mb-2 bg-slate-800 rounded p-2" data-channel="${esc(key)}">
            <div class="flex items-center justify-between mb-1">
              <span class="font-mono text-slate-300 text-xs">${esc(from)} → ${esc(to)}</span>
              ${hasDrop ? '<span class="text-xs bg-red-900 text-red-300 px-1 rounded">DROP</span>' : ''}
            </div>
            <div class="flex items-center gap-2 mb-1">
              <label class="text-xs text-slate-500 w-8 shrink-0">Delay</label>
              <input type="range" class="fault-delay-slider flex-1" min="0" max="500" step="10"
                value="${esc(delayMs)}" data-from="${esc(from)}" data-to="${esc(to)}">
              <span class="text-xs text-slate-400 w-10 text-right">${esc(delayMs)}ms</span>
            </div>
            <button class="fault-drop-btn text-xs w-full rounded py-0.5 ${
              hasDrop
                ? 'bg-red-800 hover:bg-red-700 text-red-200'
                : 'bg-slate-700 hover:bg-slate-600 text-slate-300'
            }" data-from="${esc(from)}" data-to="${esc(to)}" data-active="${hasDrop}">
              ${hasDrop ? 'Cancel drop-next' : 'Drop next message'}
            </button>
          </div>`
        }
      }
    }

    html += '<button id="fault-clear-all" class="w-full bg-slate-700 hover:bg-slate-600 text-slate-300 text-xs rounded px-2 py-1 mb-4">Clear all faults</button>'

    // ── Partition ─────────────────────────────────────────────────────────────
    html += '<div class="text-xs text-slate-500 font-semibold uppercase tracking-wide mb-1">Partition</div>'

    if (this.faults.partition && this.faults.partition.length > 0) {
      const groups = this.faults.partition.map(g => g.join(', ')).join(' | ')
      html += `<div class="bg-red-950 border border-red-800 rounded p-2 mb-1 text-xs text-red-300">Active: ${esc(groups)}</div>`
      html += '<button id="fault-heal-btn" class="w-full bg-green-900 hover:bg-green-800 text-green-300 text-xs rounded px-2 py-1">Heal partition</button>'
    } else {
      html += '<div class="text-xs text-slate-600 mb-1">No active partition</div>'
      if (pids.length >= 2) {
        html += '<div class="text-xs text-slate-500 mb-1">Select processes for side A:</div>'
        html += '<div class="flex flex-wrap gap-2 mb-2" id="partition-side-a">'
        for (const p of pids) {
          html += `<label class="flex items-center gap-1 text-xs cursor-pointer">
            <input type="checkbox" class="partition-a-check" value="${esc(p)}">
            <span class="font-mono text-slate-300">${esc(p)}</span>
          </label>`
        }
        html += '</div>'
        html += '<button id="fault-partition-btn" class="w-full bg-orange-900 hover:bg-orange-800 text-orange-200 text-xs rounded px-2 py-1">Create partition</button>'
      }
    }

    this.container.innerHTML = html
  }

  private async _onChange(e: Event): Promise<void> {
    const target = e.target as HTMLInputElement
    if (!target.classList.contains('fault-delay-slider')) return

    const from = target.dataset.from!
    const to = target.dataset.to!
    const delayMs = parseInt(target.value, 10)

    // Update the live label next to the slider
    const label = target.nextElementSibling as HTMLElement | null
    if (label) label.textContent = `${delayMs}ms`

    // Update local cache so re-render doesn't flicker
    const key = `${from}→${to}`
    if (delayMs === 0) {
      delete this.faults.delays[key]
    } else {
      this.faults.delays[key] = delayMs
    }

    try {
      const res = await fetch('/api/faults/delay', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ from, to, delayMs }),
      })
      if (!res.ok) showToast(`Delay failed (${res.status})`)
    } catch {
      showToast('Delay request failed')
    }
  }

  private async _onClick(e: MouseEvent): Promise<void> {
    const target = e.target as HTMLElement

    // Drop-next button
    const dropBtn = target.closest('.fault-drop-btn') as HTMLElement | null
    if (dropBtn) {
      const from = dropBtn.dataset.from!
      const to = dropBtn.dataset.to!
      const isActive = dropBtn.dataset.active === 'true'

      if (isActive) {
        // No cancel-drop endpoint — clear all and re-apply current delays
        await fetch('/api/faults', { method: 'DELETE' })
        for (const [key, ms] of Object.entries(this.faults.delays)) {
          const [f, t] = key.split('→')
          if (f && t) {
            await fetch('/api/faults/delay', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ from: f, to: t, delayMs: ms }),
            })
          }
        }
      } else {
        const res = await fetch('/api/faults/drop', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ from, to }),
        })
        if (!res.ok) { showToast(`Drop failed (${res.status})`); return }
      }
      await this._fetchFaults()
      return
    }

    // Clear all
    if (target.closest('#fault-clear-all')) {
      await fetch('/api/faults', { method: 'DELETE' })
      await this._fetchFaults()
      return
    }

    // Heal partition
    if (target.closest('#fault-heal-btn')) {
      const res = await fetch('/api/faults/partition', { method: 'DELETE' })
      if (!res.ok) showToast(`Heal failed (${res.status})`)
      await this._fetchFaults()
      return
    }

    // Create partition
    if (target.closest('#fault-partition-btn')) {
      const checks = [...this.container.querySelectorAll('.partition-a-check:checked')] as HTMLInputElement[]
      const sideA = checks.map(c => c.value)
      if (sideA.length === 0) {
        showToast('Select at least one process for side A', 'info')
        return
      }
      const state = simulationStore.get()
      const allPids = [...state.processes.keys()].sort()
      const sideB = allPids.filter(p => !sideA.includes(p))
      if (sideB.length === 0) {
        showToast('Side B is empty — add more processes first', 'info')
        return
      }
      const res = await fetch('/api/faults/partition', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sideA, sideB }),
      })
      if (!res.ok) showToast(`Partition failed (${res.status})`)
      await this._fetchFaults()
    }
  }
}
