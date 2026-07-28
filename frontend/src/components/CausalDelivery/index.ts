import type { SimulationState, DHTEvent } from '../../api/types'
import { showToast } from '../Toast/index'

function esc(s: string | number | undefined | null): string {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

export class CausalDeliveryMonitor {
  private container: HTMLElement
  private log: DHTEvent[] = []
  private mode: 'causal' | 'immediate' = 'causal'
  private lastState: SimulationState | null = null
  private deliveryCounts = new Map<string, { held: number; delivered: number }>()

  constructor(container: HTMLElement) {
    this.container = container
    this.container.addEventListener('click', (e) => {
      const btn = (e.target as HTMLElement).closest('.delivery-mode-btn') as HTMLElement | null
      if (!btn) return
      const mode = btn.dataset.mode as 'causal' | 'immediate'
      if (mode && mode !== this.mode) {
        const prev = this.mode
        this.mode = mode
        if (this.lastState) this._render(this.lastState)
        fetch('/api/simulation/config', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ deliveryMode: mode })
        }).then(res => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`)
        }).catch(() => {
          this.mode = prev
          if (this.lastState) this._render(this.lastState)
          showToast('Failed to change delivery mode')
        })
      }
    })
  }

  update(state: SimulationState): void {
    this.lastState = state

    const relevant = state.events
      .filter(e => e.type === 'message_held' || e.type === 'message_delivered' || e.type === 'receive')
      .slice(-20)

    this.log = relevant

    this.deliveryCounts = new Map()
    for (const e of state.events) {
      if (e.type === 'message_held' || e.type === 'message_delivered' || e.type === 'receive') {
        const pid = e.processId
        if (!this.deliveryCounts.has(pid)) {
          this.deliveryCounts.set(pid, { held: 0, delivered: 0 })
        }
        const entry = this.deliveryCounts.get(pid)!
        if (e.type === 'message_held') {
          entry.held++
        } else {
          entry.delivered++
        }
      }
    }

    this._render(state)
  }

  private _render(state: SimulationState): void {
    const isCausal = this.mode === 'causal'

    let html = ''

    html += `<div class="flex gap-2 mb-2">`
    html += `<button class="delivery-mode-btn text-xs px-2 py-1 rounded ${isCausal ? 'bg-blue-700 text-white' : 'bg-slate-700 text-slate-400'}" data-mode="causal">Causal</button>`
    html += `<button class="delivery-mode-btn text-xs px-2 py-1 rounded ${!isCausal ? 'bg-blue-700 text-white' : 'bg-slate-700 text-slate-400'}" data-mode="immediate">Immediate</button>`
    html += `</div>`

    if (isCausal) {
      if (this.deliveryCounts.size > 0) {
        let maxTotal = 0
        for (const [, counts] of this.deliveryCounts) {
          const total = counts.held + counts.delivered
          if (total > maxTotal) maxTotal = total
        }
        const barMax = Math.max(maxTotal, 1)

        html += `<div class="mb-3">`
        html += `<div class="text-xs text-slate-500 font-semibold mb-1 uppercase tracking-wide">Delivery Timeline</div>`
        for (const [pid, counts] of this.deliveryCounts) {
          const total = counts.held + counts.delivered
          const heldPct = (counts.held / barMax) * 100
          const delPct = (counts.delivered / barMax) * 100
          html += `<div class="flex items-center gap-2 py-0.5">`
          html += `<span class="font-mono text-slate-300 w-6 text-right">${esc(pid)}</span>`
          html += `<div class="flex-1 h-3 bg-slate-800 rounded-sm overflow-hidden flex">`
          if (counts.held > 0) {
            html += `<div class="bg-red-600 h-full" style="width:${heldPct}%"></div>`
          }
          if (counts.delivered > 0) {
            html += `<div class="bg-green-600 h-full" style="width:${delPct}%"></div>`
          }
          html += `</div>`
          html += `<span class="text-xs text-slate-400 w-28 text-right">${esc(counts.held)} held / ${esc(total)} total</span>`
          html += `</div>`
        }
        html += `</div>`
      }

      const heldMap = new Map<string, number>()
      for (const [pid, proc] of state.processes) {
        if (proc.heldMessages > 0) heldMap.set(pid, proc.heldMessages)
      }

      if (heldMap.size > 0) {
        html += `<div class="mb-3">`
        html += `<div class="text-xs text-red-400 font-semibold mb-1">Held Messages</div>`
        for (const [pid, count] of heldMap) {
          html += `<div class="flex justify-between items-center py-0.5">
            <span class="font-mono text-slate-300">${esc(pid)}</span>
            <span class="bg-red-900 text-red-300 px-2 rounded text-xs">${esc(count)} held</span>
          </div>`
        }
        html += `</div>`
      }

      if (this.log.length > 0) {
        html += `<div class="text-xs text-slate-500 font-semibold mb-1 uppercase tracking-wide">Recent</div>`
        for (const e of [...this.log].reverse()) {
          const isHeld = e.type === 'message_held'
          const isDelivered = e.type === 'message_delivered'
          const dot = isHeld ? '🔴' : isDelivered ? '🟢' : '🔵'
          const msgId = esc(e.message?.id?.slice(0, 8) ?? '?')
          const from = esc(e.message?.from ?? '?')
          const pid = esc(e.processId)
          html += `<div class="flex items-start gap-1 py-0.5 border-b border-slate-800">
            <span>${dot}</span>
            <div class="flex-1 min-w-0">
              <span class="font-mono text-slate-300">${pid}</span>
              <span class="text-slate-500"> ← </span>
              <span class="font-mono text-slate-400">${from}</span>
              <span class="text-slate-600 ml-1 truncate">${msgId}</span>
            </div>
          </div>`
        }
      }
    } else {
      html += `<div class="text-xs text-slate-400 italic mb-2">Immediate delivery — no hold-back queue</div>`
      html += `<div class="text-xs text-slate-500 font-semibold mb-1 uppercase tracking-wide">Timeline</div>`
      const receives = state.events.filter(e => e.type === 'receive')
      if (receives.length === 0) {
        html += `<span class="text-slate-600">No messages received yet</span>`
      } else {
        for (const e of receives) {
          const from = esc(e.message?.from ?? '?')
          const to = esc(e.processId)
          const msgId = esc(e.message?.id?.slice(0, 8) ?? '?')
          const ts = esc(new Date(e.timestamp).toLocaleTimeString())
          html += `<div class="flex items-start gap-1 py-0.5 border-b border-slate-800">
            <span class="text-blue-400">📨</span>
            <div class="flex-1 min-w-0">
              <span class="font-mono text-slate-300">${from}</span>
              <span class="text-slate-500"> → </span>
              <span class="font-mono text-slate-400">${to}</span>
              <span class="text-slate-600 ml-1">${ts}</span>
              <span class="text-slate-600 ml-1 truncate">${msgId}</span>
            </div>
          </div>`
        }
      }
    }

    if (!html) {
      html = '<span class="text-slate-600">No held messages</span>'
    }

    this.container.innerHTML = html
  }
}
