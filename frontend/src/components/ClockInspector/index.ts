import type { ProcessState } from '../../api/types'

function esc(s: string | number | undefined | null): string {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

function compareVC(a: Record<string, number>, b: Record<string, number>): 'before' | 'after' | 'concurrent' | 'identical' {
  const keys = new Set([...Object.keys(a), ...Object.keys(b)])
  let aLessB = false, bLessA = false
  for (const k of keys) {
    const av = a[k] ?? 0, bv = b[k] ?? 0
    if (av < bv) aLessB = true
    if (bv < av) bLessA = true
    if (aLessB && bLessA) return 'concurrent'
  }
  if (!aLessB && !bLessA) return 'identical'
  if (aLessB && !bLessA) return 'before'
  if (!aLessB && bLessA) return 'after'
  return 'concurrent'
}

function heatColor(val: number, max: number): string {
  if (max === 0) return 'rgb(30,41,59)'
  const ratio = Math.min(val / max, 1)
  const r = Math.round(15 + (239 - 15) * ratio)
  const g = Math.round(23 + (68 - 23) * (1 - ratio))
  const b = Math.round(42 + (68 - 42) * (1 - ratio))
  return `rgb(${r},${g},${b})`
}

export class ClockInspector {
  private container: HTMLElement

  constructor(container: HTMLElement) {
    this.container = container
  }

  show(pid: string, process: ProcessState, allProcesses: Map<string, ProcessState>): void {
    let html = `<div class="space-y-2">`
    html += `<div class="font-mono font-bold text-slate-200 mb-1">${esc(pid)}</div>`

    // Lamport
    if (process.lamportClock != null) {
      html += `<div class="flex justify-between">
        <span class="text-slate-400">Lamport:</span>
        <span class="font-mono text-amber-300">${esc(process.lamportClock)}</span>
      </div>`
    }

    // Vector clock
    if (process.vectorClock) {
      html += `<div class="mt-2 text-slate-400 text-xs uppercase tracking-wide">Vector Clock</div>`
      html += `<table class="w-full text-xs">`
      for (const [id, val] of Object.entries(process.vectorClock).sort(([a],[b])=>a.localeCompare(b))) {
        const isOwn = id === pid
        html += `<tr>
          <td class="font-mono ${isOwn ? 'text-blue-300 font-bold' : 'text-slate-300'} pr-2">${esc(id)}</td>
          <td class="font-mono text-right ${isOwn ? 'text-blue-300 font-bold' : 'text-slate-200'}">${esc(val)}</td>
        </tr>`
      }
      html += `</table>`

      // Concurrent detector
      html += `<div class="mt-2 text-slate-400 text-xs uppercase tracking-wide">Concurrent Analysis</div>`
      for (const [otherId, otherProcess] of allProcesses) {
        if (otherId === pid) continue
        const otherVc = otherProcess.vectorClock
        if (!otherVc) continue
        const result = compareVC(process.vectorClock, otherVc)
        let label: string
        let css: string
        switch (result) {
          case 'before':
            label = 'Happened Before (this happened first)'
            css = 'text-blue-400'
            break
          case 'after':
            label = 'Happened After (this happened later)'
            css = 'text-green-400'
            break
          case 'concurrent':
            label = 'Concurrent ⚡'
            css = 'text-amber-400'
            break
          case 'identical':
            label = 'Identical'
            css = 'text-slate-400'
            break
        }
        html += `<div class="${css} text-xs"><span class="font-mono">${esc(otherId)}</span> — ${label}</div>`
      }
    }

    // Matrix clock heatmap
    if (process.matrixClock) {
      html += `<div class="mt-2 text-slate-400 text-xs uppercase tracking-wide">Matrix Clock</div>`
      html += `<div class="overflow-x-auto"><table class="text-xs border-collapse">`

      const pids = Object.keys(process.matrixClock).sort()
      let maxVal = 0
      for (const row of pids) {
        for (const col of pids) {
          const val = process.matrixClock[row]?.[col] ?? 0
          if (val > maxVal) maxVal = val
        }
      }

      html += `<tr><th class="pr-1 text-slate-500"></th>`
      for (const col of pids) {
        html += `<th class="px-1 text-slate-400 text-center">${esc(col)}</th>`
      }
      html += `</tr>`

      for (const row of pids) {
        const isOwnRow = row === pid
        html += `<tr>
          <td class="pr-1 font-mono ${isOwnRow ? 'text-blue-300 font-bold' : 'text-slate-400'}">${esc(row)}</td>`
        for (const col of pids) {
          const val = process.matrixClock[row]?.[col] ?? 0
          const isOwnCell = isOwnRow && col === pid
          const bg = heatColor(val, maxVal)
          const fg = val > maxVal * 0.5 ? 'text-slate-100' : 'text-slate-300'
          const diag = isOwnCell ? ' ring-1 ring-blue-400' : ''
          html += `<td class="px-1 text-center font-mono ${fg}${diag}" style="background:${bg}">${esc(val)}</td>`
        }
        html += `</tr>`
      }
      html += `</table></div>`

      html += `<div class="flex justify-between text-xs text-slate-500 mt-1">
        <span>Min: 0</span>
        <span>Max: ${esc(maxVal)}</span>
      </div>`
    }

    html += `<div class="mt-2 flex gap-3 text-slate-400">
      <span>Events: <span class="text-slate-200">${esc(process.eventCount)}</span></span>
      <span>Held: <span class="${process.heldMessages > 0 ? 'text-red-400' : 'text-slate-200'}">${esc(process.heldMessages)}</span></span>
      <span>Status: <span class="${process.status === 'running' ? 'text-green-400' : 'text-red-400'}">${esc(process.status)}</span></span>
    </div>`

    html += `</div>`
    this.container.innerHTML = html
  }

  clear(): void {
    this.container.innerHTML = '<span class="text-slate-500">Select a process</span>'
  }
}
