import { kvWrite, kvRead, kvResolve } from '../../api/client'
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

interface KVVersion {
  value: number[]
  clock: Record<string, number>
  author: string
  written: string
}

export class KVStorePanel {
  private container: HTMLElement
  private versions: KVVersion[] = []
  private currentKey = ''

  constructor(container: HTMLElement) {
    this.container = container
    this._render()
    this.container.addEventListener('click', e => { void this._onClick(e) })
    this.container.addEventListener('submit', e => { void this._onSubmit(e as SubmitEvent) })
  }

  refresh(): void {
    this._render()
  }

  private _render(): void {
    const state = simulationStore.get()
    const pids = [...state.processes.keys()].filter(
      id => state.processes.get(id)?.status === 'running'
    )

    let html = '<form id="kv-write-form" class="space-y-2 mb-3">'
    html += '<div class="text-xs text-slate-500 font-semibold uppercase tracking-wide mb-1">Write</div>'
    html += '<input id="kv-key-input" name="key" type="text" placeholder="key" autocomplete="off" class="w-full bg-slate-800 text-slate-200 rounded px-2 py-1 text-xs border border-slate-700 focus:outline-none focus:border-blue-500">'
    html += '<input id="kv-val-input" name="value" type="text" placeholder="value" autocomplete="off" class="w-full bg-slate-800 text-slate-200 rounded px-2 py-1 text-xs border border-slate-700 focus:outline-none focus:border-blue-500">'
    html += '<select id="kv-author-select" name="author" class="w-full bg-slate-800 text-slate-200 rounded px-2 py-1 text-xs border border-slate-700">'
    if (pids.length === 0) {
      html += '<option value="">— no running processes —</option>'
    } else {
      for (const p of pids) {
        html += `<option value="${esc(p)}">${esc(p)}</option>`
      }
    }
    html += '</select>'
    html += '<div class="flex gap-2">'
    html += '<button type="submit" class="flex-1 bg-blue-700 hover:bg-blue-600 text-white text-xs rounded px-2 py-1">Write</button>'
    html += '<button type="button" id="kv-read-btn" class="flex-1 bg-slate-700 hover:bg-slate-600 text-slate-200 text-xs rounded px-2 py-1">Read</button>'
    html += '</div>'
    html += '</form>'

    if (this.currentKey) {
      if (this.versions.length === 0) {
        html += `<div class="text-xs text-slate-500">No versions for "${esc(this.currentKey)}"</div>`
      } else {
        const count = this.versions.length
        html += `<div class="text-xs text-slate-500 font-semibold uppercase tracking-wide mb-1">${esc(count)} version${count !== 1 ? 's' : ''} for "${esc(this.currentKey)}"</div>`
        for (const v of this.versions) {
          const val = new TextDecoder().decode(new Uint8Array(v.value))
          const vcStr = Object.entries(v.clock)
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([k, n]) => `${esc(k)}:${esc(n)}`)
            .join(',')
          html += `<div class="bg-slate-800 rounded p-2 mb-1 text-xs">
            <div class="flex justify-between items-center mb-0.5">
              <span class="font-mono text-blue-300 break-all">${esc(val)}</span>
              <span class="text-slate-500 ml-1 shrink-0">${esc(v.author)}</span>
            </div>
            <div class="text-slate-600 font-mono text-xs">[${vcStr}]</div>
          </div>`
        }
        if (count > 1) {
          html += '<div class="text-xs text-slate-500 font-semibold uppercase tracking-wide mb-1 mt-2">Resolve conflict</div>'
          html += '<div class="flex flex-col gap-1">'
          const strategies = [
            ['last_writer_wins', 'Last writer wins'],
            ['first_writer_wins', 'First writer wins'],
            ['keep_all', 'Keep all (no-op)'],
          ] as const
          for (const [s, label] of strategies) {
            html += `<button class="kv-resolve-btn w-full bg-amber-900 hover:bg-amber-800 text-amber-200 text-xs rounded px-2 py-1" data-strategy="${s}">${label}</button>`
          }
          html += '</div>'
        }
      }
    }

    this.container.innerHTML = html
  }

  private async _onClick(e: MouseEvent): Promise<void> {
    const target = e.target as HTMLElement

    if (target.closest('#kv-read-btn')) {
      const key = (this.container.querySelector<HTMLInputElement>('#kv-key-input')?.value ?? '').trim()
      if (!key) { showToast('Enter a key to read', 'info'); return }
      await this._doRead(key)
      return
    }

    const resolveBtn = target.closest('.kv-resolve-btn') as HTMLElement | null
    if (resolveBtn) {
      const strategy = resolveBtn.dataset.strategy as 'last_writer_wins' | 'first_writer_wins' | 'keep_all'
      if (!this.currentKey || !strategy) return
      try {
        await kvResolve(this.currentKey, strategy)
        showToast('Conflict resolved', 'success')
        await this._doRead(this.currentKey)
      } catch (err) {
        showToast(`Resolve failed: ${err instanceof Error ? err.message : String(err)}`)
      }
    }
  }

  private async _onSubmit(e: SubmitEvent): Promise<void> {
    e.preventDefault()
    const key = (this.container.querySelector<HTMLInputElement>('#kv-key-input')?.value ?? '').trim()
    const val = (this.container.querySelector<HTMLInputElement>('#kv-val-input')?.value ?? '').trim()
    const author = (this.container.querySelector<HTMLSelectElement>('#kv-author-select')?.value ?? '').trim()
    if (!key) { showToast('Key is required', 'info'); return }
    if (!author) { showToast('Select an author process', 'info'); return }
    const bytes = new TextEncoder().encode(val)
    try {
      const result = await kvWrite(key, bytes, author)
      const msg = result.conflict ? 'Written — conflict detected!' : 'Written OK'
      showToast(msg, result.conflict ? 'info' : 'success')
      await this._doRead(key)
    } catch (err) {
      showToast(`Write failed: ${err instanceof Error ? err.message : String(err)}`)
    }
  }

  private async _doRead(key: string): Promise<void> {
    try {
      const result = await kvRead(key)
      this.currentKey = key
      this.versions = result.versions ?? []
      this._render()
    } catch {
      showToast(`Key "${key}" not found`, 'info')
    }
  }
}
