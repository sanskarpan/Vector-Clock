import type { DHTEvent } from '../../api/types'

function esc(s: string | number | undefined | null): string {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

interface SnapshotProcessData {
  processId: string
  role: 'initiator' | 'participant'
  localState?: string
  recordedChannels?: Record<string, unknown>
}

interface ChannelState {
  from: string
  to: string
  messages: string[]
}

interface SnapshotDetail {
  snapshotId: string
  timestamp: string
  processes: SnapshotProcessData[]
  channels: ChannelState[]
}

export class SnapshotViewer {
  private container: HTMLElement
  private contentContainer: HTMLElement
  private currentSnapID: string | null = null
  private currentData: SnapshotDetail | null = null

  constructor(container: HTMLElement) {
    this.container = container

    this.container.innerHTML = `
      <div class="flex items-center justify-between mb-3">
        <h2 class="text-sm font-semibold text-slate-300">Snapshot Viewer</h2>
        <button id="snapshot-verify-btn" class="px-2 py-1 bg-green-800 hover:bg-green-700 rounded text-xs text-green-200 hidden">Verify</button>
      </div>
      <div id="snapshot-content" class="space-y-3"></div>
    `

    this.contentContainer = container.querySelector('#snapshot-content')!

    container.querySelector('#snapshot-verify-btn')!.addEventListener('click', () => {
      if (this.currentSnapID) {
        this.verifySnapshot(this.currentSnapID)
      }
    })
  }

  onSnapshotComplete(snapID: string, processes: any[]): void {
    this.currentSnapID = snapID

    const procData: SnapshotProcessData[] = (processes ?? []).map((p: any) => ({
      processId: p.processId ?? p.id ?? '?',
      role: p.role === 'initiator' ? 'initiator' : 'participant',
      localState: p.localState ?? p.state ?? '-',
      recordedChannels: p.recordedChannels ?? p.channels ?? undefined
    }))

    const channelSet = new Map<string, ChannelState>()
    for (const p of procData) {
      if (p.recordedChannels) {
        for (const [chKey, msgs] of Object.entries(p.recordedChannels)) {
          const [from, to] = chKey.split('->')
          channelSet.set(chKey, {
            from: from ?? '?',
            to: to ?? '?',
            messages: Array.isArray(msgs) ? msgs.map(m => String(m)) : []
          })
        }
      }
    }

    this.currentData = {
      snapshotId: snapID,
      timestamp: new Date().toISOString(),
      processes: procData,
      channels: Array.from(channelSet.values())
    }

    this.render(snapID, procData)
    this.showVerifyBtn(true)
  }

  private showVerifyBtn(show: boolean): void {
    const btn = this.container.querySelector('#snapshot-verify-btn')!
    btn.className = show
      ? 'px-2 py-1 bg-green-800 hover:bg-green-700 rounded text-xs text-green-200'
      : 'px-2 py-1 bg-green-800 hover:bg-green-700 rounded text-xs text-green-200 hidden'
  }

  private render(snapID: string, processes: SnapshotProcessData[]): void {
    const ts = this.currentData?.timestamp ?? new Date().toISOString()
    const displayTime = new Date(ts).toLocaleTimeString()

    let html = ''

    html += `<div class="bg-slate-800 rounded p-3 space-y-2">
      <div class="flex justify-between items-center">
        <span class="text-xs text-slate-400 uppercase tracking-wide">Snapshot</span>
        <span class="font-mono text-xs text-slate-500">${esc(displayTime)}</span>
      </div>
      <div class="font-mono text-xs text-slate-200 break-all">${esc(snapID)}</div>
    </div>`

    if (processes.length > 0) {
      html += `<div class="text-xs text-slate-400 font-semibold uppercase tracking-wide mb-1">Processes</div>`
      html += `<div class="space-y-2">`
      for (const p of processes) {
        const roleBadge = p.role === 'initiator'
          ? '<span class="bg-purple-900 text-purple-300 px-1.5 py-0.5 rounded text-xs">initiator</span>'
          : '<span class="bg-blue-900 text-blue-300 px-1.5 py-0.5 rounded text-xs">participant</span>'
        html += `<div class="bg-slate-800 rounded p-2 text-xs space-y-1">
          <div class="flex items-center gap-2">
            <span class="font-mono text-slate-200">${esc(p.processId)}</span>
            ${roleBadge}
          </div>
          <div class="text-slate-400"><span class="text-slate-500">State:</span> <span class="font-mono">${esc(p.localState)}</span></div>
        </div>`
      }
      html += `</div>`
    }

    const channels = this.currentData?.channels ?? []
    if (channels.length > 0) {
      html += `<div class="text-xs text-slate-400 font-semibold uppercase tracking-wide mt-3 mb-1">Channel States</div>`
      html += `<div class="space-y-2">`
      for (const ch of channels) {
        const msgCount = ch.messages.length
        html += `<div class="bg-slate-800 rounded p-2 text-xs">
          <div class="flex items-center gap-2 mb-1">
            <span class="font-mono text-slate-300">${esc(ch.from)}</span>
            <span class="text-slate-600">→</span>
            <span class="font-mono text-slate-300">${esc(ch.to)}</span>
            <span class="ml-auto text-slate-500">${esc(msgCount)} in-transit</span>
          </div>
          ${msgCount > 0 ? `<div class="text-slate-400">${esc(ch.messages.join(', '))}</div>` : '<div class="text-slate-600 italic">(empty)</div>'}
        </div>`
      }
      html += `</div>`
    }

    this.contentContainer.innerHTML = html
  }

  private async verifySnapshot(snapID: string): Promise<void> {
    const btn = this.container.querySelector('#snapshot-verify-btn') as HTMLButtonElement
    if (btn) {
      btn.textContent = 'Verifying…'
      btn.disabled = true
    }

    try {
      const res = await fetch(`/api/snapshots/${encodeURIComponent(snapID)}/verify`)
      if (!res.ok) {
        this.renderConsistencyProof(false, 'Server returned ' + res.status)
        return
      }
      const result = await res.json()
      const consistent = result.consistent === true
      this.renderConsistencyProof(consistent, result.explanation ?? (consistent ? 'All channel states recorded correctly.' : 'Inconsistent snapshot detected.'))
    } catch {
      this.renderConsistencyProof(false, 'Could not verify snapshot.')
    } finally {
      if (btn) {
        btn.textContent = 'Verify'
        btn.disabled = false
      }
    }
  }

  private renderConsistencyProof(consistent: boolean, explanation: string): void {
    const existing = this.contentContainer.querySelector('#snapshot-consistency')
    if (existing) existing.remove()

    const div = document.createElement('div')
    div.id = 'snapshot-consistency'
    div.className = `rounded p-3 text-xs ${consistent ? 'bg-green-900/50 border border-green-700' : 'bg-red-900/50 border border-red-700'}`
    div.innerHTML = `
      <div class="flex items-center gap-2 mb-1">
        <span class="font-bold ${consistent ? 'text-green-300' : 'text-red-300'}">${consistent ? '✓ Consistent' : '✗ Inconsistent'}</span>
      </div>
      <div class="${consistent ? 'text-green-400' : 'text-red-400'}">${esc(explanation)}</div>
    `
    this.contentContainer.appendChild(div)
  }

  clear(): void {
    this.currentSnapID = null
    this.currentData = null
    this.contentContainer.innerHTML = '<span class="text-slate-600 text-xs">No snapshots yet</span>'
    this.showVerifyBtn(false)
  }
}
