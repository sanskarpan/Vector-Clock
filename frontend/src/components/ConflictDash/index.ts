import type { DHTEvent } from '../../api/types'

function esc(s: string | number | undefined | null): string {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

interface ConflictEntry {
  timestamp: string
  key: string
  strategy: string
  winnerAuthor?: string
}

interface VersionInfo {
  id: string
  author: string
  time: number
  value: unknown
  parents: string[]
}

export class ConflictDash {
  private container: HTMLElement
  private logContainer: HTMLElement
  private dagContainer: HTMLElement
  private toggleContainer: HTMLElement
  private viewMode: 'vv' | 'dvv' = 'vv'
  private currentVersions: VersionInfo[] = []
  private log: ConflictEntry[] = []

  constructor(container: HTMLElement) {
    this.container = container

    this.container.innerHTML = `
      <div class="flex items-center justify-between mb-3">
        <h2 class="text-sm font-semibold text-slate-300">Conflict Dashboard</h2>
        <div class="flex gap-2 text-xs">
          <button id="conflict-vv-toggle" class="px-2 py-1 rounded bg-blue-700 text-blue-200">VV</button>
          <button id="conflict-dvv-toggle" class="px-2 py-1 rounded bg-slate-700 text-slate-400">DVV</button>
        </div>
      </div>
      <div id="conflict-dag" class="mb-4 overflow-x-auto"></div>
      <div class="flex items-center gap-2 mb-2">
        <span class="text-xs text-slate-500 uppercase tracking-wide">Resolution Log</span>
      </div>
      <div id="conflict-log" class="max-h-48 overflow-y-auto space-y-1"></div>
    `

    this.dagContainer = container.querySelector('#conflict-dag')!
    this.logContainer = container.querySelector('#conflict-log')!
    this.toggleContainer = container

    this.toggleContainer.querySelector('#conflict-vv-toggle')!.addEventListener('click', () => {
      this.viewMode = 'vv'
      this.updateToggleButtons()
      this.renderDAG(this.currentVersions)
    })
    this.toggleContainer.querySelector('#conflict-dvv-toggle')!.addEventListener('click', () => {
      this.viewMode = 'dvv'
      this.updateToggleButtons()
      this.renderDAG(this.currentVersions)
    })
  }

  private updateToggleButtons(): void {
    const vvBtn = this.toggleContainer.querySelector('#conflict-vv-toggle')!
    const dvvBtn = this.toggleContainer.querySelector('#conflict-dvv-toggle')!
    vvBtn.className = `px-2 py-1 rounded text-xs ${this.viewMode === 'vv' ? 'bg-blue-700 text-blue-200' : 'bg-slate-700 text-slate-400'}`
    dvvBtn.className = `px-2 py-1 rounded text-xs ${this.viewMode === 'dvv' ? 'bg-blue-700 text-blue-200' : 'bg-slate-700 text-slate-400'}`
  }

  onConflict(key: string, versions: any[]): void {
    const parsedVersions: VersionInfo[] = (versions ?? []).map((v: any, i: number) => ({
      id: v.id ?? `v${i}`,
      author: v.author ?? v.processId ?? '?',
      time: v.time ?? Date.now(),
      value: v.value ?? v.data,
      parents: v.parents ?? []
    }))
    parsedVersions.sort((a, b) => a.time - b.time)
    this.currentVersions = parsedVersions
    this.renderDAG(parsedVersions)
  }

  onResolve(key: string, winner: any): void {
    const entry: ConflictEntry = {
      timestamp: new Date().toISOString(),
      key,
      strategy: winner.strategy ?? 'manual',
      winnerAuthor: winner.author ?? winner.processId
    }
    this.log.unshift(entry)
    this.renderLog()
  }

  private renderLog(): void {
    if (this.log.length === 0) {
      this.logContainer.innerHTML = '<span class="text-slate-600 text-xs">No conflicts resolved yet</span>'
      return
    }
    let html =
      '<div class="grid grid-cols-4 gap-2 text-xs text-slate-500 font-semibold uppercase tracking-wide px-2 py-1 border-b border-slate-700">' +
      '<span>Time</span><span>Key</span><span>Strategy</span><span>Winner</span></div>'
    for (const entry of this.log) {
      const time = new Date(entry.timestamp).toLocaleTimeString()
      html += `<div class="grid grid-cols-4 gap-2 text-xs px-2 py-1 border-b border-slate-800 items-center">
        <span class="text-slate-500">${esc(time)}</span>
        <span class="font-mono text-slate-200">${esc(entry.key)}</span>
        <span class="text-slate-400">${esc(entry.strategy)}</span>
        <span class="font-mono text-slate-300">${esc(entry.winnerAuthor ?? '-')}</span>
      </div>`
    }
    this.logContainer.innerHTML = html
  }

  private renderDAG(versions: VersionInfo[]): void {
    if (versions.length === 0) {
      this.dagContainer.innerHTML = '<span class="text-slate-600 text-xs">No versions</span>'
      return
    }

    const nodeW = 24
    const nodeGap = 80
    const laneH = 40
    const paddingX = 20
    const paddingY = 20

    const authors = [...new Set(versions.map(v => v.author))].sort()
    const authorColors: Record<string, string> = {}
    const palette = ['#60a5fa', '#f472b6', '#34d399', '#fbbf24', '#a78bfa', '#fb923c']
    authors.forEach((a, i) => { authorColors[a] = palette[i % palette.length] })

    const totalW = versions.length * nodeGap + paddingX * 2
    const totalH = authors.length * laneH + paddingY * 2

    let svg = `<svg width="${totalW}" height="${totalH}" class="min-w-full" style="min-width:${totalW}px">`

    const getParentIds = (v: VersionInfo): string[] => v.parents ?? []

    for (let i = 0; i < versions.length; i++) {
      const v = versions[i]
      const x = paddingX + i * nodeGap + nodeW / 2
      const authorIdx = authors.indexOf(v.author)
      const y = paddingY + authorIdx * laneH + laneH / 2

      for (const pId of getParentIds(v)) {
        const pIdx = versions.findIndex(vv => vv.id === pId || vv.id.endsWith(pId))
        if (pIdx >= 0) {
          const px = paddingX + pIdx * nodeGap + nodeW / 2
          const pAuthorIdx = authors.indexOf(versions[pIdx].author)
          const py = paddingY + pAuthorIdx * laneH + laneH / 2
          svg += `<line x1="${px}" y1="${py}" x2="${x}" y2="${y}" stroke="#475569" stroke-width="2" opacity="0.6"/>`
        }
      }
    }

    const falseConflictKeys = this.detectFalseConflicts(versions)

    for (let i = 0; i < versions.length; i++) {
      const v = versions[i]
      const x = paddingX + i * nodeGap + nodeW / 2
      const authorIdx = authors.indexOf(v.author)
      const y = paddingY + authorIdx * laneH + laneH / 2
      const color = authorColors[v.author]
      const isFalse = this.viewMode === 'vv' && falseConflictKeys.has(v.id)

      svg += `<circle cx="${x}" cy="${y}" r="${isFalse ? 8 : 6}" fill="${isFalse ? '#f87171' : color}" stroke="${isFalse ? '#dc2626' : '#1e293b'}" stroke-width="2" opacity="${isFalse ? 0.8 : 1}"/>`
      svg += `<text x="${x}" y="${y + nodeW / 2 + 12}" text-anchor="middle" fill="#94a3b8" font-size="10" font-family="monospace">${esc(v.author.slice(0, 4))}</text>`
    }

    svg += '</svg>'
    this.dagContainer.innerHTML = svg
  }

  private detectFalseConflicts(versions: VersionInfo[]): Set<string> {
    const falseSet = new Set<string>()
    if (versions.length < 2) return falseSet

    for (let i = 0; i < versions.length; i++) {
      for (let j = i + 1; j < versions.length; j++) {
        const a = versions[i]
        const b = versions[j]
        if (a.author !== b.author && !this.isCausalDescendant(a, b, versions) && !this.isCausalDescendant(b, a, versions)) {
          falseSet.add(a.id)
          falseSet.add(b.id)
        }
      }
    }
    return falseSet
  }

  private isCausalDescendant(a: VersionInfo, b: VersionInfo, all: VersionInfo[]): boolean {
    const visited = new Set<string>()
    const stack = [b.id]
    while (stack.length > 0) {
      const cur = stack.pop()!
      if (cur === a.id) return true
      if (visited.has(cur)) continue
      visited.add(cur)
      const node = all.find(v => v.id === cur)
      if (node) {
        for (const p of node.parents ?? []) {
          stack.push(p)
        }
      }
    }
    return false
  }

  clear(): void {
    this.currentVersions = []
    this.log = []
    this.dagContainer.innerHTML = '<span class="text-slate-600 text-xs">No versions</span>'
    this.logContainer.innerHTML = '<span class="text-slate-600 text-xs">No conflicts resolved yet</span>'
  }
}
