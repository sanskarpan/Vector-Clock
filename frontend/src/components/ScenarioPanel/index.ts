// Scenario Panel: lists available scenarios with run buttons
interface ScenarioInfo {
  name: string
  description: string
  stepCount: number
}

// M13: escape HTML special characters to prevent XSS from scenario names/descriptions.
function esc(s: string | number | undefined | null): string {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

export class ScenarioPanel {
  private container: HTMLElement

  constructor(container: HTMLElement) {
    this.container = container
    this._load()
  }

  private async _load(): Promise<void> {
    try {
      const res = await fetch('/api/scenarios')
      if (!res.ok) {
        this._renderError('Server unavailable')
        return
      }
      const scenarios = await res.json() as ScenarioInfo[]
      this._render(scenarios)
    } catch {
      this._renderError('Could not load scenarios')
    }
  }

  private _render(scenarios: ScenarioInfo[]): void {
    if (!scenarios.length) {
      this.container.innerHTML = '<span class="text-slate-600">No scenarios</span>'
      return
    }

    let html = ''
    for (const s of scenarios) {
      // M13: escape name and description from API response.
      const safeName = esc(s.name)
      const safeDisplayName = esc(s.name.replace(/_/g, ' '))
      const safeDesc = esc(s.description)
      const safeSteps = esc(s.stepCount)
      html += `<div class="bg-slate-800 rounded p-3 space-y-1" data-scenario="${safeName}">
        <div class="flex items-center justify-between">
          <span class="font-semibold text-slate-200 text-xs">${safeDisplayName}</span>
          <button class="scenario-run-btn px-2 py-0.5 bg-blue-800 hover:bg-blue-700 rounded text-xs text-blue-200"
                  data-name="${safeName}">Run</button>
        </div>
        <div class="text-slate-400 text-xs leading-relaxed">${safeDesc}</div>
        <div class="text-slate-600 text-xs">${safeSteps} steps</div>
      </div>`
    }
    this.container.innerHTML = html

    // Wire run buttons
    this.container.querySelectorAll<HTMLButtonElement>('.scenario-run-btn').forEach(btn => {
      btn.addEventListener('click', () => this._run(btn.dataset.name!))
    })
  }

  private async _run(name: string): Promise<void> {
    const btn = this.container.querySelector<HTMLButtonElement>(`[data-name="${CSS.escape(name)}"]`)
    if (btn) {
      btn.textContent = 'Running…'
      btn.disabled = true
    }
    try {
      const res = await fetch(`/api/scenarios/${encodeURIComponent(name)}/run`, { method: 'POST' })
      if (!res.ok) console.error('scenario run failed:', res.status)
    } finally {
      if (btn) {
        btn.textContent = 'Run'
        btn.disabled = false
      }
    }
  }

  private _renderError(msg: string): void {
    // Use textContent to avoid injecting the error message into HTML.
    const span = document.createElement('span')
    span.className = 'text-red-500 text-xs'
    span.textContent = msg
    this.container.innerHTML = ''
    this.container.appendChild(span)
  }
}
