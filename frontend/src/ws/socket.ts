// WebSocket connection with automatic reconnect
import { applyEvent, isValidEvent } from '../stores/simulation'

let ws: WebSocket | null = null
let wsUrl = ''
let reconnectTimeout: ReturnType<typeof setTimeout> | null = null

export function wsConnect(path: string): void {
  wsUrl = `ws://${location.host}${path}`
  connect()
}

function connect(): void {
  if (ws?.readyState === WebSocket.OPEN) return

  ws = new WebSocket(wsUrl)

  ws.onopen = () => {
    setStatus('connected')
    ws?.send(JSON.stringify({ action: 'subscribe', types: ['*'] }))
    if (reconnectTimeout) {
      clearTimeout(reconnectTimeout)
      reconnectTimeout = null
    }
  }

  ws.onmessage = (e) => {
    try {
      const parsed = JSON.parse(e.data as string)
      // M15: validate message structure before applying to store.
      if (!isValidEvent(parsed)) return
      applyEvent(parsed)
    } catch {
      // Non-JSON or structurally invalid message — ignore silently.
    }
  }

  ws.onclose = () => {
    setStatus('disconnected')
    ws = null
    reconnectTimeout = setTimeout(connect, 2000)
  }

  ws.onerror = () => {
    ws?.close()
  }
}

function setStatus(status: 'connected' | 'disconnected'): void {
  const el = document.getElementById('ws-status')
  if (!el) return
  el.textContent = status === 'connected' ? 'Connected' : 'Disconnected'
  el.className = status === 'connected'
    ? 'text-xs px-2 py-1 rounded bg-green-900 text-green-300'
    : 'text-xs px-2 py-1 rounded bg-red-900 text-red-300'
}
