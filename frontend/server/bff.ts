// Elysia BFF — REST proxy + WebSocket fan-out hub
import { Elysia } from 'elysia'
import { cors } from '@elysiajs/cors'
import type { ServerWebSocket as BunServerWebSocket } from 'bun'

const GO_BACKEND_URL = process.env['GO_BACKEND'] ?? 'http://localhost:8080'

// WebSocket client pool: one connection to Go, many browser clients
let goWS: WebSocket | null = null

// Use a loose interface that matches what Elysia hands us. Bun's native
// ServerWebSocket<TContext> is structurally compatible for the methods we
// call (send, close).
interface MinimalWS {
  send(data: string | ArrayBuffer | Uint8Array): void;
  close(): void;
}

const browserClients = new Set<MinimalWS>()

// H11: null-safe helper — only sends if goWS is connected.
function sendToGo(data: string): void {
  if (goWS?.readyState === WebSocket.OPEN) {
    goWS.send(data)
  }
}

function connectToGo() {
  const wsUrl = GO_BACKEND_URL.replace('http', 'ws') + '/ws'
  try {
    goWS = new WebSocket(wsUrl)
    goWS.onmessage = (e) => {
      // Fan-out to all browser clients
      for (const client of browserClients) {
        try { client.send(e.data as string) } catch { /* client gone */ }
      }
    }
    goWS.onerror = () => { /* reconnect on close */ }
    goWS.onclose = () => {
      goWS = null
      setTimeout(connectToGo, 2000)
    }
  } catch {
    setTimeout(connectToGo, 2000)
  }
}

export const app = new Elysia()
  .use(cors())

  // Proxy all REST requests to Go backend.
  // The Authorization header is forwarded so that when the Go backend has
  // VC_API_TOKENS configured (auth enabled), browser clients can authenticate
  // through the BFF rather than requiring a separate service token.
  .all('/api/*', async ({ request, params }) => {
    const search = new URL(request.url).search
    const url = `${GO_BACKEND_URL}/api/v1/${(params as Record<string,'*'>)['*']}${search}`
    const headers: Record<string, string> = { 'Content-Type': 'application/json' }
    const auth = request.headers.get('Authorization')
    if (auth) headers['Authorization'] = auth
    const res = await fetch(url, {
      method: request.method,
      headers,
      body: ['GET', 'HEAD'].includes(request.method) ? undefined : await request.text()
    })
    const data = await res.text()
    return new Response(data, {
      status: res.status,
      headers: { 'Content-Type': 'application/json' }
    })
  })

  // WebSocket hub
  .ws('/ws', {
    open(ws) {
      browserClients.add(ws)
      sendToGo(JSON.stringify({ action: 'subscribe', types: ['*'] }))
    },
    close(ws) {
      browserClients.delete(ws)
    },
    message(_ws, msg) {
      // H11: sendToGo guards against null goWS during reconnect window.
      sendToGo(JSON.stringify(msg))
    }
  })

  // Serve static frontend files
  .get('/', () => Bun.file('./index.html'))
  .get('/src/*', async ({ params }) => {
    // L12: check file exists before serving; return 404 if not found.
    const file = Bun.file(`./src/${(params as Record<string,'*'>)['*']}`)
    if (!(await file.exists())) {
      return new Response('Not found', { status: 404 })
    }
    return file
  })
  .get('/node_modules/*', async ({ params }) => {
    const file = Bun.file(`./node_modules/${(params as Record<string,'*'>)['*']}`)
    if (!(await file.exists())) {
      return new Response('Not found', { status: 404 })
    }
    return file
  })

  .listen(3001)

connectToGo()

console.log(`BFF running on http://localhost:3001 (proxying to ${GO_BACKEND_URL})`)

// Export type for Eden Treaty
export type App = typeof app
