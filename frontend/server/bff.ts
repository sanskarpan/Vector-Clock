// BFF — REST proxy + WebSocket fan-out hub
// Uses native Bun.serve() so HTML imports trigger Bun's bundler, which
// transpiles TypeScript and processes CSS (including Tailwind v4) automatically.
import index from '../index.html'

const GO_BACKEND_URL = process.env['GO_BACKEND'] ?? 'http://localhost:8080'
const ALLOWED_ORIGINS = (process.env['VC_ALLOWED_ORIGINS'] ?? '').split(',').filter(Boolean)

// WebSocket client pool: one upstream connection to Go, N browser clients
let goWS: WebSocket | null = null

interface MinimalWS {
  send(data: string | ArrayBuffer | Uint8Array): void
  close(): void
}

const browserClients = new Set<MinimalWS>()

function sendToGo(data: string): void {
  if (goWS?.readyState === WebSocket.OPEN) {
    goWS.send(data)
  }
}

function connectToGo() {
  const wsUrl = GO_BACKEND_URL.replace(/^http/, 'ws') + '/ws'
  try {
    goWS = new WebSocket(wsUrl)
    goWS.onmessage = (e) => {
      for (const client of browserClients) {
        try { client.send(e.data as string) } catch { /* client gone */ }
      }
    }
    goWS.onerror = () => { /* handled by onclose */ }
    goWS.onclose = () => {
      goWS = null
      setTimeout(connectToGo, 2000)
    }
  } catch {
    setTimeout(connectToGo, 2000)
  }
}

// REST proxy helper — forwards Authorization header for token-auth paths
async function proxyRest(req: Request): Promise<Response> {
  const url = new URL(req.url)
  const target = `${GO_BACKEND_URL}/api/v1${url.pathname.replace(/^\/api/, '')}${url.search}`
  const headers: Record<string, string> = { 'Content-Type': 'application/json' }
  const auth = req.headers.get('Authorization')
  if (auth) headers['Authorization'] = auth
  try {
    const res = await fetch(target, {
      method: req.method,
      headers,
      body: ['GET', 'HEAD'].includes(req.method) ? undefined : await req.text(),
    })
    const body = await res.text()
    return new Response(body, {
      status: res.status,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    })
  } catch (err) {
    return new Response(JSON.stringify({ error: String(err) }), {
      status: 502,
      headers: { 'Content-Type': 'application/json' },
    })
  }
}

Bun.serve({
  port: 3001,

  // HTML import — Bun's bundler transpiles TypeScript, bundles modules, and
  // processes CSS (including @import "tailwindcss") automatically.
  static: {
    '/': index,
  },

  async fetch(req, server) {
    const url = new URL(req.url)

    // CORS preflight
    if (req.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        },
      })
    }

    // WebSocket upgrade
    if (url.pathname === '/ws') {
      const origin = req.headers.get('origin') ?? ''
      if (
        ALLOWED_ORIGINS.length > 0 &&
        origin !== '' &&
        !ALLOWED_ORIGINS.includes(origin)
      ) {
        return new Response('Forbidden', { status: 403 })
      }
      const ok = server.upgrade(req, { data: {} })
      if (!ok) return new Response('WebSocket upgrade failed', { status: 500 })
      return undefined as unknown as Response
    }

    // REST proxy
    if (url.pathname.startsWith('/api/')) {
      return proxyRest(req)
    }

    // Healthz passthrough
    if (url.pathname === '/healthz') {
      try {
        const res = await fetch(`${GO_BACKEND_URL}/healthz`)
        const body = await res.text()
        return new Response(body, { status: res.status })
      } catch {
        return new Response('{"status":"backend_unreachable"}', { status: 502 })
      }
    }

    // Static files — Bun's bundler resolves HTML imports above;
    // for explicit asset requests fall through to 404.
    return new Response('Not found', { status: 404 })
  },

  websocket: {
    open(ws) {
      browserClients.add(ws as unknown as MinimalWS)
      sendToGo(JSON.stringify({ action: 'subscribe', types: ['*'] }))
    },
    close(ws) {
      browserClients.delete(ws as unknown as MinimalWS)
    },
    message(_ws, msg) {
      sendToGo(typeof msg === 'string' ? msg : JSON.stringify(msg))
    },
  },
})

connectToGo()

console.log(`BFF running on http://localhost:3001 (→ ${GO_BACKEND_URL})`)
