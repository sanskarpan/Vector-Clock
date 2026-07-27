const BASE = '/api'

// H9: checkOk throws on non-2xx responses so callers don't get garbage JSON.
async function checkOk(res: Response): Promise<Response> {
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`HTTP ${res.status}: ${text}`)
  }
  return res
}

export async function getSimulationState() {
  const res = await fetch(`${BASE}/simulation/state`)
  await checkOk(res)
  return res.json()
}

export async function startSimulation(processCount: number, clockType: string, deliveryMode: string) {
  const res = await fetch(`${BASE}/simulation/start`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ processCount, clockType, deliveryMode })
  })
  await checkOk(res)
  return res.json()
}

export async function resetSimulation(processCount?: number, clockType?: string, deliveryMode?: string) {
  const res = await fetch(`${BASE}/simulation/reset`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ processCount: processCount ?? 3, clockType: clockType ?? 'vector', deliveryMode: deliveryMode ?? 'causal' })
  })
  await checkOk(res)
  return res.json()
}

export async function spawnProcess(id: string, clockType: string, deliveryMode: string) {
  const res = await fetch(`${BASE}/processes`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id, clockType, deliveryMode })
  })
  await checkOk(res)
  return res.json()
}

export async function sendInternalEvent(pid: string) {
  const res = await fetch(`${BASE}/processes/${pid}/event`, { method: 'POST' })
  await checkOk(res)
  return res.json()
}

export async function sendMessage(from: string, to: string, data: unknown) {
  const res = await fetch(`${BASE}/messages`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ from, to, data })
  })
  await checkOk(res)
  return res.json()
}

export async function triggerSnapshot(pid: string) {
  const res = await fetch(`${BASE}/processes/${pid}/snapshot`, { method: 'POST' })
  await checkOk(res)
  return res.json()
}

export async function listScenarios() {
  const res = await fetch(`${BASE}/scenarios`)
  await checkOk(res)
  return res.json()
}

export async function runScenario(name: string) {
  const res = await fetch(`${BASE}/scenarios/${name}/run`, { method: 'POST' })
  await checkOk(res)
  return res.json()
}

// H7: KV conflict-detection endpoints
export async function kvWrite(key: string, value: Uint8Array, authorId: string, contextVc?: Record<string, number>) {
  const res = await fetch(`${BASE}/kv/${encodeURIComponent(key)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ value: Array.from(value), authorId, contextVc: contextVc ?? {} })
  })
  await checkOk(res)
  return res.json()
}

export async function kvRead(key: string) {
  const res = await fetch(`${BASE}/kv/${encodeURIComponent(key)}`)
  await checkOk(res)
  return res.json()
}

export async function kvResolve(key: string, strategy: 'last_writer_wins' | 'first_writer_wins' | 'keep_all') {
  const res = await fetch(`${BASE}/kv/${encodeURIComponent(key)}/resolve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ strategy })
  })
  await checkOk(res)
  return res.json()
}
