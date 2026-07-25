import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

const eventsRate = new Rate('events_per_second');
const snapshotRate = new Rate('snapshot_completion_rate');
const msgSendDuration = new Trend('msg_send_duration');
const scenarioDuration = new Trend('scenario_duration');

const thresholds = {
  http_req_failed: ['rate<0.05'],
  http_req_duration: ['p(95)<2000'],
  events_per_second: ['rate>0'],
  snapshot_completion_rate: ['rate>0'],
};

export const options = {};

function healthCheck() {
  const res = http.get(`${BASE_URL}/healthz`);
  check(res, {
    'healthz status is 200': (r) => r.status === 200,
    'healthz has status ok': (r) => JSON.parse(r.body).status === 'ok',
  });
}

function getState() {
  const res = http.get(`${BASE_URL}/api/v1/simulation/state`);
  check(res, {
    'state status is 200': (r) => r.status === 200,
    'state has processes array': (r) => Array.isArray(JSON.parse(r.body).processes),
    'state has eventCount': (r) => typeof JSON.parse(r.body).eventCount === 'number',
  });
  return JSON.parse(res.body);
}

function getProcess(pid) {
  const res = http.get(`${BASE_URL}/api/v1/processes/${pid}`);
  check(res, {
    'get process status is 200': (r) => r.status === 200,
    'process has correct id': (r) => JSON.parse(r.body).id === pid,
    'process has clockType': (r) => ['vector', 'lamport', 'matrix'].includes(JSON.parse(r.body).clockType),
  });
}

function sendMessage(from, to) {
  const start = Date.now();
  const payload = JSON.stringify({ from, to, data: { text: `msg-${Date.now()}` } });
  const params = { headers: { 'Content-Type': 'application/json' } };
  const res = http.post(`${BASE_URL}/api/v1/messages`, payload, params);
  msgSendDuration.add(Date.now() - start);
  check(res, {
    'send message status is 200': (r) => r.status === 200,
    'send message returns ok': (r) => JSON.parse(r.body).ok === true,
  });
}

function triggerSnapshot(pid) {
  const res = http.post(`${BASE_URL}/api/v1/processes/${pid}/snapshot`);
  const ok = check(res, {
    'snapshot trigger status is 200': (r) => r.status === 200,
    'snapshot trigger has snapshotId': (r) => typeof JSON.parse(r.body).snapshotId === 'string',
  });
  if (ok) {
    const snapId = JSON.parse(res.body).snapshotId;
    snapshotRate.add(1);
    return snapId;
  }
  snapshotRate.add(0);
  return null;
}

function runScenario(name) {
  const start = Date.now();
  const res = http.post(`${BASE_URL}/api/v1/scenarios/${name}/run`);
  scenarioDuration.add(Date.now() - start);
  check(res, {
    [`scenario ${name} run status is 200`]: (r) => r.status === 200,
    [`scenario ${name} returns running`]: (r) => JSON.parse(r.body).running === name,
  });
}

function spawnProcess(id) {
  const payload = JSON.stringify({ id, clockType: 'vector', deliveryMode: 'causal' });
  const params = { headers: { 'Content-Type': 'application/json' } };
  const res = http.post(`${BASE_URL}/api/v1/processes`, payload, params);
  check(res, {
    'spawn process status is 201': (r) => r.status === 201,
    'spawn process returns id': (r) => JSON.parse(r.body).id === id,
  });
}

function killProcess(id) {
  const res = http.del(`${BASE_URL}/api/v1/processes/${id}`);
  check(res, {
    'kill process status is 200': (r) => r.status === 200,
    'kill process returns killed': (r) => JSON.parse(r.body).killed === id,
  });
}

function listScenarios() {
  const res = http.get(`${BASE_URL}/api/v1/scenarios`);
  check(res, {
    'list scenarios status is 200': (r) => r.status === 200,
    'scenarios is array': (r) => Array.isArray(JSON.parse(r.body)),
  });
}

export function setup() {
  healthCheck();
  const state = getState();
  if (!state.processes || state.processes.length === 0) {
    console.log('No processes found, running ConcurrentWrites scenario to set up...');
    runScenario('ConcurrentWrites');
    sleep(1);
  }
}

function standardWorkload() {
  group('health check', () => {
    healthCheck();
  });

  group('state and processes', () => {
    const state = getState();
    if (state.processes && state.processes.length > 0) {
      const pids = state.processes.map((p) => p.id);
      for (const pid of pids) {
        getProcess(pid);
      }
    }
  });

  group('send messages', () => {
    const state = getState();
    if (state.processes && state.processes.length >= 2) {
      const pids = state.processes.map((p) => p.id);
      for (let i = 0; i < Math.min(pids.length, 3); i++) {
        for (let j = 0; j < pids.length; j++) {
          if (i !== j) {
            sendMessage(pids[i], pids[j]);
            eventsRate.add(1);
          }
        }
      }
    }
  });

  group('list scenarios', () => {
    listScenarios();
  });
}

export default function () {
  standardWorkload();
}

// ── Smoke Test ─────────────────────────────────────────────────
export function smoke() {
  const vu = __VU;
  if (vu !== 1) return;
  group('smoke-test', () => {
    standardWorkload();
    const state = getState();
    if (state.processes && state.processes.length >= 1) {
      triggerSnapshot(state.processes[0].id);
    }
    runScenario('ConcurrentWrites');
  });
}

// ── Load Test ──────────────────────────────────────────────────
export function load() {
  group('load-test', () => {
    standardWorkload();
    const state = getState();
    if (state.processes && state.processes.length >= 2) {
      sendMessage(state.processes[0].id, state.processes[1].id);
    }
  });
}

// ── Stress Test ────────────────────────────────────────────────
export function stress() {
  group('stress-test', () => {
    for (let i = 0; i < 5; i++) {
      standardWorkload();
      const state = getState();
      if (state.processes && state.processes.length >= 2) {
        sendMessage(state.processes[0].id, state.processes[1].id);
      }
    }
  });
}

// ── Soak Test ──────────────────────────────────────────────────
export function soak() {
  group('soak-test', () => {
    standardWorkload();
    runScenario('Snapshot3P');
  });
}
