import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

const faultRecoveryRate = new Rate('fault_recovery_rate');
const processRespawnRate = new Rate('process_respawn_rate');
const rateLimitObserved = new Rate('rate_limit_observed');

const thresholds = {
  http_req_failed: ['rate<0.10'],
  http_req_duration: ['p(95)<5000'],
  fault_recovery_rate: ['rate>0.5'],
  process_respawn_rate: ['rate>0.5'],
};

export const options = {
  stages: [
    { duration: '30s', target: 10 },
    { duration: '1m', target: 20 },
    { duration: '30s', target: 0 },
  ],
  thresholds,
};

function healthCheck() {
  const res = http.get(`${BASE_URL}/healthz`);
  return check(res, {
    'healthz is 200': (r) => r.status === 200,
  });
}

function getState() {
  const res = http.get(`${BASE_URL}/api/v1/simulation/state`);
  if (res.status !== 200) return null;
  return JSON.parse(res.body);
}

function spawnProcess(id, clockType, deliveryMode) {
  const payload = JSON.stringify({
    id,
    clockType: clockType || 'vector',
    deliveryMode: deliveryMode || 'causal',
  });
  const params = { headers: { 'Content-Type': 'application/json' } };
  const res = http.post(`${BASE_URL}/api/v1/processes`, payload, params);
  const ok = check(res, {
    'spawn process is 201': (r) => r.status === 201,
  });
  if (ok) processRespawnRate.add(1);
  return ok;
}

function killProcess(id) {
  const res = http.del(`${BASE_URL}/api/v1/processes/${id}`);
  const ok = check(res, {
    'kill process is 200': (r) => r.status === 200,
  });
  return ok;
}

function sendMessage(from, to) {
  const payload = JSON.stringify({ from, to, data: { text: `chaos-${Date.now()}` } });
  const params = { headers: { 'Content-Type': 'application/json' } };
  const res = http.post(`${BASE_URL}/api/v1/messages`, payload, params);
  return check(res, {
    'send message is 200': (r) => r.status === 200,
  });
}

function runScenario(name) {
  const res = http.post(`${BASE_URL}/api/v1/scenarios/${name}/run`);
  return check(res, {
    [`scenario ${name} is 200`]: (r) => r.status === 200,
  });
}

function injectDelay(from, to, delayMs) {
  const payload = JSON.stringify({ from, to, delayMs });
  const params = { headers: { 'Content-Type': 'application/json' } };
  const res = http.post(`${BASE_URL}/api/v1/faults/delay`, payload, params);
  return check(res, {
    'inject delay is 200': (r) => r.status === 200,
  });
}

function injectDrop(from, to) {
  const payload = JSON.stringify({ from, to });
  const params = { headers: { 'Content-Type': 'application/json' } };
  const res = http.post(`${BASE_URL}/api/v1/faults/drop`, payload, params);
  return check(res, {
    'inject drop is 200': (r) => r.status === 200,
  });
}

function clearFaults() {
  const res = http.del(`${BASE_URL}/api/v1/faults`);
  return check(res, {
    'clear faults is 200': (r) => r.status === 200,
  });
}

function triggerSnapshot(pid) {
  const res = http.post(`${BASE_URL}/api/v1/processes/${pid}/snapshot`);
  return JSON.parse(res.body).snapshotId;
}

export function setup() {
  healthCheck();
  const state = getState();
  if (!state || !state.processes || state.processes.length < 3) {
    runScenario('ConcurrentWrites');
    sleep(1);
    if (!state || !state.processes || state.processes.length < 3) {
      spawnProcess('P3', 'vector', 'causal');
    }
  }
}

export default function () {
  group('baseline-health', () => {
    healthCheck();
  });

  group('normal-traffic', () => {
    const state = getState();
    if (state && state.processes && state.processes.length >= 2) {
      const pids = state.processes.map((p) => p.id);
      for (let i = 0; i < pids.length; i++) {
        for (let j = 0; j < pids.length; j++) {
          if (i !== j) {
            sendMessage(pids[i], pids[j]);
          }
        }
      }
      sendMessage(pids[0], pids[1]);
    }
  });

  group('fault-injection-and-recovery', () => {
    const state = getState();
    if (!state || !state.processes || state.processes.length < 2) return;

    const pids = state.processes.map((p) => p.id);

    const targetPid = pids[Math.floor(Math.random() * pids.length)];
    const otherPids = pids.filter((p) => p !== targetPid);

    injectDelay(targetPid, otherPids[0], 200);
    injectDrop(targetPid, otherPids.length > 1 ? otherPids[1] : otherPids[0]);

    const healthy = healthCheck();
    if (healthy) faultRecoveryRate.add(1);

    const snapId = triggerSnapshot(targetPid);
    check(snapId, {
      'snapshot id generated under faults': (s) => typeof s === 'string' && s.length > 0,
    });

    clearFaults();
  });

  group('kill-and-respawn', () => {
    const state = getState();
    if (!state || !state.processes || state.processes.length < 1) return;

    const pids = state.processes.map((p) => p.id);
    const victim = pids[Math.floor(Math.random() * pids.length)];

    const killed = killProcess(victim);
    if (killed) {
      sleep(0.2);
      const respawned = spawnProcess(victim, 'vector', 'causal');
      check(respawned, {
        'process respawned after kill': (v) => v === true,
      });
    }
  });

  group('burst-resilience', () => {
    const state = getState();
    if (!state || !state.processes || state.processes.length < 2) return;

    const pids = state.processes.map((p) => p.id);
    let statuses = [];

    for (let i = 0; i < 25; i++) {
      const res = http.post(
        `${BASE_URL}/api/v1/messages`,
        JSON.stringify({ from: pids[0], to: pids[1], data: { text: `burst-${i}` } }),
        { headers: { 'Content-Type': 'application/json' } }
      );
      statuses.push(res.status);
    }

    const no5xx = statuses.every((s) => s < 500);
    check(no5xx, {
      'burst requests produce no 5xx errors': (v) => v === true,
    });

    const rateLimited = statuses.some((s) => s === 429);
    if (rateLimited) rateLimitObserved.add(1);

    healthCheck();
  });

  group('sustained-scenario-run', () => {
    runScenario('CausalDelivery');
    runScenario('BasicLamport');
  });
}
