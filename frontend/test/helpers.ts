import type { Page } from '@playwright/test';
import { readFileSync, existsSync } from 'fs';
import { execSync } from 'child_process';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let bundledJs: string | null = null;

function getBundledJs(): string {
  if (!bundledJs) {
    const distPath = resolve(__dirname, '..', 'dist', 'main.js');
    if (!existsSync(distPath)) {
      const frontendDir = resolve(__dirname, '..');
      execSync('bun build src/main.ts --outdir=dist --target=browser --format=esm', {
        cwd: frontendDir,
        stdio: 'pipe',
      });
    }
    bundledJs = readFileSync(distPath, 'utf-8');
  }
  return bundledJs;
}

export interface MockProcess {
  id: string;
  clockType?: string;
  deliveryMode?: string;
  lamportClock?: number;
  vectorClock?: Record<string, number>;
  peers?: string[];
  eventCount?: number;
  heldMessages?: number;
  status?: string;
}

export interface MockScenario {
  name: string;
  description: string;
  stepCount: number;
}

export type SendWSMessage = (data: string) => void;
export type CloseAllWS = () => void;

/**
 * Sets up page routes and WebSocket mock for frontend testing.
 * Serves the built frontend bundle, mocks API endpoints, and simulates WebSocket.
 */
export async function setupTestPage(
  page: Page,
  options: {
    processes?: MockProcess[];
    scenarios?: MockScenario[];
  } = {}
): Promise<{ sendWSMessage: SendWSMessage; closeAllWS: CloseAllWS }> {
  const processes = options.processes ?? [
    { id: 'P1', clockType: 'vector', deliveryMode: 'causal', lamportClock: 0, vectorClock: { P1: 0, P2: 0, P3: 0 }, peers: ['P2', 'P3'], eventCount: 0, heldMessages: 0, status: 'running' },
    { id: 'P2', clockType: 'vector', deliveryMode: 'causal', lamportClock: 0, vectorClock: { P1: 0, P2: 0, P3: 0 }, peers: ['P1', 'P3'], eventCount: 0, heldMessages: 0, status: 'running' },
    { id: 'P3', clockType: 'vector', deliveryMode: 'causal', lamportClock: 0, vectorClock: { P1: 0, P2: 0, P3: 0 }, peers: ['P1', 'P2'], eventCount: 0, heldMessages: 0, status: 'running' },
  ];
  const scenarios = options.scenarios ?? [
    { name: 'BasicLamport', description: 'Lamport clock demo', stepCount: 5 },
    { name: 'ConcurrentWrites', description: 'Concurrent writes demo', stepCount: 2 },
  ];

  const bundledJs = getBundledJs();

  // Intercept root HTML — replace script src to point to our bundled file
  await page.route('**/', async (route) => {
    const url = route.request().url();
    if (url === 'http://localhost:3001/' || url === 'http://localhost:3001') {
      const response = await route.fetch();
      let body = await response.text();
      body = body.replace(
        '<script type="module" src="./src/main.ts"></script>',
        '<script type="module" src="/test-bundle.js"></script>'
      );
      await route.fulfill({
        status: 200,
        contentType: 'text/html',
        body,
      });
    } else {
      await route.continue();
    }
  });

  // Serve the bundled frontend at /test-bundle.js
  await page.route('**/test-bundle.js', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/javascript',
      body: bundledJs,
    });
  });

  // Mock simulation state
  await page.route('**/api/simulation/state', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        processes,
        config: { clockType: 'vector', deliveryMode: 'causal', channels: 'full_mesh' },
      }),
    });
  });

  // Mock tailwindcss @import in CSS to avoid 404
  await page.route('**/src/styles/tailwindcss', async (route) => {
    await route.fulfill({ status: 200, contentType: 'text/css', body: '' });
  });

  // Mock scenarios list
  await page.route('**/api/scenarios', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(scenarios),
    });
  });

  // Mock WebSocket globally
  await page.addInitScript(() => {
    const instances: {
      readyState: number;
      onopen: ((e: Event) => void) | null;
      onmessage: ((e: MessageEvent) => void) | null;
      onclose: ((e: CloseEvent) => void) | null;
      onerror: ((e: Event) => void) | null;
      send: (data: string) => void;
      close: () => void;
    }[] = [];

    (window as any).__mockWSInstances = instances;
    (window as any).__sendWSMessage = function (data: string) {
      for (const ws of instances) {
        if (ws.readyState === 1) {
          try { ws.onmessage?.({ data } as MessageEvent); } catch {}
        }
      }
    };
    (window as any).__closeAllWS = function () {
      for (const ws of instances) {
        ws.readyState = 3;
        try { ws.onclose?.(new CloseEvent('close')); } catch {}
      }
    };

    (window as any).MockWebSocket = class MockWebSocket {
      readyState = 0;
      onopen: ((e: Event) => void) | null = null;
      onmessage: ((e: MessageEvent) => void) | null = null;
      onclose: ((e: CloseEvent) => void) | null = null;
      onerror: ((e: Event) => void) | null = null;
      static CONNECTING = 0;
      static OPEN = 1;
      static CLOSING = 2;
      static CLOSED = 3;
      url: string;

      constructor(url: string) {
        this.url = url;
        instances.push(this);
        setTimeout(() => {
          this.readyState = 1;
          this.onopen?.(new Event('open'));
        }, 50);
      }

      send(_data: string): void {}
      close(): void {
        this.readyState = 3;
        this.onclose?.(new CloseEvent('close'));
      }
    };

    window.WebSocket = (window as any).MockWebSocket as unknown as typeof WebSocket;
  });

  await page.goto('/');

  return {
    sendWSMessage: (data: string) => page.evaluate((d) => (window as any).__sendWSMessage(d), data),
    closeAllWS: () => page.evaluate(() => (window as any).__closeAllWS()),
  };
}
