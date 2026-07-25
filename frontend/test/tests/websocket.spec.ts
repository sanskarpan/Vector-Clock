import { test, expect } from '@playwright/test';
import { setupTestPage } from '../helpers';

test.describe('WebSocket events', () => {
  let sendWS: (data: string) => Promise<void>;

  test.beforeEach(async ({ page }) => {
    const result = await setupTestPage(page, { scenarios: [] });
    sendWS = result.sendWSMessage;
  });

  test('WebSocket connection status shows Connected after connect', async ({ page }) => {
    const wsStatus = page.locator('#ws-status');
    await expect(wsStatus).toHaveText('Connected', { timeout: 5000 });
    await expect(wsStatus).toHaveClass(/bg-green-900/);
  });

  test('WebSocket connection status shows Disconnected on close', async ({ page }) => {
    await expect(page.locator('#ws-status')).toHaveText('Connected', { timeout: 5000 });

    // Simulate WebSocket close
    await page.evaluate(() => (window as any).__closeAllWS());

    const wsStatus = page.locator('#ws-status');
    await expect(wsStatus).toHaveText('Disconnected', { timeout: 5000 });
    await expect(wsStatus).toHaveClass(/bg-red-900/);
  });

  test('internal event via WebSocket appears in frontend diagram', async ({ page }) => {
    await expect(page.locator('#ws-status')).toHaveText('Connected', { timeout: 5000 });

    // Send an internal_event via WebSocket as the server would
    await sendWS(JSON.stringify({
      id: 'evt-internal-1',
      type: 'internal_event',
      processId: 'P1',
      timestamp: new Date().toISOString(),
      localSeq: 1,
      globalSeq: 1,
      lamportClock: 1,
      vectorClock: { P1: 1, P2: 0, P3: 0 },
      narration: 'P1 internal event',
    }));

    // Wait for the event node to appear in the SVG diagram
    const eventCircle = page.locator('#space-time-diagram circle.event-node').first();
    await expect(eventCircle).toBeVisible({ timeout: 5000 });
  });

  test('process_killed event updates process status to dead', async ({ page }) => {
    await expect(page.locator('#ws-status')).toHaveText('Connected', { timeout: 5000 });

    // Verify P2 is initially shown as running
    const processList = page.locator('#process-list');
    const p2Entry = processList.locator('.font-mono', { hasText: 'P2' }).locator('..');
    await expect(p2Entry.locator('span').last()).toHaveText('running');

    // Send process_killed event via WebSocket
    await sendWS(JSON.stringify({
      id: 'evt-kill-1',
      type: 'process_killed',
      processId: 'P2',
      timestamp: new Date().toISOString(),
      localSeq: 1,
      globalSeq: 2,
    }));

    // Wait for the status to update
    await expect(p2Entry.locator('span').last()).toHaveText('dead', { timeout: 5000 });
    await expect(p2Entry.locator('span').last()).toHaveClass(/bg-red-900/);
  });

  test('snapshot events appear in the diagram', async ({ page }) => {
    await expect(page.locator('#ws-status')).toHaveText('Connected', { timeout: 5000 });

    // Send snapshot_start event
    await sendWS(JSON.stringify({
      id: 'evt-snap-1',
      type: 'snapshot_start',
      processId: 'P1',
      timestamp: new Date().toISOString(),
      localSeq: 1,
      globalSeq: 3,
      snapshot: { snapshotId: 'snap-1' },
    }));

    // Send snapshot_complete event
    await sendWS(JSON.stringify({
      id: 'evt-snap-2',
      type: 'snapshot_complete',
      processId: 'P1',
      timestamp: new Date().toISOString(),
      localSeq: 2,
      globalSeq: 4,
      snapshot: { snapshotId: 'snap-1', complete: true },
    }));

    // Verify events appeared in diagram
    const snapCircles = page.locator('#space-time-diagram circle.event-node');
    await expect(snapCircles).toHaveCount(2, { timeout: 5000 });
  });
});
