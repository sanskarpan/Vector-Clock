import { test, expect } from '@playwright/test';
import { setupTestPage } from '../helpers';

test.describe('Simulation controls', () => {
  test.beforeEach(async ({ page }) => {
    await setupTestPage(page);
  });

  test('clicking Internal Event button triggers POST request', async ({ page }) => {
    const requestPromise = page.waitForRequest(
      (req) => req.url().includes('/api/processes') && req.url().includes('/event') && req.method() === 'POST'
    );

    page.on('dialog', (dialog) => {
      expect(dialog.message()).toContain('Process ID');
      dialog.accept('P1');
    });

    await page.locator('#btn-internal-event').click();
    const request = await requestPromise;
    expect(request.url()).toContain('/api/processes/P1/event');
  });

  test('sending a message triggers POST request with from/to', async ({ page }) => {
    const requestPromise = page.waitForRequest(
      (req) => req.url().includes('/api/messages') && req.method() === 'POST'
    );

    const dialogMessages: string[] = [];
    page.on('dialog', (dialog) => {
      dialogMessages.push(dialog.message());
      if (dialogMessages.length === 1) {
        dialog.accept('P1');
      } else {
        dialog.accept('P2');
      }
    });

    await page.locator('#btn-send-message').click();
    const request = await requestPromise;
    const body = JSON.parse(request.postData()!);
    expect(body.from).toBe('P1');
    expect(body.to).toBe('P2');
  });

  test('changing clock type dropdown updates selection', async ({ page }) => {
    const select = page.locator('#clock-type-select');
    await expect(select).toHaveValue('vector');
    await select.selectOption('lamport');
    await expect(select).toHaveValue('lamport');
    await select.selectOption('matrix');
    await expect(select).toHaveValue('matrix');
    await select.selectOption('vector');
    await expect(select).toHaveValue('vector');
  });

  test('changing delivery mode dropdown updates selection', async ({ page }) => {
    const select = page.locator('#delivery-mode-select');
    await expect(select).toHaveValue('causal');
    await select.selectOption('immediate');
    await expect(select).toHaveValue('immediate');
    await select.selectOption('causal');
    await expect(select).toHaveValue('causal');
  });

  test('clicking Reset triggers POST to simulation/reset', async ({ page }) => {
    const requestPromise = page.waitForRequest(
      (req) => req.url().includes('/api/simulation/reset') && req.method() === 'POST'
    );

    await page.locator('#btn-reset').click();
    const request = await requestPromise;
    expect(request).toBeTruthy();
  });

  test('clicking Add Process triggers POST to spawn process', async ({ page }) => {
    const requestPromise = page.waitForRequest(
      (req) => req.url().includes('/api/processes') && req.method() === 'POST' && !req.url().includes('/event')
    );

    page.on('dialog', (dialog) => {
      dialog.accept('P4');
    });

    await page.locator('#btn-add-process').click();
    const request = await requestPromise;
    const body = JSON.parse(request.postData()!);
    expect(body.id).toBe('P4');
  });

  test('running a scenario triggers POST to scenarios endpoint', async ({ page }) => {
    const runBtn = page.locator('.scenario-run-btn').first();
    await expect(runBtn).toBeVisible();

    await page.route('**/api/scenarios/*/run', async (route) => {
      await route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ running: 'BasicLamport' }),
      });
    });

    const requestPromise = page.waitForRequest(
      (req) => req.url().includes('/api/scenarios') && req.url().includes('/run')
    );

    await runBtn.click();
    const request = await requestPromise;
    expect(request.url()).toContain('BasicLamport');
  });
});
