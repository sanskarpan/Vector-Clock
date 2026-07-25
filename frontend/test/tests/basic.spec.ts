import { test, expect } from '@playwright/test';
import { setupTestPage } from '../helpers';

test.describe('Basic UI', () => {
  test.beforeEach(async ({ page }) => {
    await setupTestPage(page);
  });

  test('page title contains Vector Clock', async ({ page }) => {
    await expect(page).toHaveTitle(/Vector Clock/);
  });

  test('heading displays Vector Clock Lab', async ({ page }) => {
    await expect(page.locator('h1')).toHaveText('Vector Clock Lab');
  });

  test('Space-Time Diagram container is in the DOM', async ({ page }) => {
    await expect(page.locator('#space-time-container')).toBeAttached();
    await expect(page.locator('#space-time-diagram')).toBeAttached();
  });

  test('process lanes show P1, P2, P3', async ({ page }) => {
    const processList = page.locator('#process-list');
    await expect(processList).toBeAttached();
    const items = processList.locator('.font-mono');
    await expect(items).toHaveText(['P1', 'P2', 'P3']);
  });

  test('toolbar has clock type and delivery mode selectors', async ({ page }) => {
    await expect(page.locator('#clock-type-select')).toBeVisible();
    await expect(page.locator('#delivery-mode-select')).toBeVisible();
    const clockOptions = page.locator('#clock-type-select option');
    await expect(clockOptions).toHaveText(['Vector', 'Lamport', 'Matrix']);
    const deliveryOptions = page.locator('#delivery-mode-select option');
    await expect(deliveryOptions).toHaveText(['Causal', 'Immediate']);
  });

  test('toolbar buttons are present', async ({ page }) => {
    await expect(page.locator('#btn-internal-event')).toBeVisible();
    await expect(page.locator('#btn-send-message')).toBeVisible();
    await expect(page.locator('#btn-snapshot')).toBeVisible();
    await expect(page.locator('#btn-reset')).toBeVisible();
    await expect(page.locator('#btn-add-process')).toBeVisible();
  });

  test('WebSocket status indicator shows connected', async ({ page }) => {
    const wsStatus = page.locator('#ws-status');
    await expect(wsStatus).toBeVisible();
    await expect(wsStatus).toHaveText('Connected', { timeout: 5000 });
    await expect(wsStatus).toHaveClass(/bg-green-900/);
  });

  test('Clock Inspector section is present', async ({ page }) => {
    await expect(page.locator('#clock-inspector')).toBeVisible();
    await expect(page.locator('#clock-inspector-content')).toBeVisible();
  });

  test('Delivery Monitor section is present', async ({ page }) => {
    await expect(page.locator('#delivery-monitor')).toBeVisible();
    await expect(page.locator('#hold-back-queue')).toBeVisible();
  });

  test('Scenario Panel section is present', async ({ page }) => {
    await expect(page.locator('#scenario-panel')).toBeVisible();
    await expect(page.locator('#scenario-panel').locator('.scenario-run-btn').first()).toBeVisible();
  });
});
