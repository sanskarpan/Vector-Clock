import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  timeout: 30000,
  retries: 2,
  use: {
    baseURL: 'http://localhost:3001',
    headless: true,
  },
  webServer: {
    command: 'bun server/bff.ts',
    port: 3001,
    timeout: 30000,
    reuseExistingServer: true,
  },
});
