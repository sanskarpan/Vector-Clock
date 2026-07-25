import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  timeout: 30000,
  retries: 0,
  use: {
    baseURL: 'http://localhost:3001',
    headless: true,
  },
  webServer: {
    command: 'bun run server/bff.ts',
    port: 3001,
    timeout: 15000,
    reuseExistingServer: true,
    cwd: '/Users/sanskar/dev/Research/Projects/Vector-Clock/frontend',
  },
});
