import { defineConfig, devices } from '@playwright/test';

// run.sh starts the stack and exports E2E_*; the suites share one chain, so run serially.
// run.sh --swarm exports E2E_SWARM and runs only the swarm suite, which needs several nodes.
const swarm = !!process.env.E2E_SWARM;

export default defineConfig({
  testDir: './tests',
  ...(swarm ? { testMatch: /swarm\.spec\.ts$/ } : { testIgnore: /swarm\.spec\.ts$/ }),
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 90_000,
  expect: { timeout: 20_000 },
  reporter: [['list']],
  outputDir: './test-results/artifacts',
  use: {
    baseURL: process.env.E2E_EXPLORER_URL,
    ...devices['Desktop Chrome'],
    viewport: { width: 1360, height: 1000 },
    trace: 'retain-on-failure',
  },
});
