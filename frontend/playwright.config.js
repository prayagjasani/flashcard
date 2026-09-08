import { defineConfig } from '@playwright/test';

const port = Number(process.env.TEST_PORT || 8765);

export default defineConfig({
  testDir: './tests',
  fullyParallel: true,
  use: {
    baseURL: `http://127.0.0.1:${port}`,
    headless: true,
    serviceWorkers: 'block',
    launchOptions: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE
      ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE } : {},
  },
  webServer: {
    command: `python -m http.server ${port} --bind 127.0.0.1`,
    cwd: '..',
    url: `http://127.0.0.1:${port}/static/react/index.html`,
    reuseExistingServer: false,
  },
});
