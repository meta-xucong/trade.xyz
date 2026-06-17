const { defineConfig, devices } = require("@playwright/test");

const PORT = process.env.TRADE_XYZ_E2E_PORT || "18792";
const BASE_URL = `http://127.0.0.1:${PORT}`;
const noProxyHosts = "127.0.0.1,localhost,::1";
process.env.NO_PROXY = process.env.NO_PROXY
  ? `${process.env.NO_PROXY},${noProxyHosts}`
  : noProxyHosts;
process.env.no_proxy = process.env.NO_PROXY;

module.exports = defineConfig({
  testDir: "./tests/e2e",
  timeout: 60_000,
  expect: {
    timeout: 15_000,
  },
  use: {
    baseURL: BASE_URL,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  webServer: {
    command: `cargo run -- console --config config/dry-run.example.toml --bind 127.0.0.1:${PORT} --dry-run true`,
    url: `${BASE_URL}/`,
    env: {
      NO_PROXY: process.env.NO_PROXY,
      no_proxy: process.env.NO_PROXY,
    },
    reuseExistingServer: false,
    timeout: 120_000,
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
