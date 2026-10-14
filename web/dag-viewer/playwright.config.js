import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  timeout: 45000,
  use: {
    baseURL: "http://127.0.0.1:8001",
    viewport: { width: 1600, height: 1000 },
    headless: true,
    launchOptions: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH
      ? {
          executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
          args: ["--no-sandbox"],
        }
      : {},
  },
  webServer: {
    command: `${process.env.XVA_DEMO_PYTHON || "../../.venv/bin/python"} -m uvicorn risk_pipeline.graph_demo.api:app --host 127.0.0.1 --port 8001 --app-dir ../../src`,
    url: "http://127.0.0.1:8001/api/health",
    reuseExistingServer: false,
    env: {
      AIRFLOW_HOME: process.env.AIRFLOW_HOME || "/tmp/xva-graph-test-airflow",
      AIRFLOW__CORE__LOAD_EXAMPLES: "False",
    },
    timeout: 45000,
  },
});
