import { test, expect } from "@playwright/test";

const runId = "manual__2026-10-15T10:00:00+00:00";
const run = { dag_id: "xva_calculations", dag_run_id: runId, state: "failed" };

test("linked states, mapped logs and confirmed retry preserve the exact run", async ({
  page,
}) => {
  let retried = false;
  const mutations = [];
  await page.route("**/api/runs?*", (route) =>
    route.fulfill({ json: { runs: [run] } }),
  );
  await page.route("**/api/execution?*", (route) =>
    route.fulfill({
      json: {
        stages: [
          {
            ...run,
            state: retried ? "running" : "failed",
            tasks: [
              {
                task_id: "validate_batch",
                state: "success",
                try_number: 1,
                map_index: -1,
              },
              {
                task_id: "generate_market_scenarios",
                state: "success",
                try_number: 1,
                map_index: -1,
              },
              {
                task_id: "calculate_pv_exposure_cva",
                state: "success",
                try_number: 1,
                map_index: 0,
              },
              {
                task_id: "calculate_pv_exposure_cva",
                state: retried ? "running" : "failed",
                try_number: 2,
                map_index: 1,
                duration: 3.2,
              },
              {
                task_id: "reconcile_and_publish",
                state: "upstream_failed",
                try_number: 0,
                map_index: -1,
              },
              {
                task_id: "trigger_reports",
                state: "upstream_failed",
                try_number: 0,
                map_index: -1,
              },
            ],
          },
          {
            dag_id: "xva_reports",
            dag_run_id: null,
            state: "waiting",
            tasks: [],
          },
        ],
      },
    }),
  );
  await page.route("**/api/logs?*", (route) => {
    const url = new URL(route.request().url());
    expect(url.searchParams.get("run_id")).toBe(runId);
    expect(url.searchParams.get("map_index")).toBe("1");
    expect(url.searchParams.get("attempt")).toBe("2");
    return route.fulfill({
      json: { content: "ValueError: exposure needs investigation" },
    });
  });
  await page.route("**/api/retry", (route) => {
    mutations.push(route.request().postDataJSON());
    retried = true;
    return route.fulfill({ json: { status: "accepted" } });
  });
  await page.goto("/");
  await page.getByLabel("Execution workflow").selectOption("xva_calculations");
  await page.getByLabel("Execution run").selectOption(runId);
  await expect(page.locator(".workflow-card.runtime-failed")).toHaveCount(1);
  await expect(
    page.locator(".workflow-card.runtime-outside_execution"),
  ).toHaveCount(5);
  await page
    .getByRole("navigation", { name: "Workflows" })
    .getByRole("button", { name: /PV, exposure/ })
    .click();
  const calculate = page
    .locator(".react-flow__node-task")
    .filter({ hasText: "calculate pv exposure cva" });
  await calculate.click();
  const inspector = page.getByLabel("Node details");
  await inspector.getByLabel("Mapped task instance").selectOption("1");
  await inspector.getByRole("button", { name: "Load logs" }).click();
  await expect(inspector.getByLabel("Task logs")).toContainText(
    "exposure needs investigation",
  );
  await page.getByRole("button", { name: "Fit graph" }).click();
  await page.waitForTimeout(450);
  await page.screenshot({
    path: "test-results/execution-tasks.png",
    fullPage: true,
  });
  await inspector.getByRole("button", { name: "Retry failed task" }).click();
  expect(mutations).toEqual([]);
  await expect(inspector).toContainText("Linked DAG runs are not cleared");
  await inspector.getByRole("button", { name: "Confirm retry" }).click();
  await expect(calculate.locator(".run-status")).toHaveText("running");
  expect(mutations).toEqual([
    {
      dag_id: "xva_calculations",
      run_id: runId,
      task_id: "calculate_pv_exposure_cva",
      map_index: 1,
    },
  ]);
});

test("launch recovers an uncertain response with the same request ID and configuration", async ({
  page,
}) => {
  const submissions = [];
  await page.route("**/api/runs?*", (route) =>
    route.fulfill({
      json: {
        runs:
          submissions.length > 1
            ? [
                {
                  dag_id: "xva_generate_inputs",
                  dag_run_id: submissions[0].run_id,
                  state: "queued",
                },
              ]
            : [],
      },
    }),
  );
  await page.route("**/api/execution?*", (route) =>
    route.fulfill({
      json: {
        stages: [
          {
            dag_id: "xva_generate_inputs",
            dag_run_id: submissions[0].run_id,
            state: "queued",
            tasks: [],
          },
        ],
      },
    }),
  );
  await page.route("**/api/runs", (route) => {
    submissions.push(route.request().postDataJSON());
    return submissions.length === 1
      ? route.fulfill({
          status: 503,
          json: {
            detail: "Airflow could not be reached. Retry the same request.",
          },
        })
      : route.fulfill({
          json: {
            dag_id: "xva_generate_inputs",
            dag_run_id: submissions[0].run_id,
            state: "queued",
          },
        });
  });
  await page.goto("/");
  await page.getByRole("button", { name: "Run workflow", exact: true }).click();
  const dialog = page.getByRole("form", { name: "Launch workflow" });
  await expect(dialog.getByLabel("Run configuration")).toHaveValue(
    /"counterparty_count": 2000/,
  );
  await dialog.getByRole("button", { name: "Launch run" }).click();
  await expect(dialog.getByRole("alert")).toContainText(
    "Retry the same request",
  );
  await expect(dialog.getByLabel("Run configuration")).toBeDisabled();
  await dialog.getByRole("button", { name: "Retry same request" }).click();
  await expect(dialog).toHaveCount(0);
  expect(submissions).toHaveLength(2);
  expect(submissions[1]).toEqual(submissions[0]);
  await expect(page.getByLabel("Execution run")).toHaveValue(
    submissions[0].run_id,
  );
});

test("refresh failure removes stale success colours and definitions clear monitoring", async ({
  page,
}) => {
  let fail = false;
  await page.route("**/api/runs?*", (route) =>
    route.fulfill({ json: { runs: [{ ...run, state: "success" }] } }),
  );
  await page.route("**/api/execution?*", (route) =>
    fail
      ? route.fulfill({
          status: 503,
          json: { detail: "Airflow could not be reached." },
        })
      : route.fulfill({
          json: { stages: [{ ...run, state: "success", tasks: [] }] },
        }),
  );
  await page.goto("/");
  await page.getByLabel("Execution workflow").selectOption("xva_calculations");
  await page.getByLabel("Execution run").selectOption(runId);
  await expect(page.locator(".workflow-card.runtime-success")).toHaveCount(1);
  fail = true;
  await page.getByRole("button", { name: "Refresh execution" }).click();
  await expect(page.getByRole("alert")).toContainText(
    "Airflow could not be reached",
  );
  await expect(page.locator(".workflow-card.runtime-success")).toHaveCount(0);
  await expect(page.locator(".workflow-card.runtime-unavailable")).toHaveCount(
    7,
  );
  await page.getByLabel("Execution workflow").selectOption("");
  await expect(page.getByRole("alert")).toHaveCount(0);
  await expect(page.locator(".workflow-card.runtime-unavailable")).toHaveCount(
    0,
  );
});
