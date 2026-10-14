import { test, expect } from "@playwright/test";

// Read real repository definitions through FastAPI, without creating any Airflow runs.
test("workflow map drills into real task dependencies and preserves read-only interactions", async ({
  page,
}) => {
  const errors = [];
  const mutations = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("request", (request) => {
    if (!["GET", "HEAD", "OPTIONS"].includes(request.method()))
      mutations.push(request.url());
  });
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "The shape of your workflows." }),
  ).toBeVisible();
  await expect(page.locator(".react-flow__node-workflow")).toHaveCount(7);
  await expect(page.locator(".react-flow__minimap-node")).toHaveCount(7);
  await expect(page.locator(".react-flow__edge")).toHaveCount(4);
  await page.waitForTimeout(450);
  await page.screenshot({
    path: "test-results/workflow-map.png",
    fullPage: true,
  });
  const calculation = page
    .locator(".react-flow__node-workflow")
    .filter({ hasText: "PV, exposure & CVA" });
  await calculation.click();
  await expect(page.getByLabel("Node details")).toContainText(
    "xva_calculations",
  );
  await page.getByRole("button", { name: "Explore tasks" }).click();
  await expect(page.locator(".react-flow__node-task")).toHaveCount(5);
  await expect(page.locator(".react-flow__minimap-node")).toHaveCount(5);
  await expect(page.locator(".react-flow__edge")).toHaveCount(6);
  const calculate = page
    .locator(".react-flow__node-task")
    .filter({ hasText: "calculate pv exposure cva" });
  await calculate.click();
  await expect(page.getByLabel("Node details")).toContainText(
    "INPUT CONNECTIONS",
  );
  await expect(page.getByLabel("Node details")).toContainText(
    "generate market scenarios",
  );
  await page.waitForTimeout(450);
  await page.screenshot({
    path: "test-results/calculation-tasks.png",
    fullPage: true,
  });
  const before = await calculate.getAttribute("style");
  const bounds = await calculate.boundingBox();
  await page.mouse.move(bounds.x + 100, bounds.y + 40);
  await page.mouse.down();
  await page.mouse.move(bounds.x + 190, bounds.y + 90, { steps: 8 });
  await page.mouse.up();
  await expect(calculate).toHaveAttribute("style", before);
  await page.keyboard.press("Delete");
  await expect(page.locator(".react-flow__node-task")).toHaveCount(5);
  await expect(page.locator(".react-flow__edge")).toHaveCount(6);
  await page.getByRole("button", { name: "Horizontal layout" }).click();
  await page.getByRole("button", { name: "Fit graph" }).click();
  await expect(page.locator(".react-flow__node-task")).toHaveCount(5);
  expect(mutations).toEqual([]);
  expect(errors).toEqual([]);
});

test("library filtering and trigger navigation work without edits", async ({
  page,
}) => {
  await page.goto("/");
  await page.getByLabel("Search workflows").fill("counterparty");
  const navigation = page.getByRole("navigation", { name: "Workflows" });
  await expect(navigation.getByRole("button")).toHaveCount(1);
  await navigation.getByRole("button").click();
  await expect(page.locator(".react-flow__node-task")).toHaveCount(4);
  await page
    .locator(".react-flow__node-task")
    .filter({ hasText: "trigger reports" })
    .click();
  await page
    .getByLabel("Node details")
    .getByRole("button", { name: "Reports", exact: true })
    .click();
  await expect(page.locator(".react-flow__node-task")).toHaveCount(2);
  await expect(
    page.getByRole("heading", { name: "Reports", exact: true }),
  ).toBeVisible();
});
