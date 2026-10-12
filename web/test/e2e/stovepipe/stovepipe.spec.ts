import { test, expect } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";
import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { Stovepipe } from "@submitqueue/api/stovepipe";
const client = createClient(
  Stovepipe,
  createGrpcTransport({
    baseUrl: process.env.STOVEPIPE_E2E_URL!,
    defaultTimeoutMs: 5_000,
  }),
);
test("real ingestion, queue selection, cursors, project results, history and refresh", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const id =
    await test.step("Ingest a real request and wait for its projection", async () => {
      const { id } = await client.ingest({ queue: "monorepo/main" });
      await expect
        .poll(
          async () =>
            (await client.list({ queue: "monorepo/main" })).requests.some(
              (request) => request.requestId === id,
            ),
          { timeout: 0 },
        )
        .toBe(true);
      return id;
    });
  const requests = page.getByRole("table", { name: "Requests", exact: true });
  const projects = page.getByRole("table", { name: "Project results" });
  const queues = page.getByRole("navigation", { name: "Queues" });
  const refresh = page.getByRole("button", { name: "Refresh", exact: true });
  const rawProjects = page.getByRole("link", { name: "Raw project JSON" });

  await test.step("Open the ingested change, read history and refresh", async () => {
    await page.goto("/monorepo%2Fmain");
    await expect(
      page.getByRole("heading", { name: "monorepo/main", exact: true }),
    ).toBeVisible();
    await page
      .getByRole("table", { name: "Requests", exact: true })
      .getByRole("row")
      .filter({ has: page.getByRole("cell", { name: id, exact: true }) })
      .getByRole("link")
      .click();
    await expect(
      page.getByRole("heading", { name: `Request ${id}`, exact: true }),
    ).toBeVisible();
    await expect(
      page.getByRole("table", { name: "Request history" }).locator("tbody tr"),
    ).not.toHaveCount(0);
    await expect(projects).toHaveCount(0);
    await expect(
      page.getByText("No project results recorded on this page."),
    ).toBeVisible();
    await refresh.click();
    await expect(refresh).toBeEnabled();
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  });
  await test.step("Select the populated queue", async () => {
    await queues
      .getByRole("link", { name: "web/pagination", exact: true })
      .click();
    await expect(requests.locator("tbody tr")).toHaveCount(50);
    await page.screenshot({
      path: `${process.env.TEST_UNDECLARED_OUTPUTS_DIR}/stovepipe-queue.png`,
      fullPage: true,
    });
  });
  await test.step("Preserve the exact URI, paginate projects and export all projects", async () => {
    await requests
      .getByRole("link", {
        name: "git://web/pagination/51?file=src%2Fmain#fragment",
        exact: true,
      })
      .click();
    await expect
      .poll(() => new URL(page.url()).pathname)
      .toBe(
        "/web%2Fpagination/change/git%3A%2F%2Fweb%2Fpagination%2F51%3Ffile%3Dsrc%252Fmain%23fragment",
      );
    await page.reload();
    await expect(
      page.getByRole("heading", { name: "Request 51", exact: true }),
    ).toBeVisible();
    await expect(projects.locator("tbody tr")).toHaveCount(1);
    await expect(page.getByText("Green (0)")).toHaveCount(2);
    await expect(page.getByText("No recorded result")).toHaveCount(0);
    await page.screenshot({
      path: `${process.env.TEST_UNDECLARED_OUTPUTS_DIR}/stovepipe-request.png`,
      fullPage: true,
    });
    const rawHref = await rawProjects.getAttribute("href");
    const response = await page.request.get(rawHref!);
    expect(response.ok()).toBe(true);
    const raw = await response.json();
    expect(raw).toEqual([{ project: "project-1", breakageDegree: 0 }]);
    for (let pageNumber = 2; pageNumber <= 6; pageNumber++) {
      await page.getByRole("link", { name: "Next 10 projects →" }).click();
      await expect(page).toHaveURL(new RegExp(`projectsPage=${pageNumber}$`));
      await expect(projects).toHaveCount(0);
      await expect(
        page.getByText("No project results recorded on this page."),
      ).toBeVisible();
    }
    await expect(
      page.getByRole("link", { name: "Next 10 projects →" }),
    ).toHaveCount(0);
    await expect(rawProjects).toHaveAttribute("href", rawHref!);
  });
  await test.step("Follow and reload the queue cursor, then select an empty queue", async () => {
    await page.getByRole("link", { name: "← Queue requests" }).click();
    await page.getByRole("link", { name: "Older requests →" }).click();
    await expect(requests.locator("tbody tr")).toHaveCount(1);
    expect(new URL(page.url()).searchParams.has("page")).toBe(true);
    await page.reload();
    await expect(requests.locator("tbody tr")).toHaveCount(1);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await queues.getByRole("link", { name: "web/empty", exact: true }).click();
    await expect(
      page.getByText("No retained requests on this queue."),
    ).toBeVisible();
  });
  expect(errors).toEqual([]);
});
