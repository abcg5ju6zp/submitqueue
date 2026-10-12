import { describe, expect, it, vi } from "vitest";
import { createStovepipeWeb } from "./module.js";
import { StovepipePaths } from "./core/paths.js";
import type {
  StovepipeService,
  ProjectStatus,
} from "./extension/service/index.js";

const changeUri = "git://repo/main/sha?x=1&file=src%2Fmain#fragment";
const status: ProjectStatus = {
  requestId: "7",
  queue: "repo/main",
  changeUri,
  baseUri: "",
  requestState: "future_state",
  updatedAtMs: 9007199254740993n,
  repositoryResult: { case: "repositoryBreakageDegree", value: 0 },
  projectResultsComplete: false,
  projects: [
    { project: "green", result: { case: "breakageDegree", value: 0 } },
  ],
  nextPageToken: "",
};
const allProjects: ProjectStatus["projects"] = [
  ...status.projects,
  { project: "pending", result: { case: "breakageDegree", value: 1 } },
];
function createPageTestContext(projects: string[] = ["green", "pending"]) {
  const service: StovepipeService = {
    list: vi
      .fn()
      .mockResolvedValue({ requests: [], nextPageToken: "older+/page" }),
    getProjectStatusByURI: vi.fn().mockResolvedValue(status),
    getRequestHistoryByID: vi.fn().mockResolvedValue({
      events: [
        {
          eventId: "e1",
          timestampMs: 100n,
          occurrence: { case: "event", value: "future_event" },
          outcomeReason: "future_reason",
        },
      ],
    }),
  };
  const paths = new StovepipePaths("/stovepipe");
  const app = createStovepipeWeb({
    service,
    paths,
    queues: [{ name: "repo/main", projects }],
  });
  return { service, paths, app };
}
describe("Stovepipe page loading", () => {
  it("keeps overlapping loads bound to their own host logging callbacks", async () => {
    const { app, service, paths } = createPageTestContext();
    let release!: () => void;
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });
    const error = new Error("private upstream detail");
    vi.mocked(service.list).mockImplementation(async () => {
      await ready;
      throw error;
    });
    const first = vi.fn();
    const second = vi.fn();
    const path = paths.queue("repo/main");
    const pending = [
      app.handle({
        path, search: new URLSearchParams(),
        requestId: "first", onLog: first,
      }),
      app.handle({
        path, search: new URLSearchParams(),
        requestId: "second", onLog: second,
      }),
    ];
    release();
    const results = await Promise.all(pending);

    expect(first).toHaveBeenCalledOnce();
    expect(second).toHaveBeenCalledOnce();
    expect(first).toHaveBeenCalledWith("warn", "Stovepipe page section failed", {
      requestId: "first", path, section: "queue requests", error,
    });
    expect(second).toHaveBeenCalledWith("warn", "Stovepipe page section failed", {
      requestId: "second", path, section: "queue requests", error,
    });
    expect(JSON.stringify(results)).not.toContain("private upstream detail");
  });

  it("retains the page result when log delivery throws", async () => {
    const { app, service, paths } = createPageTestContext();
    vi.mocked(service.list).mockRejectedValue(new Error("private upstream detail"));
    const result = await app.handle({
      path: paths.queue("repo/main"), search: new URLSearchParams(),
      onLog: () => {
        throw new Error("logging unavailable");
      },
    });
    expect(result).toMatchObject({ kind: "render", model: { requests: { ok: false } } });
    expect(JSON.stringify(result)).not.toContain("private upstream detail");
  });

  it("reports and rethrows unexpected page failures without replacing the cause", async () => {
    const { service, paths } = createPageTestContext();
    const error = new Error("invalid host configuration");
    const onLog = vi.fn(() => {
      throw new Error("logging unavailable");
    });
    const app = createStovepipeWeb({
      service: () => { throw error; },
      paths,
      queues: [{ name: "repo/main", projects: ["green"] }],
    });
    const path = paths.queue("repo/main");
    await expect(app.handle({
      path, search: new URLSearchParams(), requestId: "failed", onLog,
    })).rejects.toBe(error);
    expect(onLog).toHaveBeenCalledWith("error", "Web page load failed", {
      requestId: "failed", path, error,
    });
  });
  it("uses backend queue cursors and links each request to its exact change URI", async () => {
    const { service, app, paths } = createPageTestContext();
    vi.mocked(service.list).mockResolvedValue({
      requests: [
        {
          ...status,
          stateUpdatedAtMs: 1n,
          acceptedAtMs: 1n,
          outcomeReason: "",
        },
      ],
      nextPageToken: "older+/page",
    });
    const result = await app.handle({
      path: paths.queue("repo/main"),
      search: new URLSearchParams({ page: "opaque+/cursor" }),
    });
    expect(service.list).toHaveBeenCalledWith(
      { queue: "repo/main", pageSize: 50, pageToken: "opaque+/cursor" },
      {},
    );
    if (
      result.kind !== "render" ||
      result.model.kind !== "queue" ||
      !result.model.requests.ok
    )
      throw new Error("No queue page");
    expect(result.model.requests.data.nextHref).toBe(
      paths.queue("repo/main", "older+/page"),
    );
    expect(result.model.requests.data.requests[0]?.href).toBe(
      paths.change("repo/main", changeUri),
    );
  });
  it("reads the exact URI, uses the returned ID for history and includes only returned projects", async () => {
    const { app, service, paths } = createPageTestContext();
    const result = await app.handle({
      path: paths.change("repo/main", changeUri),
      search: new URLSearchParams(),
    });
    expect(service.getProjectStatusByURI).toHaveBeenCalledWith(
      {
        queue: "repo/main",
        changeUri,
        projects: ["green", "pending"],
        pageSize: 10,
        pageToken: "",
      },
      {},
    );
    expect(service.getRequestHistoryByID).toHaveBeenCalledWith(
      { queue: "repo/main", requestId: "7" },
      {},
    );
    if (result.kind !== "render" || result.model.kind !== "request")
      throw new Error("No request page");
    expect(result.model).toMatchObject({
      changeUri,
      requestId: "7",
      status: {
        ok: true,
        data: {
          repositoryDegree: 0,
          request: { updatedAtMs: "9007199254740993", state: "future_state" },
          projects: [{ name: "green", degree: 0 }],
        },
      },
      history: {
        ok: true,
        data: [{ label: "future_event", outcomeReason: "future_reason" }],
      },
    });
    expect(() => JSON.stringify(result)).not.toThrow();
  });
  it("loads status and history without project selectors", async () => {
    const { app, service, paths } = createPageTestContext([]);
    vi.mocked(service.getProjectStatusByURI).mockResolvedValue({
      ...status,
      projects: [],
    });

    const result = await app.handle({
      path: paths.change("repo/main", changeUri),
      search: new URLSearchParams(),
    });

    expect(service.getProjectStatusByURI).toHaveBeenCalledWith(
      { queue: "repo/main", changeUri, projects: [], pageSize: 10, pageToken: "" },
      {},
    );
    expect(result).toMatchObject({
      kind: "render",
      model: {
        requestId: "7",
        status: {
          ok: true,
          data: { repositoryDegree: 0, projects: [], nextHref: null },
        },
        history: { ok: true },
      },
    });
    expect(service.getRequestHistoryByID).toHaveBeenCalledWith(
      { queue: "repo/main", requestId: "7" },
      {},
    );

    expect(await app.handle({
      path: paths.projectJSON("repo/main", changeUri),
      search: new URLSearchParams(),
    })).toEqual({ kind: "project-json", data: [] });
  });
  it("requests ten configured projects per page without adding unreturned projects", async () => {
    const projects = Array.from(
      { length: 11 },
      (_, index) => `project-${index + 1}`,
    );
    const { app, service, paths } = createPageTestContext(projects);
    vi.mocked(service.getProjectStatusByURI).mockResolvedValue({
      ...status,
      projects: [],
    });
    const first = await app.handle({
      path: paths.change("repo/main", changeUri),
      search: new URLSearchParams(),
    });
    if (
      first.kind !== "render" ||
      first.model.kind !== "request" ||
      !first.model.status.ok
    )
      throw new Error("No status");
    expect(first.model.status.data.projects).toEqual([]);
    expect(first.model.status.data.nextHref).toBe(
      paths.change("repo/main", changeUri, "2"),
    );
    const last = await app.handle({
      path: paths.change("repo/main", changeUri),
      search: new URLSearchParams({ projectsPage: "2" }),
    });
    expect(service.getProjectStatusByURI).toHaveBeenLastCalledWith(
      {
        queue: "repo/main",
        changeUri,
        projects: ["project-11"],
        pageSize: 10,
        pageToken: "",
      },
      {},
    );
    if (
      last.kind !== "render" ||
      last.model.kind !== "request" ||
      !last.model.status.ok
    )
      throw new Error("No status");
    expect(last.model.status.data).toMatchObject({
      projects: [],
      nextHref: null,
      firstHref: paths.change("repo/main", changeUri),
      rawHref: paths.projectJSON("repo/main", changeUri),
    });
  });
  it.each([
    {
      name: "skips history when status fails",
      failedMethod: "getProjectStatusByURI" as const,
      statusOK: false,
      historyCalls: 0,
    },
    {
      name: "retains status when history fails",
      failedMethod: "getRequestHistoryByID" as const,
      statusOK: true,
      historyCalls: 1,
    },
  ])("$name", async ({ failedMethod, statusOK, historyCalls }) => {
    const { app, service, paths } = createPageTestContext();
    vi.mocked(service[failedMethod]).mockRejectedValue(
      new Error("service unavailable"),
    );
    const result = await app.handle({
      path: paths.change("repo/main", changeUri),
      search: new URLSearchParams(),
    });
    if (result.kind !== "render" || result.model.kind !== "request")
      throw new Error("No request page");
    expect(result.model.status.ok).toBe(statusOK);
    expect(result.model.history.ok).toBe(false);
    expect(service.getRequestHistoryByID).toHaveBeenCalledTimes(historyCalls);
  });
  it("collects only returned projects across all raw pages and skips history", async () => {
    const { app, service, paths } = createPageTestContext([
      "green",
      "pending",
      "not-returned",
    ]);
    vi.mocked(service.getProjectStatusByURI)
      .mockResolvedValueOnce({ ...status, nextPageToken: "next" })
      .mockResolvedValueOnce({
        ...status,
        projects: allProjects.slice(1),
        nextPageToken: "",
      });
    const result = await app.handle({
      path: paths.projectJSON("repo/main", changeUri),
      search: new URLSearchParams({ projectsPage: "displayed-page" }),
    });
    expect(result).toEqual({ kind: "project-json", data: allProjects });
    expect(service.getProjectStatusByURI).toHaveBeenNthCalledWith(
      1,
      {
        queue: "repo/main",
        changeUri,
        projects: ["green", "pending", "not-returned"],
        pageSize: 0,
        pageToken: "",
      },
      {},
    );
    expect(service.getProjectStatusByURI).toHaveBeenNthCalledWith(
      2,
      {
        queue: "repo/main",
        changeUri,
        projects: ["green", "pending", "not-returned"],
        pageSize: 0,
        pageToken: "next",
      },
      {},
    );
    expect(service.getRequestHistoryByID).not.toHaveBeenCalled();
  });
  it.each([
    {
      name: "later RPC fails",
      readNextPage: async (): Promise<ProjectStatus> => {
        throw new Error("service unavailable");
      },
    },
    {
      name: "cursor repeats",
      readNextPage: async () => ({ ...status, nextPageToken: "next" }),
    },
    {
      name: "request identity changes",
      readNextPage: async () => ({ ...status, requestId: "8" }),
    },
    {
      name: "URI changes",
      readNextPage: async () => ({ ...status, changeUri: "other" }),
    },
  ])("rejects an incomplete export when $name", async ({ readNextPage }) => {
    const { app, service, paths } = createPageTestContext();
    vi.mocked(service.getProjectStatusByURI)
      .mockResolvedValueOnce({ ...status, nextPageToken: "next" })
      .mockImplementationOnce(readNextPage);
    const result = await app.handle({
      path: paths.projectJSON("repo/main", changeUri),
      search: new URLSearchParams(),
    });
    expect(result.kind).toBe("unavailable");
    expect(service.getProjectStatusByURI).toHaveBeenCalledTimes(2);
  });
  it.each(["0", "-1", "2", "1.5", "Infinity", "01"])(
    "rejects project page %s before reading the service",
    async (projectsPage) => {
      const { app, service, paths } = createPageTestContext();
      const result = await app.handle({
        path: paths.change("repo/main", changeUri),
        search: new URLSearchParams({ projectsPage }),
      });
      if (result.kind !== "render" || result.model.kind !== "request")
        throw new Error("No request page");
      expect(result.model.status.ok).toBe(false);
      expect(service.getProjectStatusByURI).not.toHaveBeenCalled();
      expect(service.getRequestHistoryByID).not.toHaveBeenCalled();
    },
  );
  it("blocks unauthorized loads before calling the service", async () => {
    const { service, paths } = createPageTestContext();
    const app = createStovepipeWeb({
      service,
      paths,
      queues: [{ name: "repo/main", projects: ["green"] }],
      authorize: () => false,
    });
    expect(
      await app.handle({
        path: paths.change("repo/main", changeUri),
        search: new URLSearchParams(),
      }),
    ).toEqual({ kind: "forbidden" });
    expect(service.getProjectStatusByURI).not.toHaveBeenCalled();
  });
  it.each([
    "/other",
    "/stovepipe/%",
    "/stovepipe/unknown",
    "/stovepipe/repo%2Fmain/request/7",
    "/stovepipe/repo%2Fmain/change/uri/extra",
  ])("rejects invalid or reserved route %s", async (path) => {
    const { app, service } = createPageTestContext();
    expect(await app.handle({ path, search: new URLSearchParams() })).toEqual({
      kind: "not-found",
    });
    expect(service.list).not.toHaveBeenCalled();
    expect(service.getProjectStatusByURI).not.toHaveBeenCalled();
  });
});
