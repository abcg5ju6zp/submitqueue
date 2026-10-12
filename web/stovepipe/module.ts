import { metrics, trace, SpanStatusCode, type Meter, type Tracer } from "@opentelemetry/api";
import { createLogCallback, type LogCallback } from "@submitqueue/web-platform/logging";
import { parseStovepipeRoute } from "./core/route.js";
import type {
  StovepipeService,
  ProjectStatus,
} from "./extension/service/index.js";
import type { Queue, StovepipePageModel } from "./entity/index.js";
import { StovepipePaths } from "./core/paths.js";
import { readSection } from "./controller/page.js";
import { loadQueueRequests } from "./controller/queue.js";
import {
  loadProjectStatus,
  loadRequestHistory,
  readAllProjectResults,
} from "./controller/request.js";

export interface StovepipeWebRequest {
  /** Percent-encoded URL pathname, including the mount path. */
  path: string;
  search: URLSearchParams;
  principal?: unknown;
  signal?: AbortSignal;
  /** Correlation ID for logs; generated when absent. */
  requestId?: string;
  /** Host callback bound to this request's logging context; absent means silent. */
  onLog?: LogCallback;
}
export type StovepipeWebResult =
  | { kind: "render"; model: StovepipePageModel }
  | { kind: "not-found" }
  | { kind: "forbidden" }
  | { kind: "redirect"; href: string }
  | { kind: "project-json"; data: ProjectStatus["projects"] }
  | { kind: "unavailable"; error: string };
export interface StovepipeWebOptions {
  service:
    | StovepipeService
    | ((request: StovepipeWebRequest) => StovepipeService);
  queues: readonly Queue[];
  paths?: StovepipePaths;
  authorize?(principal: unknown, queue: string): boolean | Promise<boolean>;
  meter?: Meter;
  tracer?: Tracer;
}
export function createStovepipeWeb(options: StovepipeWebOptions) {
  const paths = options.paths ?? new StovepipePaths();
  const meter = options.meter ?? metrics.getMeter("stovepipe-web");
  const tracer = options.tracer ?? trace.getTracer("stovepipe-web");
  const loads = meter.createCounter("stovepipe.web.page_loads");
  const latency = meter.createHistogram("stovepipe.web.page_load_duration", {
    unit: "ms",
  });
  return {
    async handle(request: StovepipeWebRequest): Promise<StovepipeWebResult> {
      const started = Date.now();
      const onLog = createLogCallback(request.onLog, {
        requestId: request.requestId ?? globalThis.crypto.randomUUID(),
        path: request.path,
      });
      return tracer.startActiveSpan(
        "stovepipe.web.page_load",
        async (span): Promise<StovepipeWebResult> => {
          try {
            if (request.path === (paths.basePath || "/")) {
              const first = options.queues[0];
              return first
                ? { kind: "redirect", href: paths.queue(first.name) }
                : { kind: "not-found" };
            }
            const route = parseStovepipeRoute(
              request.path,
              paths,
              options.queues,
            );
            if (!route) return { kind: "not-found" };
            const { queue, changeUri, projectJSON } = route;
            if (
              options.authorize &&
              !(await options.authorize(request.principal, queue.name))
            )
              return { kind: "forbidden" };
            const service =
              typeof options.service === "function"
                ? options.service(request)
                : options.service;
            if (projectJSON) {
              const result = await readSection(
                () =>
                  readAllProjectResults(
                    service,
                    queue.name,
                    changeUri,
                    queue.projects,
                    request.signal,
                  ),
                onLog,
                "project results",
              );
              return result.ok
                ? { kind: "project-json", data: result.data }
                : { kind: "unavailable", error: result.error };
            }
            const latestHref = changeUri
              ? paths.change(queue.name, changeUri)
              : paths.queue(queue.name);
            const common = {
              key: `${request.path}?${request.search}`,
              queue: queue.name,
              queues: options.queues.map((queue) => ({
                name: queue.name,
                href: paths.queue(queue.name),
              })),
              queueHref: paths.queue(queue.name),
              latestHref,
              showLatestLink: Boolean(
                request.search.get(changeUri ? "projectsPage" : "page"),
              ),
              loadedAtMs: Date.now(),
            };
            if (!changeUri) {
              const requests = await readSection(
                () =>
                  loadQueueRequests(
                    service,
                    paths,
                    queue.name,
                    request.search.get("page") ?? "",
                    request.signal,
                  ),
                onLog,
                "queue requests",
              );
              return {
                kind: "render",
                model: { ...common, kind: "queue", requests },
              };
            }
            const status = await readSection(
              () =>
                loadProjectStatus(
                  service,
                  paths,
                  queue.name,
                  changeUri,
                  queue.projects,
                  request.search.get("projectsPage") ?? "",
                  request.signal,
                ),
              onLog,
              "project status",
            );
            const requestId = status.ok ? status.data.request.id : null;
            const history = requestId
              ? await readSection(
                  () =>
                    loadRequestHistory(
                      service,
                      queue.name,
                      requestId,
                      request.signal,
                    ),
                  onLog,
                  "request history",
                )
              : {
                  ok: false as const,
                  error:
                    "History is unavailable until project status identifies the request. Try refreshing.",
                };
            return {
              kind: "render",
              model: {
                ...common,
                kind: "request",
                changeUri,
                requestId,
                status,
                history,
              },
            };
          } catch (error) {
            onLog("error", "Web page load failed", { error });
            span.recordException(error instanceof Error ? error : String(error));
            span.setStatus({ code: SpanStatusCode.ERROR });
            throw error;
          } finally {
            loads.add(1);
            latency.record(Date.now() - started);
            span.end();
          }
        },
      );
    },
  };
}
