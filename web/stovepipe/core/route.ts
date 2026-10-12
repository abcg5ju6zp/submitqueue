import { decodePathSegment } from "@submitqueue/web-platform/path-segment";
import type { Queue } from "../entity/index.js";
import type { StovepipePaths } from "./paths.js";

export function parseStovepipeRoute(
  path: string,
  paths: StovepipePaths,
  queues: readonly Queue[],
) {
  if (!path.startsWith(`${paths.basePath}/`)) return null;
  let segments: string[];
  try {
    segments = path
      .slice(paths.basePath.length + 1)
      .replace(/\/$/u, "")
      .split("/")
      .map((segment) => decodePathSegment(decodeURIComponent(segment)));
  } catch {
    return null;
  }
  const queue = queues.find((queue) => queue.name === segments[0]);
  if (!queue) return null;
  if (segments.length === 1)
    return { queue, changeUri: "", projectJSON: false };
  const projectJSON = segments.length === 4 && segments[3] === "projects.json";
  if (
    !(segments.length === 3 || projectJSON) ||
    segments[1] !== "change" ||
    !segments[2]
  )
    return null;
  return { queue, changeUri: segments[2], projectJSON };
}
