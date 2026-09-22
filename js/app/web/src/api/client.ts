import type { Event, Instance, NodeSample, Page } from "@/domain/api";

/**
 * Fetchers for the API the server mounts at `/api`. Same origin in
 * production (the SPA is embedded in the server binary); the Vite dev server
 * proxies `/api` to the running server.
 */

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly path: string,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function getJson<T>(
  path: string,
  params: Record<string, string | number | undefined>,
  signal?: AbortSignal,
): Promise<T> {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params))
    if (value !== undefined) search.set(key, String(value));
  const query = search.toString();
  const url = `/api${path}${query ? `?${query}` : ""}`;
  const response = await fetch(url, {
    signal,
    headers: { accept: "application/json" },
  });
  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new ApiError(
      response.status,
      url,
      `${response.status} ${response.statusText}${text ? `: ${text.slice(0, 200)}` : ""}`,
    );
  }
  return (await response.json()) as T;
}

export interface TimeRange {
  from: Date;
  to: Date;
}

const INSTANCE_PAGE_LIMIT = 100;
const EVENT_PAGE_LIMIT = 1000;

/** One page of instances active in the window, newest last activity first. */
export function listInstances(
  range: TimeRange,
  after?: string,
  signal?: AbortSignal,
): Promise<Page<Instance>> {
  return getJson<Page<Instance>>(
    "/observability-state/instances",
    {
      from: range.from.toISOString(),
      to: range.to.toISOString(),
      limit: INSTANCE_PAGE_LIMIT,
      after,
    },
    signal,
  );
}

export function getInstance(
  vmId: string,
  signal?: AbortSignal,
): Promise<Instance> {
  return getJson<Instance>(
    `/observability-state/instances/${encodeURIComponent(vmId)}`,
    {},
    signal,
  );
}

/** Every event of one VM, oldest first, following the cursor to the end. */
export async function vmTimeline(
  vmId: string,
  signal?: AbortSignal,
  maxPages = 5,
): Promise<{ events: Event[]; complete: boolean }> {
  const events: Event[] = [];
  let after: string | undefined;
  for (let page = 0; page < maxPages; page += 1) {
    const result = await getJson<Page<Event>>(
      `/observability-events/vms/${encodeURIComponent(vmId)}/timeline`,
      { limit: EVENT_PAGE_LIMIT, after },
      signal,
    );
    events.push(...result.items);
    if (!result.next) return { events, complete: true };
    after = result.next;
  }
  return { events, complete: false };
}

/** Events across all VMs in the window, newest first, up to `maxPages` pages. */
export async function listEvents(
  range: TimeRange,
  signal?: AbortSignal,
  maxPages = 5,
): Promise<{ events: Event[]; complete: boolean }> {
  const events: Event[] = [];
  let after: string | undefined;
  for (let page = 0; page < maxPages; page += 1) {
    const result = await getJson<Page<Event>>(
      "/observability-events",
      {
        from: range.from.toISOString(),
        to: range.to.toISOString(),
        limit: EVENT_PAGE_LIMIT,
        after,
      },
      signal,
    );
    events.push(...result.items);
    if (!result.next) return { events, complete: true };
    after = result.next;
  }
  return { events, complete: false };
}

export function nodesLatest(signal?: AbortSignal): Promise<NodeSample[]> {
  return getJson<NodeSample[]>("/essential-metrics/nodes/latest", {}, signal);
}

export function nodeSeries(
  nodeId: string,
  range: TimeRange,
  bucketSeconds: number,
  signal?: AbortSignal,
): Promise<NodeSample[]> {
  return getJson<NodeSample[]>(
    `/essential-metrics/nodes/${encodeURIComponent(nodeId)}/series`,
    {
      from: range.from.toISOString(),
      to: range.to.toISOString(),
      bucket_seconds: bucketSeconds,
    },
    signal,
  );
}
