import { useEffect, useMemo, useRef } from "react";
import { getInstance, nodeSeries, nodesLatest, vmTimeline } from "./api/client";
import { AppShell, useTimeWindow } from "./components/layout/app-shell";
import type { SourceStatus } from "./components/patterns/source-notice";
import { TooltipProvider } from "./components/ui/tooltip";
import { fetchInstancePage } from "./data/instances";
import { useLive } from "./data/live";
import { fetchTimelines, type CachedTimeline } from "./data/timelines";
import { instanceStates, type InstanceState } from "./domain/status";
import type { Instance, NodeSample } from "./domain/api";
import {
  deriveFromInstance,
  deriveInstance,
  type InstanceSummary,
} from "./domain/derive";
import { InstanceDetail } from "./features/instances/detail";
import { InstanceList, type PageInfo } from "./features/instances/list";
import { FleetPage } from "./features/fleet/page";
import { shortId } from "./lib/format";
import { matchPath, navigate, useLocation, useSearchParam } from "./lib/router";
import { useNow } from "./lib/use-now";
import { ThemeProvider } from "./providers/theme";

const POLL_MS = 5000;
/** The server's default metrics sample interval (WAYMARK_ESSENTIAL_METRICS_SAMPLE_INTERVAL_MS). */
const SAMPLE_INTERVAL_MS = 10_000;

/** Routes. Data comes from the API mounted at `/api` on the same origin. */
export function App() {
  return (
    <ThemeProvider>
      <TooltipProvider>
        <Router />
      </TooltipProvider>
    </ThemeProvider>
  );
}

function Router() {
  const { pathname } = useLocation();
  useEffect(() => {
    if (pathname === "/" || pathname === "")
      navigate("/workflows", { replace: true });
  }, [pathname]);

  const detail = matchPath("/workflows/:vmId", pathname);
  if (detail) return <DetailRoute vmId={detail.vmId} />;
  if (matchPath("/fleet", pathname)) return <FleetRoute />;
  return <InstancesRoute />;
}

function InstancesRoute() {
  const timelineCache = useRef(new Map<string, CachedTimeline>());
  const now = useNow();
  const [timeWindow] = useTimeWindow();
  const [paused] = useSearchParam("paused");
  const [after] = useSearchParam("after");
  const [pinnedTo] = useSearchParam("to");
  const [query] = useSearchParam("q");
  const [stateParam] = useSearchParam("state");
  const states = useMemo(
    () =>
      (stateParam ?? "")
        .split(",")
        .filter((value): value is InstanceState => value in instanceStates),
    [stateParam],
  );
  const pinned = pinnedTo ? new Date(pinnedTo) : null;
  const key = [
    timeWindow.id,
    after ?? "",
    pinnedTo ?? "",
    query ?? "",
    stateParam ?? "",
  ].join("|");

  const live = useLive(
    async (signal) => {
      const to = pinned ?? new Date();
      const page = await fetchInstancePage(
        {
          from: new Date(to.getTime() - timeWindow.ms),
          to,
          after,
          query: query ?? "",
          states,
          now: new Date(),
        },
        signal,
      );
      const timelineRead = await fetchTimelines(
        page.items,
        timelineCache.current,
        signal,
      );
      signal.throwIfAborted();
      timelineCache.current = timelineRead.timelines;
      return { ...page, to, ...timelineRead };
    },
    {
      intervalMs: POLL_MS,
      // A pinned `to` is a frozen page: nothing after it can appear, so
      // there is nothing to poll for.
      enabled: paused !== "1" && pinned === null,
      key,
    },
  );

  const instances = useMemo<InstanceSummary[]>(() => {
    return (live.data?.items ?? []).map((dto: Instance) =>
      deriveFromInstance(
        dto,
        live.data?.timelines.get(dto.vm_id)?.events ?? [],
        now,
      ),
    );
  }, [live.data, now]);

  const source = sourceStatus(live);
  const page: PageInfo = {
    next: live.data?.next ?? null,
    after,
    scanned: live.data?.scanned ?? 0,
    capped: live.data?.capped ?? false,
    direct: live.data?.direct ?? false,
  };
  const to = live.data?.to ?? pinned ?? now;
  return (
    <AppShell title="Workflows" now={now} source={source} pinnedTo={pinned}>
      <InstanceList
        instances={instances}
        now={now}
        range={{ from: new Date(to.getTime() - timeWindow.ms), to }}
        source={source}
        page={page}
      />
    </AppShell>
  );
}

function DetailRoute({ vmId }: { vmId: string }) {
  const now = useNow();
  const [paused] = useSearchParam("paused");
  const live = useLive(
    async (signal) => {
      const [dto, timeline] = await Promise.all([
        getInstance(vmId, signal),
        vmTimeline(vmId, signal),
      ]);
      return { dto, events: timeline.events, complete: timeline.complete };
    },
    { intervalMs: POLL_MS, enabled: paused !== "1", key: vmId },
  );
  const summary = useMemo(() => {
    if (!live.data) return undefined;
    return live.data.events.length
      ? deriveInstance(vmId, live.data.events, now)
      : deriveFromInstance(live.data.dto, [], now);
  }, [live.data, now, vmId]);
  const source = sourceStatus(live);
  return (
    <AppShell title={`Workflow ${shortId(vmId)}`} now={now} source={source}>
      <InstanceDetail summary={summary} now={now} source={source} />
    </AppShell>
  );
}

function FleetRoute() {
  const now = useNow();
  const [timeWindow] = useTimeWindow();
  const [paused] = useSearchParam("paused");
  const live = useLive(
    async (signal) => {
      const range = {
        from: new Date(Date.now() - timeWindow.ms),
        to: new Date(),
      };
      const latest = await nodesLatest(signal);
      const series = await Promise.all(
        latest.map((node) =>
          nodeSeries(node.node_id, range, SAMPLE_INTERVAL_MS / 1000, signal),
        ),
      );
      const seriesByNode: Record<string, NodeSample[]> = {};
      latest.forEach((node, index) => {
        seriesByNode[node.node_id] = series[index];
      });
      return { latest, seriesByNode, complete: true };
    },
    { intervalMs: POLL_MS, enabled: paused !== "1", key: timeWindow.id },
  );
  const source = sourceStatus(live);
  const window = {
    from: new Date(now.getTime() - timeWindow.ms),
    to: now,
  };
  return (
    <AppShell title="Fleet" now={now} source={source}>
      <FleetPage
        latest={live.data?.latest ?? []}
        seriesByNode={live.data?.seriesByNode ?? {}}
        now={now}
        window={window}
        sampleIntervalMs={SAMPLE_INTERVAL_MS}
        source={source}
      />
    </AppShell>
  );
}

function sourceStatus(live: {
  fetchedAt: Date | null;
  error: Error | null;
  loading: boolean;
  data: { complete: boolean } | null;
  refresh: () => void;
}): SourceStatus {
  return {
    fetchedAt: live.fetchedAt,
    error: live.error,
    loading: live.loading,
    complete: live.data?.complete ?? true,
    refresh: live.refresh,
  };
}
