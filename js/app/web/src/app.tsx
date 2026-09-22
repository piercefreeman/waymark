import { useEffect, useMemo } from "react";
import {
  getInstance,
  listEvents,
  listInstances,
  nodeSeries,
  nodesLatest,
  vmTimeline,
} from "./api/client";
import { AppShell, useTimeWindow } from "./components/layout/app-shell";
import type { SourceStatus } from "./components/patterns/source-notice";
import { TooltipProvider } from "./components/ui/tooltip";
import * as fixtures from "./data/fixtures";
import { useLive } from "./data/live";
import type { Event, Instance, NodeSample } from "./domain/api";
import {
  deriveFromInstance,
  deriveInstance,
  type InstanceSummary,
} from "./domain/derive";
import { InstanceDetail } from "./features/instances/detail";
import { InstanceList } from "./features/instances/list";
import { FleetPage } from "./features/fleet/page";
import { Gallery } from "./features/gallery/gallery";
import { shortId } from "./lib/format";
import { matchPath, navigate, useLocation, useSearchParam } from "./lib/router";
import { useNow } from "./lib/use-now";
import { ThemeProvider } from "./providers/theme";

const POLL_MS = 5000;
/** The server's default metrics sample interval (WAYMARK_ESSENTIAL_METRICS_SAMPLE_INTERVAL_MS). */
const SAMPLE_INTERVAL_MS = 10_000;

/**
 * Routes. Data comes from the API mounted at `/api` on the same origin.
 * `?source=sample` switches every page to the authored fixtures; nothing
 * ever falls back to them silently.
 */
export function App() {
  return (
    <ThemeProvider>
      <TooltipProvider>
        <Router />
      </TooltipProvider>
    </ThemeProvider>
  );
}

function useSourceMode() {
  const [source] = useSearchParam("source");
  const [paused] = useSearchParam("paused");
  return { sample: source === "sample", paused: paused === "1" };
}

function Router() {
  const { pathname } = useLocation();
  useEffect(() => {
    if (pathname === "/" || pathname === "")
      navigate("/instances", { replace: true });
  }, [pathname]);

  const detail = matchPath("/instances/:vmId", pathname);
  if (detail) return <DetailRoute vmId={detail.vmId} />;
  if (matchPath("/fleet", pathname)) return <FleetRoute />;
  if (matchPath("/gallery", pathname)) return <GalleryRoute />;
  return <InstancesRoute />;
}

function groupByVm(events: Event[]): Map<string, Event[]> {
  const byVm = new Map<string, Event[]>();
  for (const event of events) {
    const list = byVm.get(event.payload.vm_id);
    if (list) list.push(event);
    else byVm.set(event.payload.vm_id, [event]);
  }
  return byVm;
}

function InstancesRoute() {
  const now = useNow();
  const [timeWindow] = useTimeWindow();
  const { sample, paused } = useSourceMode();
  const live = useLive(
    async (signal) => {
      const range = {
        from: new Date(Date.now() - timeWindow.ms),
        to: new Date(),
      };
      const [page, events] = await Promise.all([
        listInstances(range, undefined, signal),
        listEvents(range, signal),
      ]);
      return {
        instances: page.items,
        events: events.events,
        complete: events.complete,
      };
    },
    { intervalMs: POLL_MS, enabled: !paused && !sample, key: timeWindow.id },
  );

  const instances = useMemo<InstanceSummary[]>(() => {
    if (sample) {
      const from = fixtures.now.getTime() - timeWindow.ms;
      return fixtures.instances.filter(
        (instance) => instance.lastEventAt.getTime() >= from,
      );
    }
    if (!live.data) return [];
    const byVm = groupByVm(live.data.events);
    return live.data.instances.map((dto: Instance) =>
      deriveFromInstance(dto, byVm.get(dto.vm_id) ?? [], now),
    );
  }, [sample, live.data, now, timeWindow.ms]);

  const source = sourceStatus(sample, live);
  return (
    <AppShell
      title="Instances"
      now={sample ? fixtures.now : now}
      source={source}
    >
      <InstanceList
        instances={instances}
        now={sample ? fixtures.now : now}
        windowLabel={timeWindow.label}
        source={source}
      />
    </AppShell>
  );
}

function DetailRoute({ vmId }: { vmId: string }) {
  const now = useNow();
  const { sample, paused } = useSourceMode();
  const live = useLive(
    async (signal) => {
      const [dto, timeline] = await Promise.all([
        getInstance(vmId, signal),
        vmTimeline(vmId, signal),
      ]);
      return { dto, events: timeline.events, complete: timeline.complete };
    },
    { intervalMs: POLL_MS, enabled: !paused && !sample, key: vmId },
  );
  const summary = useMemo(() => {
    if (sample) return fixtures.instanceById(vmId);
    if (!live.data) return undefined;
    return live.data.events.length
      ? deriveInstance(vmId, live.data.events, now)
      : deriveFromInstance(live.data.dto, [], now);
  }, [sample, live.data, now, vmId]);
  const source = sourceStatus(sample, live);
  return (
    <AppShell
      title={`Instance ${shortId(vmId)}`}
      now={sample ? fixtures.now : now}
      source={source}
    >
      <InstanceDetail
        summary={summary}
        now={sample ? fixtures.now : now}
        source={source}
      />
    </AppShell>
  );
}

function FleetRoute() {
  const now = useNow();
  const [timeWindow] = useTimeWindow();
  const { sample, paused } = useSourceMode();
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
    { intervalMs: POLL_MS, enabled: !paused && !sample, key: timeWindow.id },
  );
  const source = sourceStatus(sample, live);
  const current = sample ? fixtures.now : now;
  const window = {
    from: new Date(current.getTime() - timeWindow.ms),
    to: current,
  };
  return (
    <AppShell title="Fleet" now={current} source={source}>
      <FleetPage
        latest={sample ? fixtures.latestSamples : (live.data?.latest ?? [])}
        seriesByNode={
          sample ? fixtures.seriesByNode : (live.data?.seriesByNode ?? {})
        }
        now={current}
        window={window}
        sampleIntervalMs={
          sample ? fixtures.SAMPLE_INTERVAL_MS : SAMPLE_INTERVAL_MS
        }
        source={source}
      />
    </AppShell>
  );
}

function GalleryRoute() {
  const source: SourceStatus = {
    kind: "sample",
    fetchedAt: fixtures.now,
    error: null,
    loading: false,
    complete: true,
  };
  return (
    <AppShell title="Component gallery" now={fixtures.now} source={source}>
      <Gallery instances={fixtures.instances} now={fixtures.now} />
    </AppShell>
  );
}

function sourceStatus(
  sample: boolean,
  live: {
    fetchedAt: Date | null;
    error: Error | null;
    loading: boolean;
    data: { complete: boolean } | null;
    refresh: () => void;
  },
): SourceStatus {
  if (sample)
    return {
      kind: "sample",
      fetchedAt: fixtures.now,
      error: null,
      loading: false,
      complete: true,
    };
  return {
    kind: "live",
    fetchedAt: live.fetchedAt,
    error: live.error,
    loading: live.loading,
    complete: live.data?.complete ?? true,
    refresh: live.refresh,
  };
}
