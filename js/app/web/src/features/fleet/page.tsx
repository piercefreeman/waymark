import { cn } from "@/lib/cn";
import {
  formatInteger,
  formatRate,
  formatRelative,
  formatSeconds,
  shortId,
} from "@/lib/format";
import type { NodeSample } from "@/domain/api";
import {
  aggregateHistograms,
  alignSeries,
  completionRate,
  histogramPercentile,
  isStale,
} from "@/domain/metrics";
import type { Tone } from "@/domain/status";
import { EmptyState } from "@/components/patterns/empty-state";
import { Meter } from "@/components/patterns/meter";
import { MetricStrip, MetricTile } from "@/components/patterns/metric";
import { SectionHeader } from "@/components/patterns/section-header";
import {
  SourceNotice,
  type SourceStatus,
} from "@/components/patterns/source-notice";
import { StatusInk } from "@/components/patterns/status-ink";
import {
  TimeSeriesChart,
  type ChartSeries,
} from "@/components/patterns/time-series";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

const NODE_TONES: Tone[] = ["running", "waiting", "success", "neutral"];

/**
 * Fleet: is the cluster keeping up? Four numbers with their scope, one row
 * per node boot that reads as a sentence, and two charts on a shared axis.
 * Latency is text; distributions live in the peek of a future iteration.
 */
export function FleetPage({
  latest,
  seriesByNode,
  now,
  window,
  sampleIntervalMs,
  source,
}: {
  latest: NodeSample[];
  seriesByNode: Record<string, NodeSample[]>;
  now: Date;
  window: { from: Date; to: Date };
  sampleIntervalMs: number;
  source: SourceStatus;
}) {
  const rows = latest
    .map((sample, index) => {
      const sampledAt = new Date(sample.sampled_at);
      const stale = isStale(sampledAt, now, sampleIntervalMs);
      return {
        sample,
        sampledAt,
        stale,
        tone: NODE_TONES[index % NODE_TONES.length],
        rate: completionRate(seriesByNode[sample.node_id] ?? [sample]),
      };
    })
    .sort(
      (a, b) =>
        Number(a.stale) - Number(b.stale) ||
        b.sampledAt.getTime() - a.sampledAt.getTime(),
    );
  const fresh = rows.filter((row) => !row.stale);
  const excluded = rows.length - fresh.length;
  const sum = (pick: (sample: NodeSample) => number) =>
    fresh.reduce((total, row) => total + pick(row.sample), 0);
  const inFlight = sum((sample) => sample.in_flight_actions);
  const capacity = sum((sample) => sample.max_in_flight_actions);
  const handling = aggregateHistograms(
    fresh.map((row) => row.sample.action_handling_seconds),
  );
  const dequeue = aggregateHistograms(
    fresh.map((row) => row.sample.action_dequeue_seconds),
  );
  const scope =
    fresh.length === 0
      ? "no fresh nodes"
      : `${fresh.length} node${fresh.length === 1 ? "" : "s"}${excluded ? ` · ${excluded} stale excluded` : ""}`;

  const series = (pick: (sample: NodeSample) => number | null): ChartSeries[] =>
    rows.map((row) => ({
      id: row.sample.node_id,
      label: shortId(row.sample.node_id).slice(0, 8),
      tone: row.tone,
      points: alignSeries(
        seriesByNode[row.sample.node_id] ?? [],
        pick,
        window,
        sampleIntervalMs,
      ),
    }));

  return (
    <div className="min-w-0">
      <div className="px-gutter pt-5">
        <SectionHeader
          title="Fleet"
          count={rows.length}
          description={`node boot${rows.length === 1 ? "" : "s"} · sampled every ${Math.round(sampleIntervalMs / 1000)}s · stale after ${Math.round((sampleIntervalMs * 3) / 1000)}s`}
        />
      </div>

      <SourceNotice source={source} now={now} className="mx-gutter mt-4" />

      {rows.length === 0 ? (
        <EmptyState
          className="mt-4 border-t border-line"
          variant={source.error ? "unavailable" : "empty"}
          title={source.error ? "Metrics unavailable" : "No node samples yet"}
          description={
            source.error
              ? source.error.message
              : "Nodes appear after their first metrics sample."
          }
        />
      ) : (
        <>
          <MetricStrip className="mt-4 lg:grid-cols-4">
            <MetricTile
              label="In flight"
              value={inFlight}
              unit={`of ${capacity} slots`}
              scope={scope}
            />
            <MetricTile
              label="Queued"
              value={sum((sample) => sample.queued_action_dispatches)}
              unit="dispatches"
              scope={
                dequeue
                  ? `dequeue p95 ${formatSeconds(histogramPercentile(dequeue, 0.95))}`
                  : "waiting for a worker slot"
              }
              tone={
                dequeue && (histogramPercentile(dequeue, 0.95) ?? 0) > 0.1
                  ? "waiting"
                  : undefined
              }
            />
            <MetricTile
              label="Completions"
              value={formatRate(
                fresh.reduce((total, row) => total + (row.rate ?? 0), 0),
              )}
              unit="per second"
              scope="from counter deltas, last interval"
            />
            <MetricTile
              label="Handling time"
              value={handling ? formatSeconds(handling.p50) : "—"}
              unit="p50"
              scope={
                handling
                  ? `p95 ${formatSeconds(histogramPercentile(handling, 0.95))} · since boot`
                  : "no histogram"
              }
            />
          </MetricStrip>

          <section className="px-gutter py-5">
            <SectionHeader as="h3" title="Nodes" />
            <ol className="mt-2 divide-y divide-line border-y border-line">
              {rows.map((row) => {
                const handlingP50 = row.sample.action_handling_seconds.p50;
                const handlingP95 = histogramPercentile(
                  row.sample.action_handling_seconds,
                  0.95,
                );
                const dequeueP95 = histogramPercentile(
                  row.sample.action_dequeue_seconds,
                  0.95,
                );
                return (
                  <li
                    key={row.sample.node_id}
                    className={cn(
                      "grid items-center gap-x-6 gap-y-2 py-3 lg:grid-cols-[220px_minmax(200px,1fr)_minmax(0,1.6fr)]",
                      row.stale && "text-fg-subtle",
                    )}
                  >
                    <span className="flex min-w-0 items-center gap-2.5">
                      <span
                        aria-hidden
                        className={cn(
                          "size-2 shrink-0 rounded-full",
                          row.stale && "opacity-40",
                        )}
                        style={{ background: `var(--${row.tone})` }}
                      />
                      <span className="min-w-0">
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span
                              className="mono-data block truncate text-label text-fg"
                              tabIndex={0}
                            >
                              {shortId(row.sample.node_id)}
                            </span>
                          </TooltipTrigger>
                          <TooltipContent className="mono-data">
                            {row.sample.node_id}
                          </TooltipContent>
                        </Tooltip>
                        <span className="block text-micro">
                          {row.stale ? (
                            <StatusInk
                              tone="waiting"
                              label={`stale · last sample ${formatRelative(row.sampledAt, now)}`}
                              size="sm"
                            />
                          ) : (
                            <StatusInk
                              tone="success"
                              label={`sampled ${formatRelative(row.sampledAt, now)}`}
                              size="sm"
                            />
                          )}
                        </span>
                      </span>
                    </span>
                    <span className={cn("min-w-0", row.stale && "opacity-60")}>
                      <span className="mb-1 block text-micro text-fg-subtle">
                        In flight
                      </span>
                      <Meter
                        value={row.sample.in_flight_actions}
                        max={row.sample.max_in_flight_actions}
                        label={`In-flight actions on ${shortId(row.sample.node_id)}`}
                      />
                    </span>
                    <span
                      className={cn(
                        "min-w-0 text-label text-fg",
                        row.stale && "opacity-60",
                      )}
                    >
                      <span className="block truncate">
                        <Num>{row.sample.worker_pool_size}</Num> workers ·{" "}
                        <Num>{row.sample.driven_vm_runtimes}</Num> resident VMs
                        ·{" "}
                        <Num
                          tone={
                            row.sample.queued_action_dispatches > 10
                              ? "waiting"
                              : undefined
                          }
                        >
                          {row.sample.queued_action_dispatches}
                        </Num>{" "}
                        queued · <Num>{formatRate(row.rate)}</Num> done/s
                      </span>
                      <span className="block truncate text-micro text-fg-subtle">
                        handling p50 {formatSeconds(handlingP50)} · p95{" "}
                        {formatSeconds(handlingP95)} · dequeue p95{" "}
                        {formatSeconds(dequeueP95)} ·{" "}
                        {formatInteger(row.sample.actions_completed_total)}{" "}
                        completed since boot
                        {row.sample.essential_metrics_dropped_total > 0 && (
                          <span className="text-waiting">
                            {" "}
                            · {row.sample.essential_metrics_dropped_total}{" "}
                            samples dropped
                          </span>
                        )}
                      </span>
                    </span>
                  </li>
                );
              })}
            </ol>
            <p className="mt-2 text-micro text-fg-subtle">
              A node id names one boot; a restart shows up as a new node.
              Hostnames, CPU, and memory are not reported.
            </p>
          </section>

          <section className="grid gap-x-10 gap-y-6 border-t border-line px-gutter py-5 lg:grid-cols-2">
            <TimeSeriesChart
              title="In flight"
              unit="actions, dashed line is capacity"
              series={series((sample) => sample.in_flight_actions)}
              capacity={Math.max(
                ...rows.map((row) => row.sample.max_in_flight_actions),
              )}
              window={window}
              height={88}
            />
            <TimeSeriesChart
              title="Queued"
              unit="dispatches waiting for a slot"
              series={series((sample) => sample.queued_action_dispatches)}
              window={window}
              height={88}
            />
          </section>
        </>
      )}
    </div>
  );
}

function Num({ children, tone }: { children: React.ReactNode; tone?: Tone }) {
  return (
    <span
      className={cn("mono-data text-fg", tone === "waiting" && "text-waiting")}
    >
      {children}
    </span>
  );
}
