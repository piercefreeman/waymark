import { cn } from "@/lib/cn";
import { formatClock } from "@/lib/format";
import type { Event } from "@/domain/api";
import { isErrorStop } from "@/domain/api";
import { toneVariable } from "@/domain/status";
import { timeTicks } from "@/components/patterns/time-series";

/**
 * Settlements per bucket across the window, stacked by outcome. Shows when
 * things broke, which a count of the list below cannot.
 */
export function ActivityHistogram({
  events,
  window,
  bucketMs = 30_000,
  height = 56,
  className,
}: {
  events: Event[];
  window: { from: Date; to: Date };
  bucketMs?: number;
  height?: number;
  className?: string;
}) {
  const span = window.to.getTime() - window.from.getTime();
  const bucketCount = Math.ceil(span / bucketMs);
  const buckets = Array.from({ length: bucketCount }, () => ({
    resolved: 0,
    rejected: 0,
    errors: 0,
    calls: 0,
  }));
  for (const event of events) {
    const at = new Date(event.at).getTime();
    const index = Math.floor((at - window.from.getTime()) / bucketMs);
    if (index < 0 || index >= bucketCount) continue;
    const observation = event.payload.observation;
    if (observation.kind === "promise_settled")
      buckets[index][
        observation.settlement.kind === "rejected" ? "rejected" : "resolved"
      ] += 1;
    else if (observation.kind === "effect_emitted") {
      if (observation.effect.kind === "action_call") buckets[index].calls += 1;
      if (observation.effect.kind === "unhandled_exception")
        buckets[index].errors += 1;
    } else if (
      observation.kind === "vm_stopped" &&
      isErrorStop(observation.reason.kind)
    )
      buckets[index].errors += 1;
  }
  const peak = Math.max(
    1,
    ...buckets.map(
      (bucket) => bucket.resolved + bucket.rejected + bucket.errors,
    ),
  );
  const width = 600;
  const barWidth = width / bucketCount;
  const ticks = timeTicks(window);
  const total = buckets.reduce(
    (sum, bucket) => ({
      resolved: sum.resolved + bucket.resolved,
      rejected: sum.rejected + bucket.rejected,
      errors: sum.errors + bucket.errors,
      calls: sum.calls + bucket.calls,
    }),
    { resolved: 0, rejected: 0, errors: 0, calls: 0 },
  );
  return (
    <figure className={cn("min-w-0", className)}>
      <figcaption className="mb-1 flex items-baseline justify-between text-micro text-fg-muted">
        <span>
          Settlements per {bucketMs / 1000}s
          <span className="ml-1 text-fg-subtle">· events in this window</span>
        </span>
        <span className="flex gap-3">
          <Legend tone="success" label="resolved" value={total.resolved} />
          <Legend tone="danger" label="rejected" value={total.rejected} />
          <Legend tone="danger" label="errors" value={total.errors} hatched />
          <span className="text-fg-subtle">
            <span className="mono-data text-fg">{total.calls}</span> calls
          </span>
        </span>
      </figcaption>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        role="img"
        aria-label={`${total.resolved} resolved, ${total.rejected} rejected, ${total.errors} errors in the window`}
        className="block w-full"
        style={{ height }}
      >
        <defs>
          <pattern
            id="activity-hatch"
            width="3"
            height="3"
            patternUnits="userSpaceOnUse"
            patternTransform="rotate(45)"
          >
            <rect width="1.2" height="3" fill={toneVariable.danger} />
          </pattern>
        </defs>
        <line
          x1={0}
          x2={width}
          y1={height - 0.5}
          y2={height - 0.5}
          stroke="var(--line)"
          vectorEffect="non-scaling-stroke"
        />
        {ticks.map((tick) => {
          const x = ((tick.getTime() - window.from.getTime()) / span) * width;
          return (
            <line
              key={tick.getTime()}
              x1={x}
              x2={x}
              y1={0}
              y2={height}
              stroke="var(--chart-grid)"
              vectorEffect="non-scaling-stroke"
            />
          );
        })}
        {buckets.map((bucket, index) => {
          const scale = (height - 2) / peak;
          let y = height - 1;
          const segments = [
            { value: bucket.resolved, fill: toneVariable.success },
            { value: bucket.rejected, fill: toneVariable.danger },
            { value: bucket.errors, fill: "url(#activity-hatch)" },
          ];
          return (
            <g key={index}>
              {segments.map((segment, segmentIndex) => {
                if (segment.value === 0) return null;
                const segmentHeight = segment.value * scale;
                y -= segmentHeight;
                return (
                  <rect
                    key={segmentIndex}
                    x={index * barWidth + 0.5}
                    width={Math.max(1, barWidth - 1)}
                    y={y}
                    height={segmentHeight}
                    fill={segment.fill}
                    opacity={segmentIndex === 0 ? 0.75 : 1}
                  />
                );
              })}
            </g>
          );
        })}
      </svg>
      <div
        aria-hidden
        className="relative mt-1 h-3.5 text-[10px] text-fg-subtle"
      >
        {ticks.map((tick, index) => (
          <span
            key={tick.getTime()}
            className={cn(
              "mono-data absolute top-0 leading-none",
              index === ticks.length - 1
                ? "-translate-x-full"
                : "-translate-x-1/2",
            )}
            style={{
              left: `${((tick.getTime() - window.from.getTime()) / span) * 100}%`,
            }}
          >
            {formatClock(tick, false)}
          </span>
        ))}
      </div>
    </figure>
  );
}

function Legend({
  tone,
  label,
  value,
  hatched = false,
}: {
  tone: "success" | "danger";
  label: string;
  value: number;
  hatched?: boolean;
}) {
  return (
    <span className="inline-flex items-center gap-1">
      <span
        aria-hidden
        className={cn(
          "inline-block size-2 rounded-[1px]",
          hatched && "hatched border border-danger",
        )}
        style={
          hatched
            ? undefined
            : {
                background: toneVariable[tone],
                opacity: tone === "success" ? 0.75 : 1,
              }
        }
      />
      <span className="mono-data text-fg">{value}</span> {label}
    </span>
  );
}
