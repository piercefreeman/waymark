import { cn } from "@/lib/cn";
import type { InstanceSummary } from "@/domain/derive";
import { promiseStates, toneVariable } from "@/domain/status";

/**
 * The instance's whole life in ~160px: one lane per promise, colored by
 * settlement, with a hatched tail for anything still open. Lets an operator
 * compare run shapes across a list without opening anything.
 */
export function MiniTimeline({
  summary,
  now,
  width = 160,
  height = 12,
  className,
}: {
  summary: InstanceSummary;
  now: Date;
  width?: number;
  height?: number;
  className?: string;
}) {
  const open = summary.promises.some((promise) => promise.state === "open");
  const live = summary.state === "active" || open;
  const start = summary.firstEventAt.getTime();
  const end = Math.max(
    live ? now.getTime() : summary.lastEventAt.getTime(),
    start + 1,
  );
  const span = end - start;
  const x = (at: number) => ((at - start) / span) * width;
  const lanes = Math.max(1, summary.promises.length);
  const laneHeight = Math.max(1.5, Math.min(3, (height - 2) / lanes));
  const laneGap =
    lanes > 1 ? (height - 2 - laneHeight * lanes) / (lanes - 1) : 0;
  return (
    <svg
      viewBox={`0 0 ${width} ${height}`}
      width={width}
      height={height}
      role="img"
      aria-label={`${summary.promises.length} promises: ${summary.counts.resolved} resolved, ${summary.counts.rejected} rejected, ${summary.counts.open} open`}
      className={cn("block shrink-0", className)}
    >
      {summary.runs.map((run) => (
        <rect
          key={run.index}
          x={x(run.startedAt.getTime())}
          width={Math.max(
            0.5,
            x((run.stoppedAt ?? new Date(end)).getTime()) -
              x(run.startedAt.getTime()),
          )}
          y={0}
          height={height}
          fill="var(--fg)"
          opacity={0.05}
        />
      ))}
      {summary.promises.map((promise, index) => {
        const y = 1 + index * (laneHeight + laneGap);
        const x0 = x(promise.calledAt.getTime());
        const x1 = x((promise.settledAt ?? new Date(end)).getTime());
        const tone = promiseStates[promise.state].tone;
        return (
          <g key={promise.id}>
            <rect
              x={x0}
              y={y}
              width={Math.max(1, x1 - x0)}
              height={laneHeight}
              rx={0.5}
              fill={toneVariable[tone]}
              opacity={promise.state === "open" ? 0.45 : 0.9}
            />
            {promise.state === "rejected" && (
              <rect
                x={Math.max(0, x1 - 1.5)}
                y={y - 0.5}
                width={1.5}
                height={laneHeight + 1}
                fill={toneVariable.danger}
              />
            )}
          </g>
        );
      })}
      {summary.outcome && (
        <rect
          x={Math.min(width - 1, x(summary.outcome.at.getTime()))}
          y={0}
          width={1}
          height={height}
          fill={
            toneVariable[
              summary.outcome.kind === "complete" ? "success" : "danger"
            ]
          }
        />
      )}
      {summary.latestRun?.stopReason &&
        "error" in summary.latestRun.stopReason &&
        summary.latestRun.stoppedAt && (
          <rect
            x={Math.min(width - 1, x(summary.latestRun.stoppedAt.getTime()))}
            y={0}
            width={1}
            height={height}
            fill={toneVariable.danger}
          />
        )}
    </svg>
  );
}
