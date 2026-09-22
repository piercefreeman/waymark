import { Diamond, Moon, RotateCcw } from "lucide-react";
import { cn } from "@/lib/cn";
import {
  formatBytes,
  formatClock,
  formatDuration,
  shortId,
} from "@/lib/format";
import { stopKindLabels } from "@/domain/api";
import type { DerivedPromise, InstanceSummary } from "@/domain/derive";
import { promiseStates, toneBackground } from "@/domain/status";
import { onLinkClick } from "@/lib/router";
import { Tooltip, TooltipContent, TooltipTrigger } from "../ui/tooltip";

/**
 * The hero of the instance page. One row per promise the VM actually
 * called, grouped into strata by driver run. Bars run from the call to the
 * settlement; open promises run to now with a hatched tail. Rejections get a
 * red end tick. Inferred retries are marked but never numbered.
 */
export function Waterfall({
  summary,
  now,
  selectedId,
  hrefFor,
  className,
}: {
  summary: InstanceSummary;
  now: Date;
  selectedId: number | null;
  hrefFor: (promiseId: number) => string;
  className?: string;
}) {
  const live = summary.state === "active" || summary.counts.open > 0;
  const start = summary.firstEventAt.getTime();
  const end = Math.max(
    live ? now.getTime() : summary.lastEventAt.getTime(),
    start + 100,
  );
  const span = end - start;
  const pct = (at: number) => `${(((at - start) / span) * 100).toFixed(3)}%`;
  const ticks = niceTicks(span, 6).map((offset) => ({
    offset,
    left: `${((offset / span) * 100).toFixed(3)}%`,
  }));

  return (
    <div className={cn("min-w-0", className)}>
      <div className="grid grid-cols-[minmax(200px,260px)_minmax(0,1fr)] border-b border-line">
        <div className="px-gutter py-1 text-micro text-fg-subtle">Promise</div>
        <div className="relative h-6" aria-hidden>
          {ticks.map((tick) => (
            <span
              key={tick.offset}
              className="mono-data absolute top-1 -translate-x-1/2 text-[10px] leading-none text-fg-subtle first:translate-x-0"
              style={{ left: tick.left }}
            >
              +{formatDuration(tick.offset)}
            </span>
          ))}
        </div>
      </div>
      <ol className="divide-y divide-line">
        {summary.runs.map((run) => {
          const runEnd = (run.stoppedAt ?? new Date(end)).getTime();
          const runPromises = summary.promises.filter(
            (promise) => promise.runIndex === run.index,
          );
          const settledHere = summary.promises.filter(
            (promise) =>
              promise.runIndex !== run.index &&
              promise.settledAt &&
              promise.settledAt.getTime() >= run.startedAt.getTime() &&
              promise.settledAt.getTime() <= runEnd,
          );
          const snapshots = summary.snapshots.filter(
            (snapshot) => snapshot.runIndex === run.index,
          );
          return (
            <li key={run.index} className="bg-surface">
              <div className="grid grid-cols-[minmax(200px,260px)_minmax(0,1fr)] bg-surface-raised/60">
                <div className="flex min-w-0 items-center gap-1.5 truncate whitespace-nowrap px-gutter py-1.5 text-micro">
                  <span className="font-medium text-fg">Run {run.index}</span>
                  <span className="mono-data text-fg-muted">
                    {shortId(run.nodeId).slice(0, 8)}
                  </span>
                  {run.stopReason && (
                    <span
                      className={
                        "error" in run.stopReason
                          ? "text-danger"
                          : "text-fg-muted"
                      }
                    >
                      · {stopKindLabels[run.stopReason.kind]}
                    </span>
                  )}
                  {run.missingEvents > 0 && (
                    <span className="text-waiting">
                      · {run.missingEvents} missing
                    </span>
                  )}
                </div>
                <div className="relative h-7">
                  <div
                    className="absolute inset-y-1.5 border-x border-line-strong bg-fg/[0.04]"
                    style={{
                      left: pct(run.startedAt.getTime()),
                      width: `calc(${pct(runEnd)} - ${pct(run.startedAt.getTime())})`,
                    }}
                  >
                    <span className="mono-data absolute left-1 top-0.5 whitespace-nowrap text-[10px] leading-none text-fg-muted">
                      {formatClock(run.startedAt)}
                    </span>
                  </div>
                  {snapshots.map((snapshot) => (
                    <Tooltip key={snapshot.at.getTime()}>
                      <TooltipTrigger asChild>
                        <span
                          tabIndex={0}
                          className="absolute top-1/2 -translate-x-1/2 -translate-y-1/2 text-fg-muted"
                          style={{ left: pct(snapshot.at.getTime()) }}
                        >
                          <Diamond
                            className="size-2.5 fill-current"
                            aria-hidden
                          />
                          <span className="sr-only">Snapshot persisted</span>
                        </span>
                      </TooltipTrigger>
                      <TooltipContent>
                        Snapshot persisted · {formatBytes(snapshot.bytes)} at{" "}
                        {formatClock(snapshot.at)}
                      </TooltipContent>
                    </Tooltip>
                  ))}
                </div>
              </div>
              {[
                ...runPromises,
                ...settledHere.map((promise) => ({
                  ...promise,
                  settledOnly: true,
                })),
              ]
                .sort((a, b) => a.calledAt.getTime() - b.calledAt.getTime())
                .map((promise) => (
                  <PromiseRow
                    key={`${run.index}-${promise.id}`}
                    promise={promise}
                    settledOnly={"settledOnly" in promise}
                    selected={selectedId === promise.id}
                    href={hrefFor(promise.id)}
                    pct={pct}
                    end={end}
                    live={live}
                  />
                ))}
              {runPromises.length === 0 && settledHere.length === 0 && (
                <div className="grid grid-cols-[minmax(200px,260px)_minmax(0,1fr)]">
                  <div className="px-gutter py-1.5 text-micro text-fg-subtle">
                    No promises in this run
                  </div>
                </div>
              )}
            </li>
          );
        })}
      </ol>
    </div>
  );
}

function PromiseRow({
  promise,
  settledOnly,
  selected,
  href,
  pct,
  end,
  live,
}: {
  promise: DerivedPromise;
  settledOnly: boolean;
  selected: boolean;
  href: string;
  pct: (at: number) => string;
  end: number;
  live: boolean;
}) {
  const tone = promiseStates[promise.state].tone;
  const settledAt = promise.settledAt?.getTime() ?? end;
  const duration = promise.settledAt
    ? promise.settledAt.getTime() - promise.calledAt.getTime()
    : end - promise.calledAt.getTime();
  const endsLate = parseFloat(pct(settledAt)) > 72;
  return (
    <a
      href={href}
      onClick={onLinkClick}
      aria-current={selected ? "true" : undefined}
      data-row
      className={cn(
        "grid grid-cols-[minmax(200px,260px)_minmax(0,1fr)] border-l-2 border-transparent transition-colors duration-fast hover:bg-surface-raised",
        selected && "border-fg bg-surface-selected",
      )}
    >
      <span className="flex min-w-0 items-center gap-2 px-gutter py-1.5">
        <span
          aria-hidden
          className={cn("size-1.5 shrink-0 rounded-full", toneBackground[tone])}
        />
        <span className="min-w-0">
          <span className="mono-data block truncate text-label text-fg">
            {promise.kind === "sleep" ? (
              <span className="inline-flex items-center gap-1">
                <Moon className="size-3 text-fg-muted" aria-hidden />
                sleep {formatDuration(promise.sleepMs)}
              </span>
            ) : (
              promise.name
            )}
          </span>
          <span className="block truncate text-[10px] leading-3 text-fg-subtle">
            #{promise.id}
            {promise.possibleRetryOf !== null && (
              <Tooltip>
                <TooltipTrigger asChild>
                  <span
                    className="ml-1 inline-flex items-center gap-0.5 text-waiting"
                    tabIndex={0}
                  >
                    <RotateCcw className="size-2.5" aria-hidden />
                    retry of #{promise.possibleRetryOf}
                  </span>
                </TooltipTrigger>
                <TooltipContent>
                  Inferred: the same action name was called again after a
                  rejection. The API reports no attempt number.
                </TooltipContent>
              </Tooltip>
            )}
            {promise.module && ` · ${promise.module}`}
            {settledOnly && " · called in an earlier run"}
          </span>
        </span>
      </span>
      <span className="relative flex items-center">
        <span
          className={cn(
            "absolute top-1/2 h-3 -translate-y-1/2 rounded-[2px]",
            toneBackground[tone],
            promise.state === "open" && "hatched",
          )}
          style={{
            left: pct(promise.calledAt.getTime()),
            width: `max(2px, calc(${pct(settledAt)} - ${pct(promise.calledAt.getTime())}))`,
            opacity: promise.state === "open" ? 0.5 : 0.85,
          }}
        />
        {promise.state === "rejected" && (
          <span
            aria-hidden
            className="absolute top-1/2 h-4 w-0.5 -translate-y-1/2 bg-danger"
            style={{ left: `calc(${pct(settledAt)} - 1px)` }}
          />
        )}
        <span
          className={cn(
            "mono-data absolute top-1/2 -translate-y-1/2 whitespace-nowrap text-[10px] leading-none text-fg-muted",
            endsLate ? "-translate-x-full pr-1.5" : "pl-1.5",
          )}
          style={
            endsLate
              ? { left: pct(promise.calledAt.getTime()) }
              : { left: pct(settledAt) }
          }
        >
          {formatDuration(duration)}
          {promise.state === "open" && (live ? " · open" : " · open, stale")}
          {promise.exceptionType && (
            <span className="text-danger"> · {promise.exceptionType}</span>
          )}
        </span>
      </span>
    </a>
  );
}

function niceTicks(spanMs: number, count: number): number[] {
  const rough = spanMs / count;
  const steps = [
    10, 25, 50, 100, 250, 500, 1000, 2000, 5000, 10_000, 15_000, 30_000, 60_000,
    120_000, 300_000, 600_000, 900_000, 1_800_000, 3_600_000, 7_200_000,
  ];
  const step =
    steps.find((candidate) => candidate >= rough) ?? steps[steps.length - 1];
  const ticks: number[] = [];
  for (let offset = 0; offset <= spanMs; offset += step) ticks.push(offset);
  return ticks;
}
