import { Moon, RotateCcw } from "lucide-react";
import { cn } from "@/lib/cn";
import { formatClock, formatDuration, shortId } from "@/lib/format";
import { stopKindLabels } from "@/domain/api";
import type { InstanceSummary } from "@/domain/derive";
import { instanceStates, promiseStates, toneBackground } from "@/domain/status";
import { Identifier } from "@/components/patterns/identifier";
import { KeyValueList } from "@/components/patterns/key-value-list";
import { SectionHeader } from "@/components/patterns/section-header";
import { EventLog } from "@/components/patterns/event-log";
import { TimeAgo } from "@/components/patterns/time";

/**
 * Quick look: identity, the derived state and its evidence, then the
 * promise ledger. Called on the left, settled on the right; anything without
 * a right-hand entry is outstanding and that is the point.
 */
export function InstancePeek({
  summary,
  now,
}: {
  summary: InstanceSummary;
  now: Date;
}) {
  const run = summary.latestRun;
  return (
    <div className="divide-y divide-line">
      <section className="px-gutter py-3">
        <Identifier value={summary.vmId} full copyable className="text-label" />
        <p className="mt-1 text-micro text-fg-muted">
          {instanceStates[summary.state].rule}
        </p>
        <KeyValueList
          layout="grid"
          className="mt-3"
          items={[
            {
              label: "First event",
              value: <TimeAgo at={summary.firstEventAt} now={now} />,
              mono: true,
            },
            {
              label: "Last activity",
              value: <TimeAgo at={summary.lastEventAt} now={now} />,
              mono: true,
            },
            {
              label: "Latest run",
              value: run
                ? `run ${run.index} · ${shortId(run.nodeId).slice(0, 8)}`
                : "—",
              mono: true,
              note: run?.stopReason
                ? stopKindLabels[run.stopReason.kind]
                : "not stopped",
            },
            { label: "Driver runs", value: summary.runs.length, mono: true },
            {
              label: "Snapshots",
              value: summary.snapshots.length,
              mono: true,
              note: summary.snapshots.length
                ? `last ${formatClock(summary.snapshots[summary.snapshots.length - 1].at)}`
                : undefined,
            },
            {
              label: "Missing events",
              value: summary.missingEvents,
              mono: true,
              note: summary.missingEvents ? "gaps in run sequence" : undefined,
            },
          ]}
        />
        {summary.runErrorMessage && (
          <p className="mono-data mt-3 rounded-panel border border-danger/40 bg-danger/10 px-2.5 py-2 text-micro text-danger">
            {summary.runErrorMessage}
          </p>
        )}
      </section>

      <section className="px-gutter py-3">
        <SectionHeader
          as="h3"
          title="Promise ledger"
          count={`${summary.counts.resolved + summary.counts.rejected} settled · ${summary.counts.open} open`}
        />
        <ol className="mt-2 divide-y divide-line border-y border-line">
          {summary.promises.map((promise) => {
            const tone = promiseStates[promise.state].tone;
            return (
              <li
                key={promise.id}
                className="grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-2 py-1.5 text-micro"
              >
                <span className="min-w-0">
                  <span className="mono-data flex items-center gap-1.5 truncate text-fg">
                    {promise.kind === "sleep" && (
                      <Moon className="size-3 text-fg-muted" aria-hidden />
                    )}
                    {promise.kind === "sleep"
                      ? `sleep ${formatDuration(promise.sleepMs)}`
                      : promise.name}
                    <span className="text-fg-subtle">#{promise.id}</span>
                  </span>
                  <span className="mono-data block text-[10px] leading-3 text-fg-subtle">
                    called {formatClock(promise.calledAt)}
                    {promise.possibleRetryOf !== null && (
                      <span className="ml-1 inline-flex items-center gap-0.5 text-waiting">
                        <RotateCcw className="size-2.5" aria-hidden /> retry of
                        #{promise.possibleRetryOf}, inferred
                      </span>
                    )}
                  </span>
                </span>
                <span
                  aria-hidden
                  className={cn(
                    "h-px w-6",
                    promise.state === "open"
                      ? "border-t border-dashed border-line-strong"
                      : toneBackground[tone],
                  )}
                />
                <span className="min-w-0 text-right">
                  {promise.state === "open" ? (
                    <span className="text-running">
                      open ·{" "}
                      {formatDuration(
                        now.getTime() - promise.calledAt.getTime(),
                      )}
                      {promise.wakeAt && (
                        <span className="block text-[10px] leading-3 text-fg-subtle">
                          wakes {formatClock(promise.wakeAt)}
                        </span>
                      )}
                    </span>
                  ) : (
                    <>
                      <span
                        className={cn(
                          "mono-data block truncate",
                          promise.state === "rejected"
                            ? "text-danger"
                            : "text-fg",
                        )}
                      >
                        {promise.state === "rejected"
                          ? promise.exceptionType
                          : "resolved"}
                      </span>
                      <span className="mono-data block text-[10px] leading-3 text-fg-subtle">
                        {formatDuration(
                          (promise.settledAt?.getTime() ?? 0) -
                            promise.calledAt.getTime(),
                        )}{" "}
                        · {formatClock(promise.settledAt ?? promise.calledAt)}
                      </span>
                    </>
                  )}
                </span>
              </li>
            );
          })}
          {summary.promises.length === 0 && (
            <li className="py-3 text-center text-micro text-fg-subtle">
              No promises called yet
            </li>
          )}
        </ol>
      </section>

      <section className="py-3">
        <SectionHeader
          as="h3"
          title="Recent events"
          count={summary.events.length}
          className="px-gutter"
        />
        <EventLog events={summary.events.slice(-8)} className="mt-1" />
      </section>
    </div>
  );
}
