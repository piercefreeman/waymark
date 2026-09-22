import { ArrowLeft } from "lucide-react";
import { formatClock, formatDuration, shortId } from "@/lib/format";
import {
  onLinkClick,
  useLocation,
  useSearchParam,
  withSearch,
} from "@/lib/router";
import { stopKindLabels, stopReasonError } from "@/domain/api";
import type { InstanceSummary } from "@/domain/derive";
import { instanceStates } from "@/domain/status";
import { EmptyState } from "@/components/patterns/empty-state";
import { EventLog } from "@/components/patterns/event-log";
import { Identifier } from "@/components/patterns/identifier";
import { KeyValueList } from "@/components/patterns/key-value-list";
import { PayloadViewer } from "@/components/patterns/payload-viewer";
import { SectionHeader } from "@/components/patterns/section-header";
import {
  SourceNotice,
  type SourceStatus,
} from "@/components/patterns/source-notice";
import {
  InstanceStateInk,
  PromiseStateInk,
} from "@/components/patterns/status-ink";
import { TimeAgo } from "@/components/patterns/time";
import { Waterfall } from "@/components/patterns/waterfall";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { describeNow, elapsedMs } from "./list";

const NOT_RECORDED =
  "Waymark records the action name, module, and exception type. Arguments and returned values are not captured by the event API.";

/**
 * The instance page. A sentence and a few facts up top, the waterfall as
 * the hero, and a docked drawer beneath it for the selected promise, the
 * raw event log, and the driver runs.
 */
export function InstanceDetail({
  summary,
  now,
  source,
}: {
  summary: InstanceSummary | undefined;
  now: Date;
  source: SourceStatus;
}) {
  const { pathname, search } = useLocation();
  const [promiseParam, setPromiseParam] = useSearchParam("promise");
  const [tab, setTab] = useSearchParam("tab");

  if (!summary) {
    if (source.loading && !source.error)
      return (
        <p className="px-gutter py-6 text-label text-fg-muted">Loading…</p>
      );
    return (
      <EmptyState
        variant={source.error ? "unavailable" : "empty"}
        title="Instance not found"
        description={
          source.error?.message ??
          "No events for this vm_id are retained in the observability store, or the id is mistyped."
        }
        action={
          <a
            href="/instances"
            onClick={onLinkClick}
            className="text-label text-accent hover:underline"
          >
            Back to instances
          </a>
        }
      />
    );
  }

  const selectedId = promiseParam ? Number(promiseParam) : null;
  const promise =
    summary.promises.find((item) => item.id === selectedId) ?? null;
  const settled = summary.counts.resolved + summary.counts.rejected;
  const latest = summary.latestRun;
  const activeTab = promise
    ? (tab ?? "promise")
    : tab === "promise"
      ? "events"
      : (tab ?? "events");
  const sentence = describeNow(summary, now);

  return (
    <div className="min-w-0">
      <div className="flex flex-wrap items-center gap-3 border-b border-line px-gutter py-2">
        <a
          href="/instances"
          onClick={onLinkClick}
          className="inline-flex items-center gap-1 text-micro text-fg-muted hover:text-fg"
        >
          <ArrowLeft className="size-3" aria-hidden />
          Instances
        </a>
        <span className="h-4 w-px bg-line" aria-hidden />
        <Identifier value={summary.vmId} full copyable className="text-label" />
        <InstanceStateInk state={summary.state} />
      </div>

      <SourceNotice source={source} now={now} className="mx-gutter mt-4" />

      <section className="px-gutter py-4">
        <p className="text-section text-fg">{sentence.headline}</p>
        <p className="mt-0.5 text-label text-fg-muted">
          {instanceStates[summary.state].rule}
        </p>
        <KeyValueList
          layout="grid"
          className="mt-4 max-w-4xl"
          items={[
            {
              label: "Started",
              value: formatClock(summary.firstEventAt),
              mono: true,
              note: <TimeAgo at={summary.firstEventAt} now={now} />,
            },
            {
              label: "Elapsed",
              value: formatDuration(elapsedMs(summary, now)),
              mono: true,
              note: summary.outcome ? "to the outcome" : "so far",
            },
            {
              label: "Promises",
              value: `${settled} settled · ${summary.counts.open} open`,
              mono: true,
              note: summary.counts.rejected
                ? `${summary.counts.rejected} rejected, caught or pending`
                : "no rejections",
            },
            {
              label: "Driver runs",
              value: summary.runs.length,
              mono: true,
              note: latest
                ? `latest on node ${shortId(latest.nodeId).slice(0, 8)} · ${latest.stopReason ? stopKindLabels[latest.stopReason.kind] : "not stopped"}`
                : "none observed",
            },
            {
              label: "Events",
              value: summary.events.length,
              mono: true,
              note: summary.missingEvents
                ? `${summary.missingEvents} missing from sequence`
                : "sequence complete",
            },
          ]}
        />
        {summary.runErrorMessage && (
          <p className="mono-data mt-4 max-w-4xl rounded-panel border border-danger/40 bg-danger/10 px-3 py-2 text-label text-danger">
            {latest?.stopReason
              ? stopKindLabels[latest.stopReason.kind]
              : "Run error"}
            : {summary.runErrorMessage}
          </p>
        )}
      </section>

      <section aria-label="Promise timeline" className="border-y border-line">
        {summary.promises.length === 0 ? (
          <p className="px-gutter py-4 text-label text-fg-muted">
            No promises were called in the retained history.
          </p>
        ) : (
          <Waterfall
            summary={summary}
            now={now}
            selectedId={selectedId}
            hrefFor={(id) =>
              withSearch(pathname, search, { promise: String(id), tab: null })
            }
          />
        )}
      </section>

      <Tabs
        value={activeTab}
        onValueChange={(value) =>
          setTab(value === "promise" ? null : value, { replace: true })
        }
        className="px-gutter pt-2"
      >
        <TabsList>
          {promise && (
            <TabsTrigger value="promise">
              <span className="mono-data">
                #{promise.id} {promise.name}
              </span>
              <button
                type="button"
                aria-label="Deselect promise"
                onClick={(event) => {
                  event.stopPropagation();
                  setPromiseParam(null);
                }}
                className="ml-1 text-fg-subtle hover:text-fg"
              >
                ×
              </button>
            </TabsTrigger>
          )}
          <TabsTrigger value="events">
            Events{" "}
            <span className="mono-data text-fg-subtle">
              {summary.events.length}
            </span>
          </TabsTrigger>
          <TabsTrigger value="runs">
            Runs{" "}
            <span className="mono-data text-fg-subtle">
              {summary.runs.length}
            </span>
          </TabsTrigger>
        </TabsList>

        {promise && (
          <TabsContent value="promise" className="py-4">
            <div className="grid gap-8 lg:grid-cols-[minmax(280px,1fr)_minmax(0,2fr)]">
              <div>
                <SectionHeader
                  as="h3"
                  title="Promise"
                  description={
                    <PromiseStateInk state={promise.state} size="sm" />
                  }
                />
                <KeyValueList
                  className="mt-1"
                  items={[
                    {
                      label: "Action",
                      value:
                        promise.kind === "sleep"
                          ? `sleep ${formatDuration(promise.sleepMs)}`
                          : promise.name,
                      mono: true,
                    },
                    {
                      label: "Module",
                      value: promise.module ?? "—",
                      mono: true,
                    },
                    { label: "Promise id", value: promise.id, mono: true },
                    {
                      label: "Effect number",
                      value: `${promise.effectNumber} in run ${promise.runIndex}`,
                      mono: true,
                    },
                    {
                      label: "Called",
                      value: formatClock(promise.calledAt),
                      mono: true,
                    },
                    {
                      label: "Settled",
                      value: promise.settledAt
                        ? formatClock(promise.settledAt)
                        : "open",
                      mono: true,
                      note: promise.settledAt
                        ? `${formatDuration(promise.settledAt.getTime() - promise.calledAt.getTime())} call → settlement, incl. queueing`
                        : `${formatDuration(now.getTime() - promise.calledAt.getTime())} so far`,
                    },
                    ...(promise.exceptionType
                      ? [
                          {
                            label: "Exception type",
                            value: promise.exceptionType,
                            mono: true,
                          },
                        ]
                      : []),
                    ...(promise.wakeAt
                      ? [
                          {
                            label: "Wakes",
                            value: formatClock(promise.wakeAt),
                            mono: true,
                            note: promise.skipAllowed
                              ? "skippable"
                              : "not skippable",
                          },
                        ]
                      : []),
                    ...(promise.possibleRetryOf !== null
                      ? [
                          {
                            label: "Possible retry of",
                            value: `#${promise.possibleRetryOf}`,
                            mono: true,
                            note: "inferred from a repeated action name after a rejection",
                          },
                        ]
                      : []),
                  ]}
                />
              </div>
              <div className="grid gap-3 sm:grid-cols-2">
                <PayloadViewer
                  label="Arguments"
                  payload={{ kind: "not-recorded", reason: NOT_RECORDED }}
                />
                <PayloadViewer
                  label={promise.state === "rejected" ? "Exception" : "Result"}
                  payload={
                    promise.state === "open"
                      ? { kind: "pending" }
                      : promise.state === "rejected"
                        ? {
                            kind: "recorded",
                            value: { exception_type: promise.exceptionType },
                          }
                        : { kind: "not-recorded", reason: NOT_RECORDED }
                  }
                />
                <div className="sm:col-span-2">
                  <SectionHeader
                    as="h3"
                    title="Events involving this promise"
                    className="mb-1"
                  />
                  <EventLog
                    events={summary.events.filter((event) => {
                      const observation = event.payload.observation;
                      return (
                        (observation.kind === "effect_emitted" &&
                          "promise_state_id" in observation.effect &&
                          observation.effect.promise_state_id === promise.id) ||
                        (observation.kind === "promise_settled" &&
                          observation.promise_state_id === promise.id)
                      );
                    })}
                    className="-mx-gutter"
                  />
                </div>
              </div>
            </div>
          </TabsContent>
        )}

        <TabsContent value="events" className="py-2">
          <EventLog
            events={summary.events}
            highlightPromiseId={selectedId}
            className="-mx-gutter"
          />
        </TabsContent>

        <TabsContent value="runs" className="py-4">
          <ol className="divide-y divide-line border-y border-line">
            {summary.runs.map((run) => {
              const error = run.stopReason
                ? stopReasonError(run.stopReason)
                : null;
              return (
                <li
                  key={run.index}
                  className="grid gap-x-6 gap-y-1 py-2 text-label sm:grid-cols-[80px_220px_minmax(0,1fr)]"
                >
                  <span className="font-medium text-fg">Run {run.index}</span>
                  <span className="mono-data text-fg-muted">
                    node {shortId(run.nodeId)}
                    <span className="block text-micro text-fg-subtle">
                      {formatClock(run.startedAt)} →{" "}
                      {run.stoppedAt ? formatClock(run.stoppedAt) : "running"}
                    </span>
                  </span>
                  <span className="min-w-0">
                    <span className={error ? "text-danger" : "text-fg"}>
                      {run.stopReason
                        ? stopKindLabels[run.stopReason.kind]
                        : "Not stopped"}
                    </span>
                    {error && (
                      <span className="mono-data block truncate text-danger">
                        {error}
                      </span>
                    )}
                    <span className="block text-micro text-fg-subtle">
                      {run.events.length} events
                      {run.missingEvents > 0 &&
                        ` · ${run.missingEvents} missing`}
                    </span>
                  </span>
                </li>
              );
            })}
          </ol>
        </TabsContent>
      </Tabs>
    </div>
  );
}
