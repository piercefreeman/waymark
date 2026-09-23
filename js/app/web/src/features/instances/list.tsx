import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ArrowUpRight, ChevronLeft, ChevronRight, Search } from "lucide-react";
import { cn } from "@/lib/cn";
import {
  formatClock,
  formatDuration,
  formatRelative,
  shortId,
} from "@/lib/format";
import {
  navigate,
  onLinkClick,
  useLocation,
  useSearchParam,
  withSearch,
} from "@/lib/router";
import { stopKindLabels } from "@/domain/api";
import type { InstanceSummary } from "@/domain/derive";
import {
  instanceStateOrder,
  instanceStates,
  type InstanceState,
} from "@/domain/status";
import { EmptyState } from "@/components/patterns/empty-state";
import { FilterChips } from "@/components/patterns/filter-chips";
import { Kbd } from "@/components/patterns/kbd";
import { MiniTimeline } from "@/components/patterns/mini-timeline";
import { SectionHeader } from "@/components/patterns/section-header";
import {
  SourceNotice,
  type SourceStatus,
} from "@/components/patterns/source-notice";
import { InstanceStateInk } from "@/components/patterns/status-ink";
import { Duration } from "@/components/patterns/time";
import { Input } from "@/components/ui/input";
import { PeekPanel } from "@/components/layout/peek-panel";
import { PAGE_SIZE, SCAN_PAGE_CAP, isExactId } from "@/data/instances";
import { InstancePeek } from "./peek";

export interface PageInfo {
  next: string | null;
  after: string | null;
  pinnedTo: Date | null;
  scanned: number;
  capped: boolean;
  direct: boolean;
  loadingRows: number;
}

/**
 * The instance ledger. One row answers one question: what is this instance
 * doing right now? State, identity, a plain sentence, the shape of its
 * life so far, and how long it has taken. Everything else is in the peek.
 *
 * Pages follow the list endpoint's cursor. Paging past the head freezes the
 * window's `to` bound so the pages stay put; the bar offers a way back to
 * live. Search and state filters are applied while walking the cursor, so
 * they cover the window, not just the loaded page.
 */
export function InstanceList({
  instances,
  now,
  windowLabel,
  source,
  page,
  fetchedTo,
}: {
  instances: InstanceSummary[];
  now: Date;
  windowLabel: string;
  source: SourceStatus;
  page: PageInfo;
  fetchedTo: Date | null;
}) {
  const { pathname, search } = useLocation();
  const [stateParam] = useSearchParam("state");
  const [query, setQuery] = useSearchParam("q");
  const [selectedId, setSelectedId] = useSearchParam("vm");
  const rows = useRef<HTMLAnchorElement[]>([]);
  const [draft, setDraft] = useState(query ?? "");
  useEffect(() => setDraft(query ?? ""), [query]);
  useEffect(() => {
    if (draft === (query ?? "")) return;
    const timer = window.setTimeout(
      () => setQuery(draft.trim() || null, { replace: true }),
      isExactId(draft) ? 0 : 350,
    );
    return () => window.clearTimeout(timer);
  }, [draft, query, setQuery]);

  const stateFilter = useMemo(
    () =>
      (stateParam ?? "")
        .split(",")
        .filter((value): value is InstanceState => value in instanceStates),
    [stateParam],
  );
  const counts = useMemo(() => {
    const result = Object.fromEntries(
      instanceStateOrder.map((state) => [state, 0]),
    ) as Record<InstanceState, number>;
    for (const instance of instances) result[instance.state] += 1;
    return result;
  }, [instances]);

  const visible = instances;

  const selected =
    visible.find((instance) => instance.vmId === selectedId) ?? null;
  const closePeek = useCallback(() => setSelectedId(null), [setSelectedId]);

  useEffect(() => {
    function onKey(event: KeyboardEvent) {
      const target = event.target as HTMLElement | null;
      if (
        target instanceof HTMLInputElement ||
        target instanceof HTMLTextAreaElement
      )
        return;
      const focused = rows.current.findIndex(
        (row) => row === document.activeElement,
      );
      if (event.key === "j" || event.key === "k") {
        event.preventDefault();
        const next =
          event.key === "j"
            ? Math.min(rows.current.length - 1, focused + 1)
            : Math.max(0, focused - 1);
        rows.current[next]?.focus();
      } else if (event.key === "o" && focused >= 0) {
        event.preventDefault();
        navigate(`/instances/${visible[focused].vmId}`);
      } else if (event.key === "y" && focused >= 0) {
        void navigator.clipboard
          .writeText(visible[focused].vmId)
          .catch(() => undefined);
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [visible]);

  const chipOptions = instanceStateOrder
    .filter((state) => counts[state] > 0 || stateFilter.includes(state))
    .map((state) => ({
      value: state,
      label: instanceStates[state].label,
      count: counts[state],
      tone: instanceStates[state].tone,
    }));

  return (
    <div className="flex min-h-[calc(100svh-var(--spacing-bar))] min-w-0 flex-col">
      <div className="px-gutter pt-5">
        <SectionHeader
          title="Instances"
          count={page.after ? undefined : instances.length}
          description={
            page.pinnedTo
              ? `active in the ${windowLabel} before ${formatClock(page.pinnedTo)}`
              : `active in the last ${windowLabel}`
          }
          actions={
            <div className="relative w-64">
              <Search
                className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-fg-subtle"
                aria-hidden
              />
              <Input
                aria-label="Search the window by instance id, node id, or state"
                placeholder="Search id, node, state…"
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
                className="pl-8"
              />
            </div>
          }
        />
        {chipOptions.length > 0 && (
          <FilterChips
            className="mt-3"
            label="Filter by state"
            options={chipOptions}
            value={stateFilter}
            onChange={(next) =>
              navigate(
                withSearch(pathname, search, {
                  state: next.length ? next.join(",") : null,
                  after: null,
                }),
                { replace: true },
              )
            }
          />
        )}
      </div>

      <SourceNotice source={source} now={now} className="mx-gutter mt-4" />

      <div className="mt-4 overflow-x-auto border-t border-line">
        <div
          role="row"
          className="grid h-8 min-w-[880px] grid-cols-[130px_minmax(220px,1fr)_minmax(260px,1.5fr)_168px_96px] items-center gap-x-5 border-b border-line px-gutter text-micro text-fg-subtle"
        >
          <span role="columnheader">State</span>
          <span role="columnheader">Instance</span>
          <span role="columnheader">Now</span>
          <span role="columnheader">Timeline</span>
          <span role="columnheader" className="text-right">
            Elapsed
          </span>
        </div>
        {visible.length === 0 ? (
          <EmptyState
            variant={
              source.error && instances.length === 0
                ? "unavailable"
                : instances.length === 0
                  ? "empty"
                  : "filtered"
            }
            title={
              source.error && instances.length === 0
                ? "Instances unavailable"
                : query || stateFilter.length
                  ? "No instances match"
                  : `No instances in the last ${windowLabel}`
            }
            description={
              source.error && instances.length === 0
                ? source.error.message
                : query || stateFilter.length
                  ? page.capped
                    ? `Searched ${page.scanned} instances before stopping; page onward to keep searching, or narrow the window.`
                    : `Searched ${page.scanned} instances in the window. Search covers ids, node ids, and states; action names need a timeline read.`
                  : "Instances appear once a driver run reports an event. Try a wider window."
            }
          />
        ) : (
          <ol className="divide-y divide-line">
            {visible.map((instance, index) => {
              const isSelected = instance.vmId === selectedId;
              const href = withSearch(pathname, search, { vm: instance.vmId });
              const problem =
                instance.state === "unhandled_exception" ||
                instance.state === "run_error";
              const sentence = describeNow(instance, now);
              return (
                <li key={instance.vmId}>
                  <a
                    ref={(element) => {
                      if (element) rows.current[index] = element;
                    }}
                    href={href}
                    onClick={onLinkClick}
                    aria-current={isSelected ? "true" : undefined}
                    data-row
                    className={cn(
                      "grid h-10 min-w-[880px] grid-cols-[130px_minmax(220px,1fr)_minmax(260px,1.5fr)_168px_96px] items-center gap-x-5 border-l-2 px-gutter transition-colors duration-fast hover:bg-surface-raised",
                      problem ? "border-danger" : "border-transparent",
                      isSelected && "border-fg bg-surface-selected",
                    )}
                  >
                    <InstanceStateInk state={instance.state} size="sm" />
                    <span className="min-w-0">
                      {/* The API reports no workflow name; the first action the
                          VM called is the closest reported fact, so it leads. */}
                      <span className="mono-data block truncate text-label text-fg">
                        {instance.firstAction
                          ? instance.firstAction.name
                          : shortId(instance.vmId)}
                      </span>
                      <span className="mono-data block truncate text-micro text-fg-subtle">
                        {instance.firstAction
                          ? `${shortId(instance.vmId)}${instance.firstAction.module ? ` · ${instance.firstAction.module}` : ""}`
                          : "no action called yet"}
                      </span>
                    </span>
                    <span className="min-w-0">
                      <span
                        className={cn(
                          "block truncate text-label",
                          problem ? "text-danger" : "text-fg",
                        )}
                      >
                        {sentence.headline}
                      </span>
                      <span className="block truncate text-micro text-fg-subtle">
                        {sentence.detail}
                      </span>
                    </span>
                    <MiniTimeline summary={instance} now={now} />
                    <span className="text-right">
                      <Duration
                        ms={elapsedMs(instance, now)}
                        className="text-label text-fg-muted"
                      />
                    </span>
                  </a>
                </li>
              );
            })}
          </ol>
        )}
      </div>

      <div className="sticky bottom-0 mt-auto flex flex-wrap items-center justify-between gap-3 border-t border-line bg-surface px-gutter py-2 text-micro text-fg-subtle">
        <span>
          {page.direct
            ? "Direct lookup by id"
            : query || stateFilter.length
              ? `${instances.length} match${instances.length === 1 ? "" : "es"} in ${page.scanned} scanned${page.capped ? ` · stopped at ${SCAN_PAGE_CAP} pages` : page.next ? "" : " · whole window"}`
              : `${instances.length} on this page${page.after ? "" : page.next ? ` · newest ${PAGE_SIZE}` : ""}`}
          {page.loadingRows > 0 && ` · loading ${page.loadingRows} timelines`}
        </span>
        <span className="flex items-center gap-2">
          {page.after && (
            <a
              href={withSearch(pathname, search, {
                after: null,
                to: null,
                vm: null,
              })}
              onClick={onLinkClick}
              className="inline-flex h-6 items-center gap-1 rounded-control border border-line-strong px-2 text-fg transition-colors duration-fast hover:bg-surface-raised"
            >
              <ChevronLeft className="size-3" aria-hidden />
              Newest
            </a>
          )}
          {page.next && (
            <a
              href={withSearch(pathname, search, {
                after: page.next,
                to: (page.pinnedTo ?? fetchedTo ?? now).toISOString(),
                vm: null,
              })}
              onClick={onLinkClick}
              className="inline-flex h-6 items-center gap-1 rounded-control border border-line-strong px-2 text-fg transition-colors duration-fast hover:bg-surface-raised"
            >
              {query || stateFilter.length ? "Keep searching" : "Older"}
              <ChevronRight className="size-3" aria-hidden />
            </a>
          )}
          <span className="hidden items-center gap-1.5 lg:flex">
            <Kbd>j</Kbd>
            <Kbd>k</Kbd> move · <Kbd>↵</Kbd> peek · <Kbd>o</Kbd> open ·{" "}
            <Kbd>y</Kbd> copy id
          </span>
        </span>
      </div>

      <PeekPanel
        open={selected !== null}
        onClose={closePeek}
        title={
          selected && (
            <span className="flex min-w-0 items-center gap-2">
              <InstanceStateInk state={selected.state} size="sm" />
              <span className="mono-data truncate text-label text-fg">
                {shortId(selected.vmId)}
              </span>
            </span>
          )
        }
        actions={
          selected && (
            <a
              href={`/instances/${selected.vmId}`}
              onClick={onLinkClick}
              className="inline-flex h-6 items-center gap-1 rounded-control border border-line-strong px-2 text-micro text-fg transition-colors duration-fast hover:bg-surface-raised"
            >
              Open
              <ArrowUpRight className="size-3" aria-hidden />
            </a>
          )
        }
      >
        {selected && <InstancePeek summary={selected} now={now} />}
      </PeekPanel>
    </div>
  );
}

/** First event to the terminal event, or to now while the instance is live. */
export function elapsedMs(summary: InstanceSummary, now: Date): number {
  const end =
    summary.outcome?.at ??
    (summary.state === "active" ? now : summary.lastEventAt);
  return Math.max(0, end.getTime() - summary.firstEventAt.getTime());
}

/**
 * One plain sentence per instance, from reported facts only. The headline
 * says what is happening; the detail says where and how long.
 */
export function describeNow(
  summary: InstanceSummary,
  now: Date,
): { headline: string; detail: string } {
  const ago = formatRelative(summary.stateAt, now);
  const open = summary.promises.filter((promise) => promise.state === "open");
  const latestOpen = open[open.length - 1];
  const node = `node ${shortId(summary.lastNodeId).slice(0, 8)}`;
  const extras = [
    summary.runs.length > 1 ? `${summary.runs.length} driver runs` : null,
    summary.counts.rejected > 0
      ? `${summary.counts.rejected} rejected settlement${summary.counts.rejected === 1 ? "" : "s"}`
      : null,
  ].filter(Boolean);
  const detail = [node, ...extras].join(" · ");

  switch (summary.state) {
    case "active":
      if (latestOpen)
        return {
          headline:
            latestOpen.kind === "sleep"
              ? `Sleeping, wakes ${latestOpen.wakeAt ? formatClock(latestOpen.wakeAt) : "later"}`
              : `Running ${latestOpen.name} for ${formatDuration(now.getTime() - latestOpen.calledAt.getTime())}`,
          detail,
        };
      return {
        headline: `Running · last event ${formatRelative(summary.lastEventAt, now)}`,
        detail,
      };
    case "suspended":
      if (latestOpen?.kind === "sleep")
        return {
          headline: `Sleeping until ${latestOpen.wakeAt ? formatClock(latestOpen.wakeAt) : "wake"}`,
          detail: `suspended ${ago} · ${detail}`,
        };
      if (latestOpen)
        return {
          headline: `Waiting on ${latestOpen.name} since ${formatRelative(latestOpen.calledAt, now)}`,
          detail: `suspended ${ago} · ${detail}`,
        };
      return { headline: `Suspended ${ago}`, detail };
    case "completed":
      return {
        headline: `Completed ${ago}`,
        detail: `${summary.counts.resolved} promise${summary.counts.resolved === 1 ? "" : "s"} resolved · ${detail}`,
      };
    case "unhandled_exception":
      return {
        headline: `Unhandled ${summary.exceptionType ?? "exception"} ${ago}`,
        detail,
      };
    case "run_error": {
      const label = summary.latestRun?.stopReason
        ? stopKindLabels[summary.latestRun.stopReason.kind]
        : "Run error";
      return {
        headline: summary.runErrorMessage
          ? `${label}: ${summary.runErrorMessage}`
          : `${label} ${ago}`,
        detail: summary.runErrorMessage ? `${ago} · ${detail}` : detail,
      };
    }
    case "cancelled":
      return { headline: `Driver run cancelled ${ago}`, detail };
    case "unknown":
      return {
        headline: `No events for ${formatRelative(summary.lastEventAt, now).replace(" ago", "")}`,
        detail: latestOpen
          ? `last called ${latestOpen.name} · ${detail}`
          : detail,
      };
  }
}
