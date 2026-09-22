import type { ReactNode } from "react";
import type { InstanceSummary } from "@/domain/derive";
import {
  instanceStateOrder,
  instanceStates,
  toneVariable,
  type Tone,
} from "@/domain/status";
import { EmptyState } from "@/components/patterns/empty-state";
import { FilterChips } from "@/components/patterns/filter-chips";
import { Identifier } from "@/components/patterns/identifier";
import { Kbd } from "@/components/patterns/kbd";
import { KeyValueList } from "@/components/patterns/key-value-list";
import { Meter } from "@/components/patterns/meter";
import { MetricStrip, MetricTile } from "@/components/patterns/metric";
import { MiniTimeline } from "@/components/patterns/mini-timeline";
import { PayloadViewer } from "@/components/patterns/payload-viewer";
import { SectionHeader } from "@/components/patterns/section-header";
import {
  InstanceStateInk,
  PromiseStateInk,
  StatusInk,
} from "@/components/patterns/status-ink";
import { Duration, TimeAgo } from "@/components/patterns/time";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

/** Development-only catalog of the patterns. Not part of product navigation. */
export function Gallery({
  instances,
  now,
}: {
  instances: InstanceSummary[];
  now: Date;
}) {
  const tones: Tone[] = ["running", "success", "waiting", "danger", "neutral"];
  return (
    <div className="min-w-0 divide-y divide-line">
      <div className="px-gutter py-4">
        <SectionHeader
          title="Component gallery"
          description="every pattern in every state; tokens in src/styles"
        />
      </div>

      <Block
        title="Tokens"
        note="colors are semantic; charts read the same variables"
      >
        <div className="grid grid-cols-5 gap-3">
          {tones.map((tone) => (
            <div key={tone}>
              <div
                className="h-8 rounded-panel border border-line"
                style={{ background: toneVariable[tone] }}
              />
              <p className="mono-data mt-1 text-micro text-fg-muted">
                --{tone}
              </p>
            </div>
          ))}
        </div>
        <div className="mt-3 grid grid-cols-5 gap-3">
          {[
            "canvas",
            "surface",
            "surface-raised",
            "surface-overlay",
            "surface-selected",
          ].map((token) => (
            <div key={token}>
              <div
                className="h-8 rounded-panel border border-line"
                style={{ background: `var(--${token})` }}
              />
              <p className="mono-data mt-1 text-micro text-fg-muted">
                --{token}
              </p>
            </div>
          ))}
        </div>
        <div className="mt-4 space-y-1">
          <p className="text-title font-semibold">
            Title 18/24 · IBM Plex Sans Condensed
          </p>
          <p className="text-section font-semibold">Section 14/20</p>
          <p className="text-body">
            Body 13/18 — Every number has a unit, a scope, and a freshness.
          </p>
          <p className="text-label text-fg-muted">Label 12/16 muted</p>
          <p className="text-micro text-fg-subtle">Micro 11/14 subtle</p>
          <p className="mono-data text-metric font-medium">
            20.0 metric · IBM Plex Mono, tabular
          </p>
        </div>
      </Block>

      <Block
        title="Status as ink"
        note="glyph and colored text; a fill only marks failure on a row rule"
      >
        <div className="flex flex-wrap gap-4">
          {instanceStateOrder.map((state) => (
            <InstanceStateInk key={state} state={state} />
          ))}
        </div>
        <div className="mt-3 flex flex-wrap gap-4">
          <PromiseStateInk state="open" />
          <PromiseStateInk state="resolved" />
          <PromiseStateInk state="rejected" />
          <StatusInk tone="running" label="Live" pulse />
          <StatusInk tone="waiting" label="stale" size="sm" />
        </div>
        <p className="mt-2 text-micro text-fg-subtle">
          Rules on hover: {instanceStates.run_error.rule}
        </p>
      </Block>

      <Block title="Controls">
        <div className="flex flex-wrap items-center gap-2">
          <Button variant="default">Primary</Button>
          <Button>Outline</Button>
          <Button variant="ghost">Ghost</Button>
          <Button variant="danger">Danger</Button>
          <Button disabled>Disabled</Button>
          <Input
            placeholder="Filter this page…"
            className="w-56"
            aria-label="Example input"
          />
          <span className="flex gap-0.5">
            <Kbd>⌘</Kbd>
            <Kbd>K</Kbd>
          </span>
        </div>
        <div className="mt-3">
          <FilterChips
            label="Example chips"
            options={[
              {
                value: "a",
                label: "Unhandled exception",
                count: 2,
                tone: "danger",
              },
              { value: "b", label: "Active", count: 3, tone: "running" },
              { value: "c", label: "Completed", count: 6, tone: "success" },
            ]}
            value={["a"]}
            onChange={() => undefined}
            scope="page-local"
          />
        </div>
      </Block>

      <Block
        title="Metrics"
        note="never larger than 20px; scope caption always present"
      >
        <MetricStrip className="-mx-gutter">
          <MetricTile
            label="In flight"
            value={126}
            unit="/ 200"
            scope="2 fresh nodes · 1 excluded"
          />
          <MetricTile
            label="Dequeue p95"
            value="84 ms"
            scope="queue wait"
            tone="waiting"
          />
          <MetricTile
            label="Promises"
            value={4}
            unit="settled · 1 open"
            scope="1 rejected, caught"
            tone="danger"
          />
          <MetricTile
            label="Unavailable"
            value="—"
            scope="no sample in window"
          />
        </MetricStrip>
      </Block>

      <Block
        title="Meters"
        note="load is blue; unknown capacity is unavailable, never 0%"
      >
        <div className="max-w-sm space-y-3">
          <Meter value={48} max={80} label="Normal" />
          <Meter value={78} max={80} label="Near capacity" />
          <Meter value={100} max={80} label="Above reported capacity" />
          <Meter value={null} max={null} label="No sample" />
        </div>
      </Block>

      <Block title="Time and identity">
        <div className="flex flex-wrap items-center gap-6 text-label">
          <TimeAgo at={new Date(now.getTime() - 42_000)} now={now} />
          <Duration ms={1_420} kind="call-to-settlement" />
          <Duration ms={null} />
          <Identifier value="019a7e21-6ad0-7000-8000-a1b2c3d4e5f6" copyable />
          <Identifier
            value="019a7e21-6ad0-7000-8000-a1b2c3d4e5f6"
            full
            copyable
          />
        </div>
      </Block>

      <Block title="Mini timelines" note="each instance's life at 160px">
        <div className="grid gap-x-8 gap-y-2 sm:grid-cols-2 xl:grid-cols-3">
          {instances.slice(0, 9).map((instance) => (
            <div key={instance.vmId} className="flex items-center gap-3">
              <MiniTimeline summary={instance} now={now} />
              <InstanceStateInk state={instance.state} size="sm" />
            </div>
          ))}
        </div>
      </Block>

      <Block
        title="Payloads"
        note="recorded, not recorded, and pending must never look alike"
      >
        <div className="grid gap-3 sm:grid-cols-3">
          <PayloadViewer
            label="Recorded"
            payload={{
              kind: "recorded",
              value: { exception_type: "PaymentMismatch" },
            }}
          />
          <PayloadViewer
            label="Not recorded"
            payload={{
              kind: "not-recorded",
              reason:
                "The event API records action names and exception types only.",
            }}
          />
          <PayloadViewer label="Pending" payload={{ kind: "pending" }} />
        </div>
      </Block>

      <Block title="Key–value">
        <div className="grid gap-6 sm:grid-cols-2">
          <KeyValueList
            items={[
              { label: "Called", value: "14:32:01.412", mono: true },
              { label: "Settled", value: "open", note: "1.42 s so far" },
            ]}
          />
          <KeyValueList
            layout="grid"
            items={[
              { label: "Runs", value: 2, mono: true },
              { label: "Snapshots", value: 1, mono: true },
              { label: "Missing", value: 0, mono: true },
            ]}
          />
        </div>
      </Block>

      <Block title="Empty states">
        <div className="grid divide-y divide-line border border-line sm:grid-cols-2 sm:divide-x sm:divide-y-0">
          <EmptyState
            variant="empty"
            title="No instances in this window"
            description="Widen the time window."
          />
          <EmptyState
            variant="filtered"
            title="No instances match"
            description="Clear the chips."
          />
          <EmptyState
            variant="unavailable"
            title="Metrics unavailable"
            description="The metrics endpoint did not respond. Last success 14:31:50."
          />
          <EmptyState
            variant="error"
            title="Request failed"
            description="502 from /api/observability-state/instances"
            action={<Button size="sm">Retry</Button>}
          />
        </div>
      </Block>
    </div>
  );
}

function Block({
  title,
  note,
  children,
}: {
  title: string;
  note?: string;
  children: ReactNode;
}) {
  return (
    <section className="px-gutter py-4">
      <SectionHeader
        as="h3"
        title={title}
        description={note}
        className="mb-3"
      />
      {children}
    </section>
  );
}
