import type { Event, Histogram, NodeSample, StopReason } from "../domain/api";
import {
  deriveInstance,
  observationKind,
  type InstanceSummary,
} from "../domain/derive";

/**
 * Sample data for the design preview. Instances are authored as event
 * streams and everything else is derived from them, so the preview can only
 * show what the observability API can report. Never mixed with live data:
 * every consumer receives `source: "sample"`.
 */

export const SAMPLE_SOURCE = "sample" as const;

/** Frozen at module load so a static preview doesn't drift between renders. */
export const now = new Date(Math.floor(Date.now() / 1000) * 1000);

export const SAMPLE_INTERVAL_MS = 15_000;
export const WINDOW_MS = 15 * 60_000;
export const timeWindow = {
  from: new Date(now.getTime() - WINDOW_MS),
  to: now,
};

// Offsets are milliseconds relative to `now`; these helpers return negative
// values, so `minutes(6) + seconds(2)` is 6m02s ago and `minutes(6) + 40` is
// 40 ms after that.
const seconds = (value: number) => -value * 1000;
const minutes = (value: number) => seconds(value * 60);
const hours = (value: number) => minutes(value * 60);

// One id per node boot. `alpha_previous` is the boot that `alpha` replaced.
export const nodes = {
  alpha_previous: {
    id: "3b8d1f0c-22e1-4d5b-9a63-b8b0f0a2b2c1",
    bootedAt: new Date(now.getTime() + hours(9)),
    endedAt: new Date(now.getTime() + hours(3) + minutes(2)),
    poolSize: 8,
    maxInFlight: 80,
  },
  alpha: {
    id: "7f3a9c21-5d1e-4c6a-8f0b-1e2d3c4b5a69",
    bootedAt: new Date(now.getTime() + hours(3)),
    endedAt: null,
    poolSize: 8,
    maxInFlight: 80,
  },
  beta: {
    id: "a1c4e7b2-9f3d-4a8c-b6e5-2d1f0c9b8a77",
    bootedAt: new Date(now.getTime() + hours(26)),
    endedAt: null,
    poolSize: 8,
    maxInFlight: 80,
  },
  gamma: {
    id: "c9d2b5e8-1a4f-4e7b-9c3d-5f6a7b8c9d10",
    bootedAt: new Date(now.getTime() + hours(26)),
    endedAt: null,
    poolSize: 4,
    maxInFlight: 40,
  },
} as const;

type NodeKey = keyof typeof nodes;

type Step =
  | { t: number; start: NodeKey }
  | { t: number; call: string; module?: string; id: number }
  | { t: number; sleep: number; id: number; skip?: boolean }
  | { t: number; resolve: number }
  | { t: number; reject: number; type: string }
  | { t: number; snapshot: number; dropped?: boolean }
  | { t: number; stop: StopReason }
  | { t: number; complete: true }
  | { t: number; unhandled: string };

interface Scenario {
  vmId: string;
  steps: Step[];
}

const suspend: StopReason = { kind: "no_ready_frames_or_waiting_promises" };

const scenarios: Scenario[] = [
  {
    // Active: the payment call is in flight on a resident VM.
    vmId: "019a7e21-6ad0-7000-8000-a1b2c3d4e5f6",
    steps: [
      { t: seconds(4.2), start: "alpha" },
      {
        t: seconds(4.19),
        call: "fetch_order",
        module: "commerce.orders",
        id: 1,
      },
      { t: seconds(4.01), resolve: 1 },
      {
        t: seconds(4.0),
        call: "validate_inventory",
        module: "commerce.orders",
        id: 2,
      },
      { t: seconds(3.68), resolve: 2 },
      {
        t: seconds(3.65),
        call: "charge_payment",
        module: "commerce.payments",
        id: 3,
      },
    ],
  },
  {
    // Active with an inferred retry after a rejection that was caught.
    vmId: "019a7e21-72aa-7000-8000-c0ffee00beef",
    steps: [
      { t: seconds(52), start: "beta" },
      {
        t: seconds(51.9),
        call: "render_template",
        module: "notifications.email",
        id: 1,
      },
      { t: seconds(51.4), resolve: 1 },
      {
        t: seconds(51.3),
        call: "send_email",
        module: "notifications.email",
        id: 2,
      },
      { t: seconds(50.0), reject: 2, type: "SMTPTimeout" },
      {
        t: seconds(48),
        call: "send_email",
        module: "notifications.email",
        id: 3,
      },
    ],
  },
  {
    // Unhandled exception after a rejected reconciliation.
    vmId: "019a7e20-5678-7000-8000-d1e2f3a4b5c6",
    steps: [
      { t: minutes(6) + seconds(2.2), start: "gamma" },
      {
        t: minutes(6) + seconds(2.19),
        call: "load_invoice",
        module: "billing.invoices",
        id: 1,
      },
      { t: minutes(6) + seconds(1.83), resolve: 1 },
      {
        t: minutes(6) + seconds(1.8),
        call: "reconcile_payment",
        module: "billing.invoices",
        id: 2,
      },
      { t: minutes(6) + seconds(0.02), reject: 2, type: "PaymentMismatch" },
      { t: minutes(6), unhandled: "PaymentMismatch" },
      { t: minutes(6) + 40, stop: suspend },
    ],
  },
  {
    // Run error: the driver failed while handling an effect. No outcome yet.
    vmId: "019a7e21-7ee1-7000-8000-b1c2d3e4f5a6",
    steps: [
      { t: minutes(3) + seconds(9), start: "alpha" },
      {
        t: minutes(3) + seconds(8.9),
        call: "download_document",
        module: "documents.pipeline",
        id: 1,
      },
      { t: minutes(3) + seconds(8.1), resolve: 1 },
      {
        t: minutes(3) + seconds(8.05),
        call: "extract_text",
        module: "documents.pipeline",
        id: 2,
      },
      {
        t: minutes(3),
        stop: {
          kind: "effect_handling",
          error: "worker reservation timed out after 30s (pool 8/8 busy)",
        },
      },
    ],
  },
  {
    // Suspended on a long sleep; snapshot persisted before the driver stopped.
    vmId: "019a7e1e-bbbb-7000-8000-f1a2b3c4d5e6",
    steps: [
      { t: minutes(9) + seconds(1), start: "alpha" },
      {
        t: minutes(9) + seconds(0.98),
        call: "collect_updates",
        module: "notifications.digest",
        id: 1,
      },
      { t: minutes(9) + seconds(0.38), resolve: 1 },
      { t: minutes(9) + seconds(0.36), sleep: 3600, id: 2 },
      { t: minutes(9) + seconds(0.3), snapshot: 14_212 },
      { t: minutes(9), stop: suspend },
    ],
  },
  {
    // Suspended waiting on an external action; typical durable shape.
    vmId: "019a7e19-0a0a-7000-8000-0b1c2d3e4f50",
    steps: [
      { t: minutes(35), start: "beta" },
      {
        t: minutes(35) + 30,
        call: "await_approval",
        module: "procurement.requests",
        id: 1,
      },
      { t: minutes(35) + 60, snapshot: 6_180 },
      { t: minutes(35) + 90, stop: suspend },
    ],
  },
  {
    // Revived on a new node boot after the old one went away; two driver runs.
    vmId: "019a7e10-2222-7000-8000-a9b8c7d6e5f4",
    steps: [
      { t: hours(3) + minutes(12) + seconds(1.0), start: "alpha_previous" },
      {
        t: hours(3) + minutes(12) + seconds(0.98),
        call: "download_document",
        module: "documents.pipeline",
        id: 1,
      },
      { t: hours(3) + minutes(12) + seconds(0.2), resolve: 1 },
      {
        t: hours(3) + minutes(12) + seconds(0.18),
        call: "parse_pages",
        module: "documents.pipeline",
        id: 2,
      },
      {
        t: hours(3) + minutes(12) + seconds(0.1),
        snapshot: 22_040,
        dropped: true,
      },
      { t: hours(3) + minutes(12), stop: suspend },
      { t: hours(2) + minutes(58) + seconds(3), start: "alpha" },
      { t: hours(2) + minutes(58) + seconds(2.99), resolve: 2 },
      {
        t: hours(2) + minutes(58) + seconds(2.97),
        call: "index_pages",
        module: "documents.search",
        id: 3,
      },
      { t: hours(2) + minutes(58) + seconds(0.57), resolve: 3 },
      { t: hours(2) + minutes(58) + seconds(0.56), complete: true },
      { t: hours(2) + minutes(58) + seconds(0.52), stop: suspend },
    ],
  },
  {
    // Cancelled driver run.
    vmId: "019a7e1c-eeee-7000-8000-9a8b7c6d5e4f",
    steps: [
      { t: minutes(20) + seconds(3), start: "gamma" },
      {
        t: minutes(20) + seconds(2.9),
        call: "generate_report",
        module: "analytics.reports",
        id: 1,
      },
      { t: minutes(20), stop: { kind: "cancelled" } },
    ],
  },
  {
    // Active long-running rebuild.
    vmId: "019a7e1f-aaaa-7000-8000-e1f2a3b4c5d6",
    steps: [
      { t: seconds(24.6), start: "beta" },
      {
        t: seconds(24.59),
        call: "fetch_catalog",
        module: "catalog.sync",
        id: 1,
      },
      { t: seconds(21.5), resolve: 1 },
      {
        t: seconds(21.4),
        call: "rebuild_index",
        module: "catalog.search",
        id: 2,
      },
    ],
  },
  {
    // Unknown: no stop observed, but the node has gone quiet.
    vmId: "019a7e1d-3333-7000-8000-1a2b3c4d5e6f",
    steps: [
      { t: minutes(8) + seconds(4), start: "gamma" },
      {
        t: minutes(8) + seconds(3.9),
        call: "sync_catalog",
        module: "catalog.sync",
        id: 1,
      },
    ],
  },
  {
    // Completed: a fan-out of six page fetches then a merge.
    vmId: "019a7e21-4c4c-7000-8000-6e5d4c3b2a19",
    steps: [
      { t: seconds(41), start: "alpha" },
      ...[1, 2, 3, 4, 5, 6].map((index) => ({
        t: seconds(40.99) + index,
        call: "fetch_page",
        module: "crawler.pages",
        id: index,
      })),
      { t: seconds(40.2), resolve: 3 },
      { t: seconds(40.1), resolve: 1 },
      { t: seconds(39.9), resolve: 5 },
      { t: seconds(39.6), resolve: 2 },
      { t: seconds(39.4), resolve: 6 },
      { t: seconds(38.7), resolve: 4 },
      {
        t: seconds(38.65),
        call: "merge_results",
        module: "crawler.pages",
        id: 7,
      },
      { t: seconds(38.1), resolve: 7 },
      { t: seconds(38.09), complete: true },
      { t: seconds(38.05), stop: suspend },
    ],
  },
  {
    // Completed: enrichment, two quick actions.
    vmId: "019a7e20-1234-7000-8000-c1d2e3f4a5b6",
    steps: [
      { t: minutes(2) + seconds(5), start: "alpha" },
      {
        t: minutes(2) + seconds(4.99),
        call: "lookup_company",
        module: "customers.enrichment",
        id: 1,
      },
      { t: minutes(2) + seconds(4.48), resolve: 1 },
      {
        t: minutes(2) + seconds(4.46),
        call: "save_profile",
        module: "customers.enrichment",
        id: 2,
      },
      { t: minutes(2) + seconds(4.15), resolve: 2 },
      { t: minutes(2) + seconds(4.14), complete: true },
      { t: minutes(2) + seconds(4.1), stop: suspend },
    ],
  },
  {
    // Completed after two rejected deliveries; the retries are inferred.
    vmId: "019a7e1e-cccc-7000-8000-a2b3c4d5e6f7",
    steps: [
      { t: minutes(11) + seconds(5), start: "beta" },
      {
        t: minutes(11) + seconds(4.99),
        call: "sign_payload",
        module: "integrations.webhooks",
        id: 1,
      },
      { t: minutes(11) + seconds(4.9), resolve: 1 },
      {
        t: minutes(11) + seconds(4.88),
        call: "deliver_webhook",
        module: "integrations.webhooks",
        id: 2,
      },
      { t: minutes(11) + seconds(2.9), reject: 2, type: "ConnectionTimeout" },
      {
        t: minutes(11) + seconds(2.85),
        call: "deliver_webhook",
        module: "integrations.webhooks",
        id: 3,
      },
      { t: minutes(11) + seconds(0.85), reject: 3, type: "ConnectionTimeout" },
      {
        t: minutes(11) + seconds(0.8),
        call: "deliver_webhook",
        module: "integrations.webhooks",
        id: 4,
      },
      { t: minutes(11) + seconds(0.3), resolve: 4 },
      { t: minutes(11) + seconds(0.29), complete: true },
      { t: minutes(11) + seconds(0.25), stop: suspend },
    ],
  },
  {
    // Completed with a short resolved sleep.
    vmId: "019a7e1d-dddd-7000-8000-b2c3d4e5f6a7",
    steps: [
      { t: minutes(14) + seconds(12.3), start: "beta" },
      {
        t: minutes(14) + seconds(12.29),
        call: "query_records",
        module: "analytics.exports",
        id: 1,
      },
      { t: minutes(14) + seconds(4.1), resolve: 1 },
      { t: minutes(14) + seconds(4.05), sleep: 5, id: 2, skip: true },
      { t: minutes(14) + 950, resolve: 2 },
      {
        t: minutes(14) + 1000,
        call: "upload_export",
        module: "analytics.exports",
        id: 3,
      },
      { t: minutes(14) + 5050, resolve: 3 },
      { t: minutes(14) + 5060, complete: true },
      { t: minutes(14) + 5100, stop: suspend },
    ],
  },
  {
    // Unhandled after a caught validation error.
    vmId: "019a7e1c-4444-7000-8000-fedcba987654",
    steps: [
      { t: minutes(14) + seconds(1), start: "gamma" },
      {
        t: minutes(14) + seconds(0.98),
        call: "validate_schema",
        module: "ingest.validation",
        id: 1,
      },
      { t: minutes(14) + seconds(0.4), reject: 1, type: "SchemaError" },
      {
        t: minutes(14) + seconds(0.38),
        call: "quarantine_record",
        module: "ingest.validation",
        id: 2,
      },
      { t: minutes(14) + seconds(0.1), resolve: 2 },
      { t: minutes(14), unhandled: "ValueError" },
      { t: minutes(14) + 30, stop: suspend },
    ],
  },
  {
    // Completed: simple three-step job.
    vmId: "019a7e1b-5555-7000-8000-0123456789ab",
    steps: [
      { t: minutes(13) + seconds(2), start: "alpha" },
      {
        t: minutes(13) + seconds(1.99),
        call: "resize_image",
        module: "media.images",
        id: 1,
      },
      { t: minutes(13) + seconds(1.2), resolve: 1 },
      {
        t: minutes(13) + seconds(1.18),
        call: "store_object",
        module: "media.storage",
        id: 2,
      },
      { t: minutes(13) + seconds(0.5), resolve: 2 },
      {
        t: minutes(13) + seconds(0.48),
        call: "invalidate_cache",
        module: "media.cdn",
        id: 3,
      },
      { t: minutes(13) + seconds(0.1), resolve: 3 },
      { t: minutes(13) + seconds(0.09), complete: true },
      { t: minutes(13), stop: suspend },
    ],
  },
];

function stamp(offsetMs: number): string {
  return new Date(now.getTime() + offsetMs).toISOString();
}

function buildEvents(scenario: Scenario): Event[] {
  const events: Event[] = [];
  let nodeId: string = nodes.alpha.id;
  let runSequence = 0;
  let effectNumber = 0;
  const push = (t: number, observation: Event["payload"]["observation"]) => {
    events.push({
      node_id: nodeId,
      node_sequence: 0,
      at: stamp(t),
      kind: observationKind(observation),
      payload: { vm_id: scenario.vmId, run_sequence: runSequence, observation },
    });
    runSequence += 1;
  };
  for (const step of scenario.steps) {
    if ("start" in step) {
      nodeId = nodes[step.start].id;
      runSequence = 0;
      effectNumber = 0;
      push(step.t, { kind: "vm_started" });
    } else if ("call" in step) {
      push(step.t, {
        kind: "effect_emitted",
        effect_number: effectNumber++,
        effect: {
          kind: "action_call",
          promise_state_id: step.id,
          action_name: step.call,
          module_name: step.module ?? null,
        },
      });
    } else if ("sleep" in step) {
      push(step.t, {
        kind: "effect_emitted",
        effect_number: effectNumber++,
        effect: {
          kind: "sleep",
          promise_state_id: step.id,
          duration: { secs: step.sleep, nanos: 0 },
          skip_allowed: step.skip ?? false,
        },
      });
    } else if ("resolve" in step) {
      push(step.t, {
        kind: "promise_settled",
        promise_state_id: step.resolve,
        settlement: { kind: "resolved" },
      });
    } else if ("reject" in step) {
      push(step.t, {
        kind: "promise_settled",
        promise_state_id: step.reject,
        settlement: { kind: "rejected", exception_type: step.type },
      });
    } else if ("snapshot" in step) {
      if (step.dropped) runSequence += 1;
      else
        push(step.t, {
          kind: "snapshot_persisted",
          size_in_bytes: step.snapshot,
        });
    } else if ("stop" in step) {
      push(step.t, { kind: "vm_stopped", reason: step.stop });
    } else if ("complete" in step) {
      push(step.t, {
        kind: "effect_emitted",
        effect_number: effectNumber++,
        effect: { kind: "complete" },
      });
    } else if ("unhandled" in step) {
      push(step.t, {
        kind: "effect_emitted",
        effect_number: effectNumber++,
        effect: { kind: "unhandled_exception", exception_type: step.unhandled },
      });
    }
  }
  return events;
}

/** All sample events, ordered by time with per-node sequence numbers assigned. */
export const events: Event[] = (() => {
  const all = scenarios.flatMap(buildEvents);
  all.sort((a, b) => a.at.localeCompare(b.at));
  const sequenceByNode = new Map<string, number>();
  for (const event of all) {
    const next = (sequenceByNode.get(event.node_id) ?? 0) + 1;
    sequenceByNode.set(event.node_id, next);
    event.node_sequence = next;
  }
  return all;
})();

export function eventsFor(vmId: string): Event[] {
  return events.filter((event) => event.payload.vm_id === vmId);
}

export const instances: InstanceSummary[] = scenarios
  .map((scenario) =>
    deriveInstance(scenario.vmId, eventsFor(scenario.vmId), now),
  )
  .sort((a, b) => b.lastEventAt.getTime() - a.lastEventAt.getTime());

export function instanceById(vmId: string): InstanceSummary | undefined {
  return instances.find((instance) => instance.vmId === vmId);
}

// ---------------------------------------------------------------------------
// Node metrics
// ---------------------------------------------------------------------------

const DEQUEUE_BOUNDS = [0.00001, 0.0001, 0.001, 0.01, 0.1, 1];
const HANDLING_BOUNDS = [
  0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 45, 60, 120, 300,
];

function seeded(seed: number) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1_664_525 + 1_013_904_223) >>> 0;
    return state / 0x1_0000_0000;
  };
}

/** Cumulative histogram from a log-logistic distribution around a median. */
function histogram(
  bounds: number[],
  total: number,
  medianSeconds: number,
  spread: number,
  bimodalSeconds?: number,
): Histogram {
  const cdf = (bound: number, median: number) =>
    1 / (1 + Math.exp(-(Math.log(bound) - Math.log(median)) / spread));
  const counts = bounds.map((bound) => {
    const primary = cdf(bound, medianSeconds);
    const secondary = bimodalSeconds ? cdf(bound, bimodalSeconds) : primary;
    return Math.round(
      total * (bimodalSeconds ? primary * 0.94 + secondary * 0.06 : primary),
    );
  });
  counts[counts.length - 1] = total;
  const p50 = medianSeconds <= bounds[bounds.length - 1] ? medianSeconds : null;
  return { bounds, counts, sum: total * medianSeconds * 1.3, p50 };
}

interface NodeProfile {
  key: NodeKey;
  seed: number;
  inFlight: [number, number];
  queued: [number, number];
  driven: [number, number];
  ratePerSecond: number;
  dequeueMedian: number;
  handlingMedian: number;
  /** Buckets (from window start) with no sample. */
  gaps?: [number, number][];
  /** Stop sampling this many buckets before the window end. */
  staleAfterBuckets?: number;
  dropped: number;
}

const profiles: NodeProfile[] = [
  {
    key: "alpha",
    seed: 11,
    inFlight: [38, 62],
    queued: [0, 9],
    driven: [12, 30],
    ratePerSecond: 32,
    dequeueMedian: 0.0008,
    handlingMedian: 0.19,
    gaps: [[32, 38]],
    dropped: 0,
  },
  {
    key: "beta",
    seed: 23,
    inFlight: [55, 79],
    queued: [6, 24],
    driven: [20, 44],
    ratePerSecond: 41,
    dequeueMedian: 0.012,
    handlingMedian: 0.24,
    dropped: 3,
  },
  {
    key: "gamma",
    seed: 37,
    inFlight: [10, 22],
    queued: [0, 2],
    driven: [4, 9],
    ratePerSecond: 12,
    dequeueMedian: 0.0004,
    handlingMedian: 0.14,
    staleAfterBuckets: 16,
    dropped: 0,
  },
];

function buildSamples(profile: NodeProfile): NodeSample[] {
  const random = seeded(profile.seed);
  const node = nodes[profile.key];
  const bucketCount = WINDOW_MS / SAMPLE_INTERVAL_MS;
  const lastBucket = bucketCount - (profile.staleAfterBuckets ?? 0);
  const samples: NodeSample[] = [];
  let inFlight = (profile.inFlight[0] + profile.inFlight[1]) / 2;
  let queued = (profile.queued[0] + profile.queued[1]) / 2;
  let driven = (profile.driven[0] + profile.driven[1]) / 2;
  let completed = 120_000 + Math.round(random() * 5000);
  let handled = 40_000;
  const walk = (value: number, [min, max]: [number, number], step: number) =>
    Math.max(min, Math.min(max, value + (random() - 0.5) * step));
  for (let bucket = 0; bucket <= bucketCount; bucket += 1) {
    inFlight = walk(inFlight, profile.inFlight, 12);
    queued = walk(queued, profile.queued, 8);
    driven = walk(driven, profile.driven, 6);
    const perSample = profile.ratePerSecond * (SAMPLE_INTERVAL_MS / 1000);
    completed += Math.round(perSample * (0.8 + random() * 0.4));
    handled += Math.round(perSample);
    if (bucket >= lastBucket) break;
    if (profile.gaps?.some(([from, to]) => bucket >= from && bucket < to))
      continue;
    const at = new Date(
      timeWindow.from.getTime() + bucket * SAMPLE_INTERVAL_MS,
    );
    samples.push({
      node_id: node.id,
      sampled_at: at.toISOString(),
      worker_pool_size: node.poolSize,
      max_in_flight_actions: node.maxInFlight,
      in_flight_actions: Math.round(inFlight),
      queued_action_dispatches: Math.round(queued),
      driven_vm_runtimes: Math.round(driven),
      actions_completed_total: completed,
      last_action_completed_at: new Date(at.getTime() - 200).toISOString(),
      action_dequeue_seconds: histogram(
        DEQUEUE_BOUNDS,
        handled,
        profile.dequeueMedian * (0.9 + (queued / profile.queued[1]) * 0.6),
        0.9,
      ),
      action_handling_seconds: histogram(
        HANDLING_BOUNDS,
        handled,
        profile.handlingMedian,
        0.7,
        28,
      ),
      essential_metrics_dropped_total: profile.dropped,
    });
  }
  return samples;
}

export const seriesByNode: Record<string, NodeSample[]> = Object.fromEntries(
  profiles.map((profile) => [nodes[profile.key].id, buildSamples(profile)]),
);

/** What `GET /essential-metrics/nodes/latest` would return, including the retired boot. */
export const latestSamples: NodeSample[] = [
  ...Object.values(seriesByNode).map((samples) => samples[samples.length - 1]),
  {
    node_id: nodes.alpha_previous.id,
    sampled_at: nodes.alpha_previous.endedAt.toISOString(),
    worker_pool_size: 8,
    max_in_flight_actions: 80,
    in_flight_actions: 3,
    queued_action_dispatches: 0,
    driven_vm_runtimes: 2,
    actions_completed_total: 402_118,
    last_action_completed_at: new Date(
      nodes.alpha_previous.endedAt.getTime() - 4000,
    ).toISOString(),
    action_dequeue_seconds: histogram(DEQUEUE_BOUNDS, 402_118, 0.0009, 0.9),
    action_handling_seconds: histogram(HANDLING_BOUNDS, 402_118, 0.2, 0.7, 28),
    essential_metrics_dropped_total: 0,
  },
];

export function nodeBoot(nodeId: string) {
  return Object.values(nodes).find((node) => node.id === nodeId) ?? null;
}
