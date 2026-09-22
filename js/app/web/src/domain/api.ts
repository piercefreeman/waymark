/**
 * Wire types for the observability and metrics APIs. These mirror the Rust
 * response structs field for field (snake_case, `kind`-tagged enums) so the
 * UI can only display what the backend actually reports.
 *
 * Sources:
 *   crates/lib/api-observability-state-http/src/common.rs
 *   crates/lib/observability-events-payload/src/vm_driver/payload.rs
 *   crates/lib/api-essential-metrics-http/src/nodes/common.rs
 */

export type Timestamp = string; // RFC 3339, UTC
export type Uuid = string;

// ---------------------------------------------------------------------------
// GET /api/observability-state/instances
// ---------------------------------------------------------------------------

export type OutcomeKind = "complete" | "unhandled_exception";

export type StopKind =
  | "step"
  | "no_ready_frames_or_waiting_promises"
  | "snapshot_serialization"
  | "snapshot_persistence"
  | "effect_handling"
  | "getting_promise_settlements"
  | "cancelled";

export interface Instance {
  vm_id: Uuid;
  latest_run: Run | null;
  outcome: Outcome | null;
  last_event: LastEvent;
}

export interface Run {
  node_id: Uuid;
  started_at: Timestamp;
  stopped: Stopped | null;
}

export interface Stopped {
  at: Timestamp;
  reason: StopKind;
}

export interface Outcome {
  at: Timestamp;
  kind: OutcomeKind;
}

export interface LastEvent {
  at: Timestamp;
  node_id: Uuid;
  node_sequence: number;
  run_sequence: number;
  kind: string;
}

export interface Page<T> {
  items: T[];
  next: string | null;
}

// ---------------------------------------------------------------------------
// GET /api/observability-events/vms/{vm_id}/timeline
// ---------------------------------------------------------------------------

export interface SerdeDuration {
  secs: number;
  nanos: number;
}

export type EffectSummary =
  | { kind: "complete" }
  | { kind: "unhandled_exception"; exception_type: string }
  | {
      kind: "action_call";
      promise_state_id: number;
      action_name: string;
      module_name: string | null;
    }
  | {
      kind: "sleep";
      promise_state_id: number;
      duration: SerdeDuration;
      skip_allowed: boolean;
    };

export type Settlement =
  { kind: "resolved" } | { kind: "rejected"; exception_type: string };

export type StopReason =
  | { kind: "step"; error: string }
  | { kind: "no_ready_frames_or_waiting_promises" }
  | { kind: "snapshot_serialization"; error: string }
  | { kind: "snapshot_persistence"; error: string }
  | { kind: "effect_handling"; error: string }
  | { kind: "getting_promise_settlements"; error: string }
  | { kind: "cancelled" };

export type Observation =
  | { kind: "vm_started" }
  | { kind: "effect_emitted"; effect_number: number; effect: EffectSummary }
  | {
      kind: "promise_settled";
      promise_state_id: number;
      settlement: Settlement;
    }
  | { kind: "snapshot_persisted"; size_in_bytes: number }
  | { kind: "vm_stopped"; reason: StopReason };

export interface EventPayload {
  vm_id: Uuid;
  run_sequence: number;
  observation: Observation;
}

export interface Event {
  node_id: Uuid;
  node_sequence: number;
  at: Timestamp;
  /** Dotted kind tag, e.g. "vm_driver.effect_emitted.action_call". */
  kind: string;
  payload: EventPayload;
}

// ---------------------------------------------------------------------------
// GET /api/essential-metrics/nodes/latest and /nodes/{id}/series
// ---------------------------------------------------------------------------

export interface Histogram {
  /** Upper bounds in seconds, ascending. */
  bounds: number[];
  /** Cumulative counts, one per bound. */
  counts: number[];
  sum: number;
  /** Interpolated median; null when the median lies above the last bound. */
  p50: number | null;
}

export interface NodeSample {
  /** Identity of one node boot. A restart produces a new id. */
  node_id: Uuid;
  sampled_at: Timestamp;
  worker_pool_size: number;
  max_in_flight_actions: number;
  in_flight_actions: number;
  queued_action_dispatches: number;
  driven_vm_runtimes: number;
  /** Monotonic since boot. Rates come from differences, never from the raw value. */
  actions_completed_total: number;
  last_action_completed_at: Timestamp | null;
  action_dequeue_seconds: Histogram;
  action_handling_seconds: Histogram;
  essential_metrics_dropped_total: number;
}

export function durationMs(duration: SerdeDuration): number {
  return duration.secs * 1000 + duration.nanos / 1_000_000;
}

export function stopReasonError(reason: StopReason): string | null {
  switch (reason.kind) {
    case "step":
    case "snapshot_serialization":
    case "snapshot_persistence":
    case "effect_handling":
    case "getting_promise_settlements":
      return reason.error;
    case "no_ready_frames_or_waiting_promises":
    case "cancelled":
      return null;
  }
}

export function isErrorStop(kind: StopKind): boolean {
  switch (kind) {
    case "step":
    case "snapshot_serialization":
    case "snapshot_persistence":
    case "effect_handling":
    case "getting_promise_settlements":
      return true;
    case "no_ready_frames_or_waiting_promises":
    case "cancelled":
      return false;
  }
}

export const stopKindLabels: Record<StopKind, string> = {
  step: "Step failed",
  no_ready_frames_or_waiting_promises: "Suspended",
  snapshot_serialization: "Snapshot serialization failed",
  snapshot_persistence: "Snapshot persistence failed",
  effect_handling: "Effect handling failed",
  getting_promise_settlements: "Settlement fetch failed",
  cancelled: "Cancelled",
};
