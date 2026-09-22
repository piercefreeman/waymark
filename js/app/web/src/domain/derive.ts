import {
  durationMs,
  isErrorStop,
  type Event,
  type Instance,
  type Observation,
  type OutcomeKind,
  type StopKind,
  type StopReason,
} from "./api.ts";
import type { InstanceState, PromiseState } from "./status.ts";

/**
 * Everything the UI shows about an instance is derived here from its event
 * timeline. Nothing is invented: no display names, no payloads, no future
 * actions, no attempt numbers.
 */

export const DEFAULT_FRESHNESS_MS = 60_000;

export interface DerivedPromise {
  id: number;
  kind: "action" | "sleep";
  name: string;
  module: string | null;
  effectNumber: number;
  /** Index of the driver run that emitted the call. */
  runIndex: number;
  calledAt: Date;
  settledAt: Date | null;
  state: PromiseState;
  exceptionType: string | null;
  sleepMs: number | null;
  skipAllowed: boolean | null;
  /** For sleeps: the earliest the VM can wake. */
  wakeAt: Date | null;
  /** Same action name called again after a rejection. Inferred, not reported. */
  possibleRetryOf: number | null;
}

export interface DerivedRun {
  index: number;
  nodeId: string;
  startedAt: Date;
  stoppedAt: Date | null;
  stopReason: StopReason | null;
  events: Event[];
  /** Positions missing from the run's own sequence. */
  missingEvents: number;
}

export interface DerivedSnapshot {
  at: Date;
  bytes: number;
  runIndex: number;
}

export interface InstanceSummary {
  vmId: string;
  state: InstanceState;
  stateAt: Date;
  firstEventAt: Date;
  lastEventAt: Date;
  lastEvent: Event;
  lastNodeId: string;
  outcome: { kind: OutcomeKind; at: Date } | null;
  runs: DerivedRun[];
  latestRun: DerivedRun | null;
  promises: DerivedPromise[];
  snapshots: DerivedSnapshot[];
  counts: { open: number; resolved: number; rejected: number };
  /** The first action the VM called. Labeled as derived wherever it is shown. */
  firstAction: { name: string; module: string | null } | null;
  /** Exception type from the outcome, the latest error stop, or the latest rejection. */
  exceptionType: string | null;
  runErrorMessage: string | null;
  missingEvents: number;
  events: Event[];
}

function toDate(timestamp: string): Date {
  return new Date(timestamp);
}

export function deriveRuns(events: Event[]): DerivedRun[] {
  const runs: DerivedRun[] = [];
  let current: DerivedRun | null = null;
  for (const event of events) {
    const observation = event.payload.observation;
    if (observation.kind === "vm_started" || current === null) {
      current = {
        index: runs.length,
        nodeId: event.node_id,
        startedAt: toDate(event.at),
        stoppedAt: null,
        stopReason: null,
        events: [],
        missingEvents: 0,
      };
      runs.push(current);
    }
    current.events.push(event);
    if (observation.kind === "vm_stopped") {
      current.stoppedAt = toDate(event.at);
      current.stopReason = observation.reason;
    }
  }
  for (const run of runs) {
    const last = run.events[run.events.length - 1];
    run.missingEvents = Math.max(
      0,
      last.payload.run_sequence + 1 - run.events.length,
    );
  }
  return runs;
}

export function derivePromises(
  events: Event[],
  runs: DerivedRun[],
): DerivedPromise[] {
  const runIndexByStart = new Map<Event, number>();
  for (const run of runs)
    for (const event of run.events) runIndexByStart.set(event, run.index);

  const byId = new Map<number, DerivedPromise>();
  const lastRejectedByName = new Map<string, number>();
  for (const event of events) {
    const observation = event.payload.observation;
    const runIndex = runIndexByStart.get(event) ?? 0;
    if (observation.kind === "effect_emitted") {
      const effect = observation.effect;
      if (effect.kind === "action_call") {
        const retryOf = lastRejectedByName.get(effect.action_name) ?? null;
        byId.set(effect.promise_state_id, {
          id: effect.promise_state_id,
          kind: "action",
          name: effect.action_name,
          module: effect.module_name,
          effectNumber: observation.effect_number,
          runIndex,
          calledAt: toDate(event.at),
          settledAt: null,
          state: "open",
          exceptionType: null,
          sleepMs: null,
          skipAllowed: null,
          wakeAt: null,
          possibleRetryOf: retryOf,
        });
      } else if (effect.kind === "sleep") {
        const ms = durationMs(effect.duration);
        const calledAt = toDate(event.at);
        byId.set(effect.promise_state_id, {
          id: effect.promise_state_id,
          kind: "sleep",
          name: "sleep",
          module: null,
          effectNumber: observation.effect_number,
          runIndex,
          calledAt,
          settledAt: null,
          state: "open",
          exceptionType: null,
          sleepMs: ms,
          skipAllowed: effect.skip_allowed,
          wakeAt: new Date(calledAt.getTime() + ms),
          possibleRetryOf: null,
        });
      }
    } else if (observation.kind === "promise_settled") {
      const promise = byId.get(observation.promise_state_id);
      if (!promise) continue;
      promise.settledAt = toDate(event.at);
      if (observation.settlement.kind === "rejected") {
        promise.state = "rejected";
        promise.exceptionType = observation.settlement.exception_type;
        lastRejectedByName.set(promise.name, promise.id);
      } else {
        promise.state = "resolved";
        lastRejectedByName.delete(promise.name);
      }
    }
  }
  return [...byId.values()].sort(
    (a, b) => a.calledAt.getTime() - b.calledAt.getTime() || a.id - b.id,
  );
}

export function deriveInstanceState(
  outcome: InstanceSummary["outcome"],
  latestRun: DerivedRun | null,
  lastEventAt: Date,
  now: Date,
  freshnessMs = DEFAULT_FRESHNESS_MS,
  openPromises = 0,
): { state: InstanceState; at: Date } {
  if (outcome)
    return {
      state: outcome.kind === "complete" ? "completed" : "unhandled_exception",
      at: outcome.at,
    };
  if (latestRun?.stopReason && latestRun.stoppedAt) {
    const kind = latestRun.stopReason.kind;
    if (kind === "cancelled")
      return { state: "cancelled", at: latestRun.stoppedAt };
    if (isErrorStop(kind))
      return { state: "run_error", at: latestRun.stoppedAt };
    return { state: "suspended", at: latestRun.stoppedAt };
  }
  // A resident VM waiting on a promise (an action in flight, a sleep) emits
  // nothing until the promise settles, so silence is not evidence of death.
  if (openPromises > 0) return { state: "active", at: lastEventAt };
  const fresh = now.getTime() - lastEventAt.getTime() <= freshnessMs;
  return { state: fresh ? "active" : "unknown", at: lastEventAt };
}

export function deriveInstance(
  vmId: string,
  events: Event[],
  now: Date,
  freshnessMs = DEFAULT_FRESHNESS_MS,
): InstanceSummary {
  const sorted = [...events].sort(
    (a, b) => a.at.localeCompare(b.at) || a.node_sequence - b.node_sequence,
  );
  const runs = deriveRuns(sorted);
  const promises = derivePromises(sorted, runs);
  const snapshots: DerivedSnapshot[] = [];
  let outcome: InstanceSummary["outcome"] = null;
  for (const run of runs)
    for (const event of run.events) {
      const observation = event.payload.observation;
      if (observation.kind === "snapshot_persisted")
        snapshots.push({
          at: toDate(event.at),
          bytes: observation.size_in_bytes,
          runIndex: run.index,
        });
      if (observation.kind === "effect_emitted") {
        if (observation.effect.kind === "complete")
          outcome = { kind: "complete", at: toDate(event.at) };
        if (observation.effect.kind === "unhandled_exception")
          outcome = { kind: "unhandled_exception", at: toDate(event.at) };
      }
    }
  const latestRun = runs[runs.length - 1] ?? null;
  const lastEvent = sorted[sorted.length - 1];
  const lastEventAt = toDate(lastEvent.at);
  const counts = { open: 0, resolved: 0, rejected: 0 };
  for (const promise of promises) counts[promise.state] += 1;
  const { state, at } = deriveInstanceState(
    outcome,
    latestRun,
    lastEventAt,
    now,
    freshnessMs,
    counts.open,
  );
  const firstAction = promises.find((promise) => promise.kind === "action");
  const unhandled = sorted
    .map((event) => event.payload.observation)
    .find(
      (
        observation,
      ): observation is Extract<Observation, { kind: "effect_emitted" }> =>
        observation.kind === "effect_emitted" &&
        observation.effect.kind === "unhandled_exception",
    );
  const latestRejected = [...promises]
    .reverse()
    .find((promise) => promise.state === "rejected");
  const runErrorMessage =
    latestRun?.stopReason && "error" in latestRun.stopReason
      ? latestRun.stopReason.error || null
      : null;
  return {
    vmId,
    state,
    stateAt: at,
    firstEventAt: toDate(sorted[0].at),
    lastEventAt,
    lastEvent,
    lastNodeId: lastEvent.node_id,
    outcome,
    runs,
    latestRun,
    promises,
    snapshots,
    counts,
    firstAction: firstAction
      ? { name: firstAction.name, module: firstAction.module }
      : null,
    exceptionType:
      unhandled && unhandled.effect.kind === "unhandled_exception"
        ? unhandled.effect.exception_type
        : (latestRejected?.exceptionType ?? null),
    runErrorMessage,
    missingEvents: runs.reduce((sum, run) => sum + run.missingEvents, 0),
    events: sorted,
  };
}

/**
 * The instance as the list endpoint reports it, combined with whatever
 * events are available for it. The DTO is authoritative for state (it sees
 * history the window may not include); events add promises, runs, and the
 * timeline. With no events at all the summary still stands on the DTO.
 */
export function deriveFromInstance(
  instance: Instance,
  events: Event[],
  now: Date,
  freshnessMs = DEFAULT_FRESHNESS_MS,
): InstanceSummary {
  const lastEventAt = toDate(instance.last_event.at);
  const outcome: InstanceSummary["outcome"] = instance.outcome
    ? { kind: instance.outcome.kind, at: toDate(instance.outcome.at) }
    : null;
  const latestRunDto = instance.latest_run;
  const dtoRun: DerivedRun | null = latestRunDto
    ? {
        index: 0,
        nodeId: latestRunDto.node_id,
        startedAt: toDate(latestRunDto.started_at),
        stoppedAt: latestRunDto.stopped
          ? toDate(latestRunDto.stopped.at)
          : null,
        stopReason: latestRunDto.stopped
          ? stopReasonFromKind(latestRunDto.stopped.reason)
          : null,
        events: [],
        missingEvents: 0,
      }
    : null;
  const { state, at } = deriveInstanceState(
    outcome,
    dtoRun,
    lastEventAt,
    now,
    freshnessMs,
  );
  if (events.length === 0) {
    const placeholder: Event = {
      node_id: instance.last_event.node_id,
      node_sequence: instance.last_event.node_sequence,
      at: instance.last_event.at,
      kind: instance.last_event.kind,
      payload: {
        vm_id: instance.vm_id,
        run_sequence: instance.last_event.run_sequence,
        observation: { kind: "vm_started" },
      },
    };
    return {
      vmId: instance.vm_id,
      state,
      stateAt: at,
      firstEventAt: dtoRun?.startedAt ?? lastEventAt,
      lastEventAt,
      lastEvent: placeholder,
      lastNodeId: instance.last_event.node_id,
      outcome,
      runs: dtoRun ? [dtoRun] : [],
      latestRun: dtoRun,
      promises: [],
      snapshots: [],
      counts: { open: 0, resolved: 0, rejected: 0 },
      firstAction: null,
      exceptionType: null,
      runErrorMessage: null,
      missingEvents: 0,
      events: [],
    };
  }
  const fromEvents = deriveInstance(instance.vm_id, events, now, freshnessMs);
  // Prefer the DTO's view of outcome, stop, and timing; the window may have
  // cut the event history short, but the state endpoint has seen all of it.
  // Open promises still come from events: they keep a quiet VM "active".
  const withPromises = deriveInstanceState(
    outcome,
    dtoRun,
    lastEventAt,
    now,
    freshnessMs,
    fromEvents.counts.open,
  );
  return {
    ...fromEvents,
    state: withPromises.state,
    stateAt: withPromises.at,
    outcome,
    lastEventAt:
      lastEventAt > fromEvents.lastEventAt
        ? lastEventAt
        : fromEvents.lastEventAt,
    lastNodeId: instance.last_event.node_id,
    latestRun: fromEvents.latestRun ?? dtoRun,
    runs: fromEvents.runs.length ? fromEvents.runs : dtoRun ? [dtoRun] : [],
  };
}

/** The state endpoint reports only the stop kind, not its error text. */
function stopReasonFromKind(kind: StopKind): StopReason {
  switch (kind) {
    case "no_ready_frames_or_waiting_promises":
    case "cancelled":
      return { kind };
    case "step":
    case "snapshot_serialization":
    case "snapshot_persistence":
    case "effect_handling":
    case "getting_promise_settlements":
      return { kind, error: "" };
  }
}

/** The list endpoint's view of an instance, reconstructed from its events. */
export function toInstanceDto(summary: InstanceSummary): Instance {
  const run = summary.latestRun;
  return {
    vm_id: summary.vmId,
    latest_run: run
      ? {
          node_id: run.nodeId,
          started_at: run.startedAt.toISOString(),
          stopped:
            run.stoppedAt && run.stopReason
              ? { at: run.stoppedAt.toISOString(), reason: run.stopReason.kind }
              : null,
        }
      : null,
    outcome: summary.outcome
      ? { at: summary.outcome.at.toISOString(), kind: summary.outcome.kind }
      : null,
    last_event: {
      at: summary.lastEvent.at,
      node_id: summary.lastEvent.node_id,
      node_sequence: summary.lastEvent.node_sequence,
      run_sequence: summary.lastEvent.payload.run_sequence,
      kind: observationKind(summary.lastEvent.payload.observation),
    },
  };
}

/** Dotted kind tag as the events API reports it: "vm_driver.effect_emitted.action_call". */
export function observationKind(observation: Observation): string {
  switch (observation.kind) {
    case "vm_started":
      return "vm_driver.vm_started";
    case "effect_emitted":
      return `vm_driver.effect_emitted.${observation.effect.kind}`;
    case "promise_settled":
      return `vm_driver.promise_settled.${observation.settlement.kind}`;
    case "snapshot_persisted":
      return "vm_driver.snapshot_persisted";
    case "vm_stopped":
      return `vm_driver.vm_stopped.${observation.reason.kind}`;
  }
}

/** Short kind without the namespace, for dense columns. */
export function observationShortKind(observation: Observation): string {
  return observationKind(observation).replace(/^vm_driver\./, "");
}

/** Human detail for one event, using only reported fields. */
export function observationDetail(observation: Observation): string {
  switch (observation.kind) {
    case "vm_started":
      return "driver run started";
    case "effect_emitted": {
      const effect = observation.effect;
      switch (effect.kind) {
        case "complete":
          return "workflow completed";
        case "unhandled_exception":
          return effect.exception_type;
        case "action_call":
          return `${effect.action_name}${effect.module_name ? ` · ${effect.module_name}` : ""} → promise ${effect.promise_state_id}`;
        case "sleep":
          return `sleep ${Math.round(durationMs(effect.duration) / 1000)}s${effect.skip_allowed ? " · skippable" : ""} → promise ${effect.promise_state_id}`;
      }
      break;
    }
    case "promise_settled":
      return observation.settlement.kind === "rejected"
        ? `promise ${observation.promise_state_id} rejected · ${observation.settlement.exception_type}`
        : `promise ${observation.promise_state_id} resolved`;
    case "snapshot_persisted":
      return `${observation.size_in_bytes.toLocaleString()} bytes`;
    case "vm_stopped": {
      const reason = observation.reason;
      return "error" in reason
        ? `${reason.kind}: ${reason.error}`
        : reason.kind;
    }
  }
}
