import assert from "node:assert/strict";
import { test } from "node:test";
import type { Event, Observation } from "./api.ts";
import { deriveInstance } from "./derive.ts";

const vmId = "019a7e21-6ad0-7000-8000-a1b2c3d4e5f6";
const nodeA = "7f3a9c21-5d1e-4c6a-8f0b-1e2d3c4b5a69";
const nodeB = "a1c4e7b2-9f3d-4a8c-b6e5-2d1f0c9b8a77";

function stream(
  steps: [offsetMs: number, node: string, observation: Observation][],
): Event[] {
  const base = Date.parse("2026-09-22T14:00:00Z");
  let runSequence = 0;
  return steps.map(([offset, node, observation], index) => {
    if (observation.kind === "vm_started") runSequence = 0;
    const event: Event = {
      node_id: node,
      node_sequence: index + 1,
      at: new Date(base + offset).toISOString(),
      kind: observation.kind,
      payload: { vm_id: vmId, run_sequence: runSequence, observation },
    };
    runSequence += 1;
    return event;
  });
}

const call = (id: number, name: string): Observation => ({
  kind: "effect_emitted",
  effect_number: id,
  effect: {
    kind: "action_call",
    promise_state_id: id,
    action_name: name,
    module_name: "m",
  },
});
const resolved = (id: number): Observation => ({
  kind: "promise_settled",
  promise_state_id: id,
  settlement: { kind: "resolved" },
});
const rejected = (id: number, type: string): Observation => ({
  kind: "promise_settled",
  promise_state_id: id,
  settlement: { kind: "rejected", exception_type: type },
});

test("outcome takes precedence over the latest run's stop reason", () => {
  const events = stream([
    [0, nodeA, { kind: "vm_started" }],
    [10, nodeA, call(1, "fetch")],
    [200, nodeA, resolved(1)],
    [
      210,
      nodeA,
      {
        kind: "effect_emitted",
        effect_number: 2,
        effect: { kind: "complete" },
      },
    ],
    [
      220,
      nodeA,
      {
        kind: "vm_stopped",
        reason: { kind: "no_ready_frames_or_waiting_promises" },
      },
    ],
  ]);
  const summary = deriveInstance(
    vmId,
    events,
    new Date(Date.parse("2026-09-22T14:00:01Z")),
  );
  assert.equal(summary.state, "completed");
  assert.equal(summary.counts.resolved, 1);
  assert.equal(summary.firstAction?.name, "fetch");
});

test("an error stop without an outcome is a run error, not a workflow failure", () => {
  const events = stream([
    [0, nodeA, { kind: "vm_started" }],
    [10, nodeA, call(1, "extract")],
    [
      30_000,
      nodeA,
      {
        kind: "vm_stopped",
        reason: { kind: "effect_handling", error: "pool busy" },
      },
    ],
  ]);
  const summary = deriveInstance(
    vmId,
    events,
    new Date(Date.parse("2026-09-22T14:01:00Z")),
  );
  assert.equal(summary.state, "run_error");
  assert.equal(summary.runErrorMessage, "pool busy");
  assert.equal(summary.outcome, null);
  assert.equal(summary.counts.open, 1);
});

test("a rejection is counted but never changes state; a repeated call is an inferred retry", () => {
  const events = stream([
    [0, nodeA, { kind: "vm_started" }],
    [10, nodeA, call(1, "deliver")],
    [500, nodeA, rejected(1, "Timeout")],
    [510, nodeA, call(2, "deliver")],
  ]);
  const summary = deriveInstance(
    vmId,
    events,
    new Date(Date.parse("2026-09-22T14:00:01Z")),
  );
  assert.equal(summary.state, "active");
  assert.equal(summary.counts.rejected, 1);
  assert.equal(summary.exceptionType, "Timeout");
  assert.equal(summary.promises[1].possibleRetryOf, 1);
});

test("a fresh run with no stop is active; a stale one with no open promise is unknown", () => {
  const events = stream([
    [0, nodeA, { kind: "vm_started" }],
    [10, nodeA, call(1, "sync")],
    [20, nodeA, resolved(1)],
  ]);
  const fresh = deriveInstance(
    vmId,
    events,
    new Date(Date.parse("2026-09-22T14:00:30Z")),
  );
  const stale = deriveInstance(
    vmId,
    events,
    new Date(Date.parse("2026-09-22T14:05:00Z")),
  );
  assert.equal(fresh.state, "active");
  assert.equal(stale.state, "unknown");
});

test("runs split on vm_started and sequence gaps are reported", () => {
  const events = stream([
    [0, nodeA, { kind: "vm_started" }],
    [10, nodeA, call(1, "download")],
    [20, nodeA, { kind: "snapshot_persisted", size_in_bytes: 100 }],
    [
      30,
      nodeA,
      {
        kind: "vm_stopped",
        reason: { kind: "no_ready_frames_or_waiting_promises" },
      },
    ],
    [60_000, nodeB, { kind: "vm_started" }],
    [60_010, nodeB, resolved(1)],
  ]);
  // Drop the snapshot event to simulate a lost observation.
  const withGap = events.filter(
    (event) => event.payload.observation.kind !== "snapshot_persisted",
  );
  const summary = deriveInstance(
    vmId,
    withGap,
    new Date(Date.parse("2026-09-22T14:01:01Z")),
  );
  assert.equal(summary.runs.length, 2);
  assert.equal(summary.runs[0].nodeId, nodeA);
  assert.equal(summary.runs[1].nodeId, nodeB);
  assert.equal(summary.runs[0].missingEvents, 1);
  assert.equal(summary.promises[0].state, "resolved");
  assert.equal(summary.promises[0].runIndex, 0);
});

test("a quiet VM with an open promise stays active: silence is not death", () => {
  const events = stream([
    [0, nodeA, { kind: "vm_started" }],
    [10, nodeA, call(1, "slow_action")],
  ]);
  const summary = deriveInstance(
    vmId,
    events,
    new Date(Date.parse("2026-09-22T14:10:00Z")),
  );
  assert.equal(summary.state, "active");
});
