import assert from "node:assert/strict";
import test from "node:test";

import { action } from "./action.js";
import {
  createActionManifest,
  type ActionManifestEntry,
} from "./action-manifest.js";
import { executeActionDispatch } from "./action-runtime.js";
import { ActionRuntime } from "./internal/proto/action.js";
import type { ActionDispatch } from "./internal/proto/messages.js";
import {
  decodeWorkflowArguments,
  encodeWorkflowArguments,
} from "./values.js";

const double = action(async function double(value: number): Promise<number> {
  return value * 2;
});

const manifest = createActionManifest([
  {
    actionName: "double",
    moduleName: "src/actions.ts",
    parameterNames: ["value"],
    implementation: double,
  },
]);

function dispatch(overrides: Partial<ActionDispatch> = {}): ActionDispatch {
  return {
    actionId: "action-1",
    instanceId: "instance-1",
    sequence: 2,
    actionName: "double",
    moduleName: "src/actions.ts",
    kwargs: encodeWorkflowArguments({ value: 4 }),
    timeoutSeconds: undefined,
    maxRetries: undefined,
    attemptNumber: 1,
    dispatchToken: "dispatch-token",
    metadata: Buffer.from([1, 2, 3]),
    runtime: ActionRuntime.ACTION_RUNTIME_JAVASCRIPT,
    ...overrides,
  };
}

test("JavaScript action dispatch preserves protocol metadata and bigint timing", async () => {
  const result = await executeActionDispatch(dispatch(), manifest);

  assert.equal(result.success, true);
  assert.deepEqual(
    decodeWorkflowArguments(result.payload ?? { arguments: [] }),
    { result: 8 },
  );
  assert.equal(result.dispatchToken, "dispatch-token");
  assert.deepEqual(result.metadata, Buffer.from([1, 2, 3]));
  assert.equal(typeof result.workerStartNs, "bigint");
  assert.ok(result.workerEndNs >= result.workerStartNs);
});

test("JavaScript action dispatch serializes failures", async () => {
  for (const invalid of [
    dispatch({ actionName: "missing" }),
    dispatch({ runtime: ActionRuntime.ACTION_RUNTIME_PYTHON }),
    dispatch({ kwargs: encodeWorkflowArguments({ wrong: 4 }) }),
  ]) {
    const result = await executeActionDispatch(invalid, manifest);
    assert.equal(result.success, false);
    assert.equal(result.errorType, "Error");
    assert.ok(result.errorMessage);
    assert.ok(
      "error" in decodeWorkflowArguments(result.payload ?? { arguments: [] }),
    );
  }
});

test("action manifests reject duplicates and untyped functions", () => {
  assert.throws(
    () =>
      createActionManifest([
        {
          actionName: "double",
          moduleName: "src/actions.ts",
          parameterNames: ["value"],
          implementation: double,
        },
        {
          actionName: "double",
          moduleName: "src/actions.ts",
          parameterNames: ["value"],
          implementation: double,
        },
      ]),
    /duplicate action manifest entry/,
  );
  assert.throws(
    () =>
      createActionManifest([
        {
          actionName: "plain",
          moduleName: "src/actions.ts",
          parameterNames: [],
          implementation:
            (async () => undefined) as unknown as ActionManifestEntry["implementation"],
        },
      ]),
    /was not declared with action/,
  );
});
