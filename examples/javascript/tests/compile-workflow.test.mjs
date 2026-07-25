import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import manifest from "../.waymark/actions.mjs";
import workflow from "../.waymark/workflow.mjs";

test("example compiles and bundles executable JavaScript actions", async () => {
  assert.equal(workflow.workflowName, "ExampleMathWorkflow");
  assert.equal(workflow.inputName, "input");
  assert.equal(
    workflow.hash,
    createHash("sha256").update(workflow.bytes).digest("hex"),
  );
  assert.deepEqual(
    workflow.actions.map((entry) => entry.actionName),
    ["computeFactorial", "computeFibonacci", "summarizeMath"],
  );

  const factorial = manifest.get(
    "src/waymark/actions.ts",
    "computeFactorial",
  );
  const fibonacci = manifest.get(
    "src/waymark/actions.ts",
    "computeFibonacci",
  );
  const summarize = manifest.get("src/waymark/actions.ts", "summarizeMath");

  assert.equal(await factorial?.implementation(5), 120);
  assert.equal(await fibonacci?.implementation(5), 5);
  assert.deepEqual(await summarize?.implementation(5, 120, 5), {
    factorial: 120,
    fibonacci: 5,
    number: 5,
    summary: "5! is 120; Fibonacci(5) is 5.",
  });
});
