import assert from "node:assert/strict";
import test from "node:test";

import {
  decodeWorkflowArguments,
  encodeWorkflowArguments,
  encodeWorkflowValue,
} from "./values.js";

test("workflow values round trip through the shared protobuf domain", () => {
  const encoded = encodeWorkflowArguments({
    profile: { active: true, score: 4.5, tags: ["new", null] },
    count: 3,
  });

  assert.deepEqual(decodeWorkflowArguments(encoded), {
    count: 3,
    profile: { active: true, score: 4.5, tags: ["new", null] },
  });
});

test("workflow values reject unsafe JavaScript values with a path", () => {
  for (const value of [
    undefined,
    1n,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.MAX_SAFE_INTEGER + 1,
    Symbol("value"),
    () => undefined,
    new Date(),
  ]) {
    assert.throws(() => encodeWorkflowValue(value), TypeError);
  }

  const cyclic: { self?: unknown } = {};
  cyclic.self = cyclic;
  assert.throws(() => encodeWorkflowValue(cyclic), /\$\.self contains a cycle/);
});
