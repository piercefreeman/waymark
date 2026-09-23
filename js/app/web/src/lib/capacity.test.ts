import assert from "node:assert/strict";
import test from "node:test";
import { capacityPercent } from "./capacity.ts";

test("capacity distinguishes idle, unknown, and over-capacity samples", () => {
  assert.equal(capacityPercent(48, 80), 60);
  assert.equal(capacityPercent(0, 80), 0);
  assert.equal(capacityPercent(100, 80), 125);
  for (const [used, capacity] of [
    [null, 80],
    [48, null],
    [0, 0],
    [-1, 80],
    [NaN, 80],
    [48, Infinity],
  ] as const) {
    assert.equal(capacityPercent(used, capacity), null);
  }
});
