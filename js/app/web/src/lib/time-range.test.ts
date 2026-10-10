import assert from "node:assert/strict";
import { test } from "node:test";
import { parseTimeRange } from "./time-range.ts";

test("custom ranges preserve exact bounds and reject missing or reversed dates", () => {
  const from = "2026-10-05T15:30:12.345Z";
  const to = "2026-10-05T16:46:45.986Z";
  assert.deepEqual(parseTimeRange(from, to), {
    from: new Date(from),
    to: new Date(to),
  });
  assert.equal(parseTimeRange(from, null), null);
  assert.equal(parseTimeRange("invalid", to), null);
  assert.equal(parseTimeRange(from, "invalid"), null);
  assert.equal(parseTimeRange(from, from), null);
  assert.equal(parseTimeRange(to, from), null);
});
