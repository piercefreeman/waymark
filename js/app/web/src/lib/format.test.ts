import assert from "node:assert/strict";
import { test } from "node:test";
import {
  formatDuration,
  formatRelative,
  formatSeconds,
  shortId,
} from "./format.ts";

test("formatDuration sizes the unit to the magnitude", () => {
  assert.equal(formatDuration(180), "180 ms");
  assert.equal(formatDuration(1_420), "1.42 s");
  assert.equal(formatDuration(72_000), "1m 12s");
  assert.equal(formatDuration(2 * 3_600_000 + 5 * 60_000), "2h 05m");
  assert.equal(formatDuration(null), "—");
});

test("formatSeconds handles histogram magnitudes", () => {
  assert.equal(formatSeconds(0.000084), "84 µs");
  assert.equal(formatSeconds(0.012), "12 ms");
  assert.equal(formatSeconds(1.3), "1.30 s");
  assert.equal(formatSeconds(null), "—");
});

test("formatRelative is coarse and stable", () => {
  const now = new Date("2026-09-22T14:32:09Z");
  assert.equal(
    formatRelative(new Date("2026-09-22T14:32:05Z"), now),
    "just now",
  );
  assert.equal(
    formatRelative(new Date("2026-09-22T14:31:57Z"), now),
    "12s ago",
  );
  assert.equal(formatRelative(new Date("2026-09-22T14:28:09Z"), now), "4m ago");
  assert.equal(formatRelative(new Date("2026-09-22T11:32:09Z"), now), "3h ago");
  assert.equal(
    formatRelative(new Date("2026-09-22T14:32:12Z"), now),
    "just now",
  );
});

test("shortId keeps the leading and trailing groups", () => {
  assert.equal(
    shortId("019a7e21-6ad0-7000-8000-a1b2c3d4e5f6"),
    "019a7e21…e5f6",
  );
  assert.equal(shortId("short"), "short");
});
