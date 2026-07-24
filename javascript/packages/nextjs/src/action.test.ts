import assert from "node:assert/strict";
import test from "node:test";

import { action, isAction } from "./action.js";

test("action preserves the typed implementation", async () => {
  const double = action(async function double(value: number): Promise<number> {
    return value * 2;
  });

  assert.equal(await double(4), 8);
  assert.equal(isAction(double), true);
  assert.equal(isAction(async () => undefined), false);
});
