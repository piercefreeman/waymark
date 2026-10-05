import assert from "node:assert/strict";
import { test } from "node:test";
import { act, createElement, StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { Window } from "happy-dom";
import { useLive, type LiveState } from "./live.ts";

test("refreshes retain data, pause stays paused, and superseded polls stop", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] });
  const window = new Window();
  Object.assign(window, { setTimeout, clearTimeout });
  Object.assign(globalThis, {
    window,
    document: window.document,
    IS_REACT_ACT_ENVIRONMENT: true,
  });
  const container = document.createElement("div");
  const root = createRoot(container);
  context.after(async () => {
    await act(async () => root.unmount());
    await window.happyDOM.close();
  });

  const requests: {
    signal: AbortSignal;
    resolve: (value: string) => void;
    reject: (error: Error) => void;
  }[] = [];
  const fetcher = (signal: AbortSignal) =>
    new Promise<string>((resolve, reject) => {
      requests.push({ signal, resolve, reject });
    });
  let options = { key: "first", enabled: true, intervalMs: 5000 };
  let current: LiveState<string>;
  function Probe() {
    current = useLive(fetcher, options);
    return createElement("span", null, current.data ?? "loading");
  }
  const render = () =>
    act(async () =>
      root.render(createElement(StrictMode, null, createElement(Probe))),
    );
  await render();
  // StrictMode cancels the first mount's effect before mounting it again.
  const discarded = requests.shift()!;
  assert.equal(discarded.signal.aborted, true);
  await act(async () => discarded.resolve("discarded mount"));
  await act(async () => requests[0].resolve("first snapshot"));
  assert.equal(container.textContent, "first snapshot");

  await act(async () => context.mock.timers.tick(5000));
  assert.equal(requests.length, 2);
  assert.equal(container.textContent, "first snapshot");

  // Pause while a poll is in flight. Its late completion must not re-arm it.
  options = { ...options, enabled: false };
  await render();
  assert.equal(requests[1].signal.aborted, true);
  assert.equal(requests.length, 2, "pausing must not start another request");
  await act(async () => requests[1].resolve("late response"));
  await act(async () => context.mock.timers.tick(10_000));
  assert.equal(requests.length, 2, "an aborted poll must not schedule again");
  assert.equal(container.textContent, "first snapshot");

  await act(async () => current.refresh());
  assert.equal(requests.length, 3, "manual refresh still works while paused");
  await act(async () => requests[2].reject(new Error("offline")));
  assert.equal(container.textContent, "first snapshot");
  assert.equal(current!.error?.message, "offline");

  options = { ...options, key: "second", enabled: true };
  await render();
  assert.equal(container.textContent, "loading");
  assert.equal(current!.fetchedAt, null);
  assert.equal(current!.error, null);
  await act(async () => requests[3].resolve("second snapshot"));
  assert.equal(container.textContent, "second snapshot");

  await act(async () => context.mock.timers.tick(5000));
  await act(async () =>
    window.document.dispatchEvent(new window.Event("visibilitychange")),
  );
  assert.equal(
    requests.length,
    5,
    "returning to a tab must not overlap its pending poll",
  );
  options = { ...options, key: "third", enabled: false };
  await render();
  assert.equal(requests[4].signal.aborted, true);
  assert.equal(container.textContent, "loading");
  await act(async () => requests[4].resolve("superseded query"));
  assert.equal(container.textContent, "loading");
  await act(async () => requests[5].resolve("third snapshot"));
  assert.equal(container.textContent, "third snapshot");
  await act(async () => context.mock.timers.tick(10_000));
  assert.equal(requests.length, 6, "a new query loads once while paused");
  options = { ...options, enabled: true };
  await render();
  await act(async () => root.unmount());
  await act(async () => requests[6].resolve("response after unmount"));
  await act(async () => context.mock.timers.tick(10_000));
  assert.equal(requests.length, 7, "unmounted polls must not schedule again");
});
