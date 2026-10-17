import assert from "node:assert/strict";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { act, createElement } from "react";
import { createRoot } from "react-dom/client";
import { Window } from "happy-dom";
import { createServer } from "vite";

test("changing the time filter resets pagination and keeps the search and states", async (context) => {
  const window = new Window({
    url: "http://localhost/workflows?w=24h&q=node-1&state=completed&paused=1&after=older&from=2026-10-05T12%3A00%3A00Z&to=2026-10-05T16%3A46%3A45Z&vm=selected",
  });
  Object.assign(globalThis, {
    window,
    document: window.document,
    Event: window.Event,
    IS_REACT_ACT_ENVIRONMENT: true,
  });
  const server = await createServer({
    root: fileURLToPath(new URL("../../../", import.meta.url)),
    server: { middlewareMode: true, hmr: false, watch: null },
    logLevel: "silent",
  });
  const container = document.createElement("div");
  const root = createRoot(container);
  context.after(async () => {
    await act(async () => root.unmount());
    await server.close();
    await window.happyDOM.close();
  });
  const { useTimeWindow } = (await server.ssrLoadModule(
    "/src/components/layout/app-shell.tsx",
  )) as typeof import("./app-shell.tsx");
  let setWindow: ReturnType<typeof useTimeWindow>[1];
  function Probe() {
    const [timeWindow, update] = useTimeWindow();
    setWindow = update;
    return createElement("span", null, timeWindow.id);
  }
  await act(async () => root.render(createElement(Probe)));
  assert.equal(container.textContent, "24h");
  await act(async () => setWindow("6h"));
  assert.equal(container.textContent, "6h");
  const search = new URLSearchParams(window.location.search);
  assert.equal(search.get("w"), "6h");
  assert.equal(search.get("q"), "node-1");
  assert.equal(search.get("state"), "completed");
  assert.equal(search.get("paused"), "1");
  for (const key of ["after", "from", "to", "vm"])
    assert.equal(search.has(key), false);
  await act(async () => setWindow("15m"));
  assert.equal(container.textContent, "15m");
  assert.equal(new URLSearchParams(window.location.search).has("w"), false);
});
