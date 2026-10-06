import assert from "node:assert/strict";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { act, createElement } from "react";
import { createRoot } from "react-dom/client";
import { Window } from "happy-dom";
import { createServer } from "vite";
import type { Instance } from "./domain/api.ts";

test("workflow search navigates safely and previews suspend list refreshes", async (context) => {
  const foundId = "b4fba47f-b09e-43cf-94ea-82d660df2f80";
  const missingId = "00000000-0000-4000-8000-000000000001";
  const delayedId = "11111111-1111-1111-1111-111111111111";
  const window = new Window({
    url: `http://localhost/workflows?q=${missingId}&state=active&paused=1`,
  });
  Object.assign(globalThis, {
    window,
    document: window.document,
    Event: window.Event,
    HTMLElement: window.HTMLElement,
    HTMLInputElement: window.HTMLInputElement,
    HTMLTextAreaElement: window.HTMLTextAreaElement,
    MutationObserver: window.MutationObserver,
    getComputedStyle: window.getComputedStyle.bind(window),
    requestAnimationFrame: window.requestAnimationFrame.bind(window),
    cancelAnimationFrame: window.cancelAnimationFrame.bind(window),
    IS_REACT_ACT_ENVIRONMENT: true,
  });
  const instance: Instance = {
    vm_id: foundId,
    latest_run: null,
    outcome: { at: "2026-10-05T16:00:00Z", kind: "complete" },
    last_event: {
      at: "2026-10-05T16:00:00Z",
      node_id: "22222222-2222-2222-2222-222222222222",
      node_sequence: 1,
      run_sequence: 0,
      kind: "vm_driver.effect_emitted.complete",
    },
  };
  const requests: string[] = [];
  let listItems: Instance[] = [];
  let holdList = false;
  let pendingList: {
    signal: AbortSignal;
    resolve: (response: Response) => void;
  };
  let finishDelayed: (response: Response) => void;
  context.mock.method(
    globalThis,
    "fetch",
    async (input: string, options: RequestInit) => {
      requests.push(input);
      if (input.startsWith("/api/observability-state/instances?")) {
        if (holdList)
          return new Promise<Response>((resolve) => {
            pendingList = { signal: options.signal!, resolve };
          });
        return Response.json({ items: listItems, next: null });
      }
      if (input.endsWith(delayedId))
        return new Promise<Response>((resolve) => (finishDelayed = resolve));
      if (input.endsWith(missingId))
        return new Response("Not found", { status: 404 });
      if (input.toLowerCase().endsWith(foundId)) return Response.json(instance);
      return Response.json({ items: [], next: null });
    },
  );
  const server = await createServer({
    root: fileURLToPath(new URL("../", import.meta.url)),
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
  const { App } = (await server.ssrLoadModule(
    "/src/app.tsx",
  )) as typeof import("./app.tsx");
  const { navigate } = (await server.ssrLoadModule(
    "/src/lib/router.ts",
  )) as typeof import("./lib/router.ts");

  await act(async () => root.render(createElement(App)));
  assert.equal(window.location.pathname, "/workflows");
  assert.match(container.textContent ?? "", /No workflows match/);
  assert.equal(container.querySelectorAll('input[type="search"]').length, 1);

  // A completed workflow outside the current window still opens despite the active filter.
  await act(async () =>
    navigate(`/workflows?q=${foundId.toUpperCase()}&state=active&paused=1`),
  );
  assert.equal(window.location.pathname, `/workflows/${foundId}`);
  assert.equal(requests.filter((path) => path.includes("/timeline")).length, 1);

  await act(async () => navigate(`/workflows?q=${delayedId}&paused=1`));
  await act(async () => navigate("/workflows?q=partial&paused=1"));
  await act(async () =>
    finishDelayed(Response.json({ ...instance, vm_id: delayedId })),
  );
  assert.equal(window.location.pathname, "/workflows");
  assert.equal(new URLSearchParams(window.location.search).get("q"), "partial");

  context.mock.timers.enable({ apis: ["setTimeout"] });
  Object.assign(window, { setTimeout, clearTimeout });
  listItems = [instance];
  await act(async () => navigate("/workflows"));
  const liveButton = () =>
    container.querySelector<HTMLButtonElement>("header button[aria-pressed]")!;
  const openPreview = () =>
    act(async () =>
      container.querySelector<HTMLAnchorElement>("ol a")!.click(),
    );
  const closePreview = () =>
    act(async () =>
      container
        .querySelector<HTMLButtonElement>('aside button[aria-label="Close"]')!
        .click(),
    );
  assert.equal(liveButton().getAttribute("aria-pressed"), "true");

  // Complete a stale refresh after selecting its row: it must not remove the preview.
  holdList = true;
  await act(async () => context.mock.timers.tick(5000));
  const beforePreview = requests.length;
  await openPreview();
  assert.equal(pendingList!.signal.aborted, true);
  assert.equal(liveButton().getAttribute("aria-pressed"), "false");
  assert.equal(liveButton().title, "Close preview and resume live updates");
  assert.equal(
    new URLSearchParams(window.location.search).has("paused"),
    false,
  );
  await act(async () =>
    pendingList.resolve(Response.json({ items: [], next: null })),
  );
  await act(async () => {
    window.document.dispatchEvent(new window.Event("visibilitychange"));
    context.mock.timers.tick(15_000);
  });
  assert.equal(requests.length, beforePreview);
  assert.ok(container.querySelector("aside"));

  holdList = false;
  listItems = [];
  await closePreview();
  assert.equal(liveButton().getAttribute("aria-pressed"), "true");
  assert.equal(requests.length, beforePreview + 1);
  assert.equal(container.querySelector("aside"), null);

  // Closing a preview must preserve a deliberate pause or a historical range.
  listItems = [instance];
  for (const search of [
    "?w=24h&paused=1",
    "?from=2026-10-05T15:00:00Z&to=2026-10-05T17:00:00Z",
  ]) {
    await act(async () => navigate(`/workflows${search}`));
    const before = requests.length;
    await openPreview();
    await closePreview();
    await act(async () => context.mock.timers.tick(10_000));
    assert.equal(requests.length, before);
    assert.deepEqual(
      [...new URLSearchParams(window.location.search)],
      [...new URLSearchParams(search)],
    );
  }
});
