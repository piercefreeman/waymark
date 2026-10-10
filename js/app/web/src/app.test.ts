import assert from "node:assert/strict";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { act, createElement } from "react";
import { createRoot } from "react-dom/client";
import { Window } from "happy-dom";
import { createServer } from "vite";
import type { Event, Instance, Observation } from "./domain/api.ts";

test("workflow search navigates safely and held lists keep refreshing workflow data", async (context) => {
  const foundId = "b4fba47f-b09e-43cf-94ea-82d660df2f80";
  const missingId = "00000000-0000-4000-8000-000000000001";
  const delayedId = "11111111-1111-1111-1111-111111111111";
  const secondId = "33333333-3333-3333-3333-333333333333";
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
    workflow_name: "CheckoutWorkflow",
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
  const second: Instance = {
    ...instance,
    vm_id: secondId,
    workflow_name: null,
  };
  const events: Event[] = [];
  function addEvent(observation: Observation) {
    events.push({
      node_id: instance.last_event.node_id,
      node_sequence: events.length + 1,
      at: new Date(
        Date.parse(instance.last_event.at) + events.length * 1000,
      ).toISOString(),
      kind: observation.kind,
      payload: {
        vm_id: foundId,
        workflow_name: "CheckoutWorkflow",
        run_sequence: events.length,
        observation,
      },
    });
    instance.last_event = {
      ...instance.last_event,
      node_sequence: events.length,
    };
  }
  let listItems: Instance[] = [];
  let holdList = false;
  let failInstance = false;
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
      if (input.endsWith(secondId)) return Response.json(second);
      if (input.toLowerCase().endsWith(foundId))
        return failInstance
          ? new Response("unavailable", { status: 503 })
          : Response.json(instance);
      if (input.includes(foundId) && input.includes("/timeline"))
        return Response.json({ items: events, next: null });
      return Response.json({ items: [], next: null });
    },
  );
  const server = await createServer({
    root: fileURLToPath(new URL("../", import.meta.url)),
    server: { middlewareMode: true, hmr: false, watch: null },
    logLevel: "silent",
  });
  const container = document.createElement("div");
  document.body.append(container);
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
  assert.match(container.textContent!, /CheckoutWorkflow/);

  await act(async () => navigate(`/workflows?q=${delayedId}&paused=1`));
  await act(async () => navigate("/workflows?q=partial&paused=1"));
  await act(async () =>
    finishDelayed(Response.json({ ...instance, vm_id: delayedId })),
  );
  assert.equal(window.location.pathname, "/workflows");
  assert.equal(new URLSearchParams(window.location.search).get("q"), "partial");

  context.mock.timers.enable({ apis: ["setTimeout"] });
  Object.assign(window, { setTimeout, clearTimeout });
  instance.outcome = null;
  addEvent({ kind: "vm_started" });
  addEvent({
    kind: "effect_emitted",
    effect_number: 0,
    effect: {
      kind: "action_call",
      promise_state_id: 1,
      action_name: "charge_payment",
      module_name: null,
    },
  });
  listItems = [instance, second];
  await act(async () => navigate("/workflows?q=checkout"));
  assert.equal(container.querySelectorAll("a[data-row]").length, 1);
  assert.equal(
    container.querySelector('a[data-row] [title="CheckoutWorkflow"]')
      ?.textContent,
    "CheckoutWorkflow",
  );
  await act(async () => navigate("/workflows"));
  assert.match(
    container.querySelectorAll("a[data-row]")[1].textContent!,
    /Name not recorded/,
  );
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
  const rowIds = () =>
    Array.from(
      container.querySelectorAll<HTMLAnchorElement>("a[data-row]"),
      (row) => new URL(row.href).searchParams.get("vm"),
    );
  const listReads = () =>
    requests.filter((path) =>
      path.startsWith("/api/observability-state/instances?"),
    ).length;
  const rangeLabel = () =>
    container.textContent!.match(
      /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} – .*? UTC/,
    )![0];
  const initialRange = rangeLabel();

  // Complete a stale refresh after selecting its row: it must not remove the preview.
  holdList = true;
  await act(async () => context.mock.timers.tick(5000));
  const beforePreview = listReads();
  await openPreview();
  assert.match(
    container.querySelector("aside")!.textContent!,
    /CheckoutWorkflow/,
  );
  assert.equal(pendingList!.signal.aborted, true);
  assert.equal(liveButton().getAttribute("aria-pressed"), "false");
  assert.equal(liveButton().title, "Close preview and resume the list");
  assert.match(liveButton().textContent!, /List paused/);
  assert.equal(
    new URLSearchParams(window.location.search).has("paused"),
    false,
  );
  await act(async () =>
    pendingList.resolve(Response.json({ items: [], next: null })),
  );
  instance.outcome = { kind: "complete", at: "2026-10-05T16:00:03Z" };
  addEvent({
    kind: "promise_settled",
    promise_state_id: 1,
    settlement: { kind: "resolved" },
  });
  addEvent({
    kind: "effect_emitted",
    effect_number: 1,
    effect: { kind: "complete" },
  });
  second.outcome = { kind: "unhandled_exception", at: "2026-10-05T16:00:03Z" };
  listItems = [];
  await act(async () => {
    context.mock.timers.tick(5000);
  });
  assert.equal(listReads(), beforePreview);
  assert.deepEqual(rowIds(), [foundId, secondId]);
  assert.equal(rangeLabel(), initialRange);
  assert.ok(container.querySelector("aside"));
  assert.match(
    container.querySelector("aside")!.textContent!,
    /1 settled · 0 open/,
  );
  assert.match(
    container.querySelector("a[data-row]")!.textContent!,
    /Completed/,
  );
  assert.match(
    container.querySelectorAll("a[data-row]")[1].textContent!,
    /Unhandled exception/,
  );

  // A failed row refresh retains the row and preview, and retries next time.
  failInstance = true;
  await act(async () => context.mock.timers.tick(5000));
  assert.deepEqual(rowIds(), [foundId, secondId]);
  assert.match(container.textContent!, /Some data is missing or out of date/);
  assert.match(
    container.querySelector("aside")!.textContent!,
    /1 settled · 0 open/,
  );
  failInstance = false;
  await act(async () => context.mock.timers.tick(5000));
  assert.doesNotMatch(
    container.textContent!,
    /Some data is missing or out of date/,
  );

  holdList = false;
  await closePreview();
  assert.equal(liveButton().getAttribute("aria-pressed"), "true");
  assert.equal(listReads(), beforePreview + 1);
  assert.equal(container.querySelector("aside"), null);

  // Closing a preview preserves held membership, but data still refreshes.
  listItems = [instance];
  for (const search of [
    "?w=24h&paused=1",
    "?from=2026-10-05T15:00:00Z&to=2026-10-05T17:00:00Z",
  ]) {
    await act(async () => navigate(`/workflows${search}`));
    const before = requests.length;
    const beforeList = listReads();
    await openPreview();
    await closePreview();
    await act(async () => context.mock.timers.tick(10_000));
    assert.ok(requests.length > before);
    assert.equal(listReads(), beforeList);
    assert.deepEqual(
      [...new URLSearchParams(window.location.search)],
      [...new URLSearchParams(search)],
    );
  }

  // A shared preview opens directly even after its workflow leaves the page.
  listItems = [second];
  await act(async () => navigate(`/workflows?vm=${foundId}`));
  assert.deepEqual(rowIds(), [secondId]);
  assert.match(
    container.querySelector("aside")!.textContent!,
    /charge_payment/,
  );
  const beforeShared = listReads();
  await act(async () => context.mock.timers.tick(5000));
  assert.equal(listReads(), beforeShared);
  assert.deepEqual(rowIds(), [secondId]);

  // A nonexistent preview must not hold the list without an open panel.
  await act(async () => navigate(`/workflows?vm=${missingId}`));
  assert.equal(container.querySelector("aside"), null);
  assert.equal(liveButton().getAttribute("aria-pressed"), "true");
  const beforeMissing = listReads();
  await act(async () => context.mock.timers.tick(5000));
  assert.ok(listReads() > beforeMissing);

  // Promise inspection stays separate from the page's Events/Runs tabs.
  await act(async () => navigate(`/workflows/${foundId}?tab=runs`));
  const promiseRow = container.querySelector<HTMLAnchorElement>(
    '[aria-label="Promise timeline"] a[data-row]',
  )!;
  promiseRow.focus();
  await act(async () => promiseRow.click());
  assert.match(
    container.querySelector("aside")!.textContent!,
    /charge_payment/,
  );
  assert.match(
    container.querySelector('[role="tab"][aria-selected="true"]')!.textContent!,
    /Runs/,
  );
  assert.equal(new URLSearchParams(window.location.search).get("promise"), "1");
  const closePromise = container.querySelector<HTMLButtonElement>(
    'aside button[aria-label="Close"]',
  )!;
  closePromise.focus();
  await act(async () => context.mock.timers.tick(5000));
  assert.equal(
    document.activeElement,
    closePromise,
    "refreshes must not steal focus",
  );
  await act(async () =>
    window.document.dispatchEvent(
      new window.KeyboardEvent("keydown", { key: "Escape", bubbles: true }),
    ),
  );
  assert.equal(container.querySelector("aside"), null);
  assert.equal(document.activeElement, promiseRow);
  assert.equal(new URLSearchParams(window.location.search).get("tab"), "runs");
  assert.equal(
    new URLSearchParams(window.location.search).has("promise"),
    false,
  );

  await act(async () => navigate(`/workflows/${foundId}?promise=1`));
  assert.ok(
    container.querySelector("aside"),
    "shared promise links open the panel",
  );
  await act(async () =>
    container
      .querySelector<HTMLButtonElement>('aside button[aria-label="Close"]')!
      .click(),
  );
  assert.equal(container.querySelector("aside"), null);
  await act(async () => navigate(`/workflows/${foundId}?promise=999`));
  assert.equal(
    container.querySelector("aside"),
    null,
    "missing promises have no panel",
  );
});
