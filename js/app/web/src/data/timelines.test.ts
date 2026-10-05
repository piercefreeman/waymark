import assert from "node:assert/strict";
import { setImmediate } from "node:timers/promises";
import { test } from "node:test";
import type { Event, Instance } from "../domain/api.ts";
import { fetchTimelines, type CachedTimeline } from "./timelines.ts";

function instance(vmId: string, sequence = 1): Instance {
  return {
    vm_id: vmId,
    latest_run: null,
    outcome: null,
    last_event: {
      node_id: "node",
      node_sequence: sequence,
      run_sequence: 0,
      kind: "vm_started",
      at: "2026-10-05T12:00:00Z",
    },
  };
}

function response(instance: Instance): Response {
  const event: Event = {
    ...instance.last_event,
    payload: {
      vm_id: instance.vm_id,
      run_sequence: instance.last_event.run_sequence,
      observation: { kind: "vm_started" },
    },
  };
  return Response.json({ items: [event], next: null });
}

test("timelines publish together, reuse unchanged rows, and retry failed reads", async (context) => {
  const requests: ((response: Response) => void)[] = [];
  context.mock.method(
    globalThis,
    "fetch",
    () => new Promise<Response>((resolve) => requests.push(resolve)),
  );
  const signal = new AbortController().signal;
  const first = instance("first", 2);
  const second = instance("second");
  const previous = new Map<string, CachedTimeline>([
    ["first", { key: "node:1", events: [], complete: true }],
  ]);
  let published = false;
  const read = fetchTimelines([first, second], previous, signal).then(
    (result) => {
      published = true;
      return result;
    },
  );
  assert.equal(requests.length, 2);
  requests[0](response(first));
  await setImmediate();
  assert.equal(published, false, "one row cannot publish a partial snapshot");
  assert.equal(previous.get("first")?.key, "node:1");
  assert.equal(previous.has("second"), false);
  requests[1](response(second));
  const result = await read;
  assert.equal(result.complete, true);
  assert.equal(result.timelines.get("first")?.events.length, 1);
  assert.equal(result.timelines.get("second")?.events.length, 1);

  await fetchTimelines([first, second], result.timelines, signal);
  assert.equal(requests.length, 2, "unchanged rows reuse cached timelines");

  const changed = instance("first", 3);
  const failedRead = fetchTimelines([changed], result.timelines, signal);
  requests[2](new Response("unavailable", { status: 503 }));
  const failed = await failedRead;
  assert.equal(failed.complete, false);
  assert.equal(failed.timelines.get("first"), result.timelines.get("first"));
  const retry = fetchTimelines([changed], failed.timelines, signal);
  assert.equal(requests.length, 4, "failed rows retry without a new event");
  requests[3](response(changed));
  assert.equal((await retry).complete, true);
});

test("timeline concurrency is bounded and cancellation never publishes a snapshot", async (context) => {
  const requests: ((response: Response) => void)[] = [];
  const controller = new AbortController();
  context.mock.method(
    globalThis,
    "fetch",
    (_url: string, options: RequestInit) => {
      assert.equal(options.signal, controller.signal);
      return new Promise<Response>((resolve) => requests.push(resolve));
    },
  );
  const instances = Array.from({ length: 7 }, (_, index) =>
    instance(String(index)),
  );
  const read = fetchTimelines(instances, new Map(), controller.signal);
  assert.equal(requests.length, 6);
  requests[0](response(instances[0]));
  await setImmediate();
  assert.equal(requests.length, 7);
  controller.abort();
  const rejected = assert.rejects(read, { name: "AbortError" });
  for (let index = 1; index < requests.length; index += 1)
    requests[index](response(instances[index]));
  await rejected;
});
