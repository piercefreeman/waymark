import assert from "node:assert/strict";
import type { ClientDuplexStream } from "@grpc/grpc-js";
import { EventEmitter } from "node:events";
import { setImmediate } from "node:timers/promises";
import test from "node:test";

import { action } from "./action.js";
import { createActionManifest } from "./action-manifest.js";
import { ActionRuntime } from "./internal/proto/action.js";
import {
  Ack,
  ActionDispatch,
  ActionResult,
  type Envelope,
  MessageKind,
  WorkerHello,
} from "./internal/proto/messages.js";
import { encodeWorkflowArguments } from "./values.js";
import { runNodeWorkerConnection } from "./worker.js";

class FakeWorkerStream extends EventEmitter {
  readonly writes: Envelope[] = [];
  ended = false;

  write(
    envelope: Envelope,
    callback: (error?: Error | null) => void,
  ): boolean {
    this.writes.push(envelope);
    callback();
    return true;
  }

  end(): void {
    this.ended = true;
  }
}

function dispatchEnvelope(deliveryId: bigint, name: string): Envelope {
  return {
    deliveryId,
    partitionId: 4,
    kind: MessageKind.MESSAGE_KIND_ACTION_DISPATCH,
    payload: Buffer.from(
      ActionDispatch.encode({
        actionId: `action-${deliveryId}`,
        instanceId: "instance-1",
        sequence: Number(deliveryId),
        actionName: "slowGreeting",
        moduleName: "src/actions.ts",
        kwargs: encodeWorkflowArguments({ name }),
        timeoutSeconds: undefined,
        maxRetries: undefined,
        attemptNumber: 1,
        dispatchToken: `token-${deliveryId}`,
        metadata: Buffer.from([Number(deliveryId)]),
        runtime: ActionRuntime.ACTION_RUNTIME_JAVASCRIPT,
      }).finish(),
    ),
  };
}

test("Node worker handshakes, ACKs receipt, and runs dispatches concurrently", async () => {
  const releases: Array<() => void> = [];
  const started: string[] = [];
  const slowGreeting = action(async function slowGreeting(
    name: string,
  ): Promise<string> {
    started.push(name);
    await new Promise<void>((resolve) => releases.push(resolve));
    return `Hello ${name}`;
  });
  const manifest = createActionManifest([
    {
      actionName: "slowGreeting",
      moduleName: "src/actions.ts",
      parameterNames: ["name"],
      implementation: slowGreeting,
    },
  ]);
  const fake = new FakeWorkerStream();
  const stream = fake as unknown as ClientDuplexStream<Envelope, Envelope>;
  const connection = runNodeWorkerConnection(stream, 42n, manifest);

  await setImmediate();
  fake.emit("data", dispatchEnvelope(10n, "Ada"));
  fake.emit("data", dispatchEnvelope(11n, "Grace"));
  await setImmediate();

  const helloEnvelope = fake.writes[0];
  assert.equal(
    helloEnvelope?.kind,
    MessageKind.MESSAGE_KIND_WORKER_HELLO,
  );
  const hello = WorkerHello.decode(helloEnvelope?.payload ?? Buffer.alloc(0));
  assert.equal(hello.workerId, 42n);
  assert.equal(hello.runtime, ActionRuntime.ACTION_RUNTIME_JAVASCRIPT);
  assert.deepEqual(started, ["Ada", "Grace"]);
  assert.deepEqual(
    fake.writes.slice(1).map((envelope) => envelope.kind),
    [MessageKind.MESSAGE_KIND_ACK, MessageKind.MESSAGE_KIND_ACK],
  );
  assert.deepEqual(
    fake.writes.slice(1).map((envelope) =>
      Ack.decode(envelope.payload).ackedDeliveryId
    ),
    [10n, 11n],
  );

  for (const release of releases) {
    release();
  }
  await setImmediate();
  const results = fake.writes.filter(
    (envelope) => envelope.kind === MessageKind.MESSAGE_KIND_ACTION_RESULT,
  );
  assert.equal(results.length, 2);
  assert.deepEqual(
    results.map((envelope) => envelope.deliveryId).sort(),
    [10n, 11n],
  );
  assert.ok(
    results
      .map((envelope) => ActionResult.decode(envelope.payload))
      .every(
        (result) =>
          result.success &&
          result.dispatchToken?.startsWith("token-") === true &&
          result.metadata.length === 1,
      ),
  );

  fake.emit("end");
  await connection;
});

test("Node worker cleanly ends its stream when cancelled", async () => {
  const manifest = createActionManifest([]);
  const fake = new FakeWorkerStream();
  const controller = new AbortController();
  const connection = runNodeWorkerConnection(
    fake as unknown as ClientDuplexStream<Envelope, Envelope>,
    7n,
    manifest,
    controller.signal,
  );

  await setImmediate();
  controller.abort();
  await connection;

  assert.equal(fake.ended, true);
});
