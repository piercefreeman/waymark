import {
  credentials,
  type ClientDuplexStream,
} from "@grpc/grpc-js";
import { pathToFileURL } from "node:url";

import { ActionManifest } from "./action-manifest.js";
import { executeActionDispatch } from "./action-runtime.js";
import { ActionRuntime } from "./internal/proto/action.js";
import {
  Ack,
  ActionDispatch,
  ActionResult,
  type Envelope,
  MessageKind,
  WorkerBridgeClient,
  WorkerHello,
} from "./internal/proto/messages.js";

const grpcMaxMessageSizeBytes = 25 * 1024 * 1024;

function writeEnvelope(
  stream: ClientDuplexStream<Envelope, Envelope>,
  envelope: Envelope,
): Promise<void> {
  return new Promise((resolve, reject) => {
    stream.write(envelope, (error?: Error | null) => {
      if (error === null || error === undefined) {
        resolve();
      } else {
        reject(error);
      }
    });
  });
}

function ackEnvelope(envelope: Envelope): Envelope {
  return {
    deliveryId: envelope.deliveryId,
    partitionId: envelope.partitionId,
    kind: MessageKind.MESSAGE_KIND_ACK,
    payload: Buffer.from(
      Ack.encode({ ackedDeliveryId: envelope.deliveryId }).finish(),
    ),
  };
}

async function handleEnvelope(
  stream: ClientDuplexStream<Envelope, Envelope>,
  envelope: Envelope,
  manifest: ActionManifest,
): Promise<void> {
  await writeEnvelope(stream, ackEnvelope(envelope));
  if (envelope.kind !== MessageKind.MESSAGE_KIND_ACTION_DISPATCH) {
    return;
  }

  const dispatch = ActionDispatch.decode(envelope.payload);
  const result = await executeActionDispatch(dispatch, manifest);
  await writeEnvelope(stream, {
    deliveryId: envelope.deliveryId,
    partitionId: envelope.partitionId,
    kind: MessageKind.MESSAGE_KIND_ACTION_RESULT,
    payload: Buffer.from(ActionResult.encode(result).finish()),
  });
}

export async function runNodeWorkerConnection(
  stream: ClientDuplexStream<Envelope, Envelope>,
  workerId: bigint,
  manifest: ActionManifest,
  signal?: AbortSignal,
): Promise<void> {
  if (signal?.aborted === true) {
    stream.end();
    return;
  }
  const pending = new Set<Promise<void>>();
  let stopping = false;
  let settle: (() => void) | undefined;
  let rejectConnection: ((error: unknown) => void) | undefined;
  const ended = new Promise<void>((resolve, reject) => {
    settle = resolve;
    rejectConnection = reject;
  });

  const onData = (envelope: Envelope) => {
    if (stopping) {
      return;
    }
    const task = handleEnvelope(stream, envelope, manifest);
    pending.add(task);
    void task
      .catch((error) => rejectConnection?.(error))
      .finally(() => pending.delete(task));
  };
  const onError = (error: unknown) => rejectConnection?.(error);
  const onEnd = () => settle?.();
  const onAbort = () => {
    stopping = true;
    void Promise.allSettled([...pending]).then(() => {
      stream.end();
      settle?.();
    });
  };

  stream.on("data", onData);
  stream.on("error", onError);
  stream.on("end", onEnd);
  signal?.addEventListener("abort", onAbort, { once: true });

  try {
    await writeEnvelope(stream, {
      deliveryId: 0n,
      partitionId: 0,
      kind: MessageKind.MESSAGE_KIND_WORKER_HELLO,
      payload: Buffer.from(
        WorkerHello.encode({
          workerId,
          runtime: ActionRuntime.ACTION_RUNTIME_JAVASCRIPT,
        }).finish(),
      ),
    });
    await ended;
  } finally {
    stopping = true;
    await Promise.allSettled([...pending]);
    stream.off("data", onData);
    stream.off("error", onError);
    stream.off("end", onEnd);
    signal?.removeEventListener("abort", onAbort);
  }
}

function waitForReconnect(
  milliseconds: number,
  signal?: AbortSignal,
): Promise<void> {
  if (signal?.aborted === true) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    const onAbort = () => {
      clearTimeout(timeout);
      resolve();
    };
    const timeout = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

export interface RunNodeWorkerOptions {
  readonly bridge: string;
  readonly manifest: ActionManifest;
  readonly reconnectDelayMilliseconds?: number;
  readonly signal?: AbortSignal;
  readonly workerId: bigint;
}

function signalAborted(signal?: AbortSignal): boolean {
  return signal?.aborted ?? false;
}

export async function runNodeWorker(
  options: RunNodeWorkerOptions,
): Promise<void> {
  while (!signalAborted(options.signal)) {
    const client = new WorkerBridgeClient(
      options.bridge,
      credentials.createInsecure(),
      {
        "grpc.max_send_message_length": grpcMaxMessageSizeBytes,
        "grpc.max_receive_message_length": grpcMaxMessageSizeBytes,
      },
    );
    try {
      await runNodeWorkerConnection(
        client.attach(),
        options.workerId,
        options.manifest,
        options.signal,
      );
    } catch (error) {
      if (signalAborted(options.signal)) {
        return;
      }
      console.error(`Waymark worker connection failed: ${String(error)}`);
    } finally {
      client.close();
    }
    await waitForReconnect(
      options.reconnectDelayMilliseconds ?? 1_000,
      options.signal,
    );
  }
}

export async function loadActionManifest(
  filePath: string,
): Promise<ActionManifest> {
  const imported: unknown = await import(pathToFileURL(filePath).href);
  if (
    imported === null ||
    typeof imported !== "object" ||
    !("default" in imported)
  ) {
    throw new TypeError(`${filePath} does not export a default action manifest`);
  }
  const manifest: unknown = imported.default;
  if (
    manifest === null ||
    typeof manifest !== "object" ||
    !("get" in manifest) ||
    typeof manifest.get !== "function"
  ) {
    throw new TypeError(`${filePath} default export is not an action manifest`);
  }
  return manifest as ActionManifest;
}
