import grpc from "@grpc/grpc-js";

import { actionRegistry } from "./action.js";
import { resolveGrpcTarget } from "./bridge.js";
import {
  deserializeWorkflowArguments,
  serializeErrorPayload,
  serializeResultPayload,
} from "./serialization.js";
import {
  fromProtoMessage,
  getGrpcProto,
  getMessagesProto,
  toProtoMessage,
} from "./proto.js";

export type WorkerOptions = {
  workerId?: number;
  target?: string;
  onError?: (error: Error) => void;
  onEnd?: () => void;
};

export type WorkerRuntime = {
  client: unknown;
  stream: grpc.ClientDuplexStream<unknown, unknown>;
  stop: () => void;
};

export function startWorker(options: WorkerOptions = {}): WorkerRuntime {
  const messagesProto = getMessagesProto();
  const grpcProto = getGrpcProto();
  const workerId = Number.isFinite(options.workerId)
    ? Number(options.workerId)
    : Number(process.env.RAPPEL_WORKER_ID || 1);
  const target = options.target || resolveGrpcTarget();

  const client = new grpcProto.WorkerBridgeClient(
    target,
    grpc.credentials.createInsecure()
  );

  const stream = client.attach();
  stream.on("data", (envelope: unknown) => {
    void handleEnvelope(envelope as typeof messagesProto.Envelope.prototype);
  });
  stream.on("error", (err: Error) => {
    if (options.onError) {
      options.onError(err);
      return;
    }
    console.error("worker stream error:", err.message);
  });
  stream.on("end", () => {
    if (options.onEnd) {
      options.onEnd();
      return;
    }
    console.error("worker stream ended");
  });

  sendWorkerHello();

  return {
    client,
    stream,
    stop: () => stream.end(),
  };

  async function handleEnvelope(
    envelope: typeof messagesProto.Envelope.prototype
  ) {
    const kind = envelope.getKind();
    if (kind === messagesProto.MessageKind.MESSAGE_KIND_ACTION_DISPATCH) {
      await handleDispatch(envelope);
      return;
    }

    if (kind === messagesProto.MessageKind.MESSAGE_KIND_HEARTBEAT) {
      await sendAck(envelope);
      return;
    }

    await sendAck(envelope);
  }

  async function handleDispatch(
    envelope: typeof messagesProto.Envelope.prototype
  ) {
    await sendAck(envelope);

    const dispatch = messagesProto.ActionDispatch.deserializeBinary(
      envelope.getPayload_asU8()
    );

    const actionName = dispatch.getActionName();
    const moduleName = dispatch.getModuleName();
    const entry = actionRegistry.get(moduleName, actionName);
    if (!entry || !entry.fn) {
      await sendErrorResult(
        envelope,
        dispatch,
        new Error(`Action not found: ${moduleName}.${actionName}`)
      );
      return;
    }

    const kwargsPayload = await fromProtoMessage(
      "rappel.messages.WorkflowArguments",
      dispatch.getKwargs()
    );
    const kwargs = deserializeWorkflowArguments(kwargsPayload);

    const params = entry.params ?? [];
    const args = params.map((name) => kwargs[name]);

    const startNs = process.hrtime.bigint();
    let resultPayload;
    let success = true;

    try {
      const timeoutSeconds = dispatch.hasTimeoutSeconds()
        ? dispatch.getTimeoutSeconds()
        : 0;
      const resultPromise = Promise.resolve(entry.fn(...args));
      const result =
        timeoutSeconds > 0
          ? await withTimeout(resultPromise, timeoutSeconds * 1000)
          : await resultPromise;
      resultPayload = serializeResultPayload(result);
    } catch (error) {
      success = false;
      resultPayload = serializeErrorPayload(error);
    }

    const endNs = process.hrtime.bigint();
    const payloadMessage = await toProtoMessage(
      "rappel.messages.WorkflowArguments",
      resultPayload
    );

    const response = new messagesProto.ActionResult();
    response.setActionId(dispatch.getActionId());
    response.setSuccess(success);
    response.setWorkerStartNs(startNs.toString());
    response.setWorkerEndNs(endNs.toString());
    response.setPayload(payloadMessage);

    const dispatchToken = dispatch.getDispatchToken();
    if (dispatchToken) {
      response.setDispatchToken(dispatchToken);
    }

    const responseEnvelope = new messagesProto.Envelope();
    responseEnvelope.setDeliveryId(envelope.getDeliveryId());
    responseEnvelope.setPartitionId(envelope.getPartitionId());
    responseEnvelope.setKind(
      messagesProto.MessageKind.MESSAGE_KIND_ACTION_RESULT
    );
    responseEnvelope.setPayload(response.serializeBinary());

    stream.write(responseEnvelope);
  }

  async function sendErrorResult(
    envelope: typeof messagesProto.Envelope.prototype,
    dispatch: typeof messagesProto.ActionDispatch.prototype,
    error: Error
  ) {
    const payloadMessage = await toProtoMessage(
      "rappel.messages.WorkflowArguments",
      serializeErrorPayload(error)
    );

    const response = new messagesProto.ActionResult();
    response.setActionId(dispatch.getActionId());
    response.setSuccess(false);
    response.setWorkerStartNs("0");
    response.setWorkerEndNs("0");
    response.setPayload(payloadMessage);

    const dispatchToken = dispatch.getDispatchToken();
    if (dispatchToken) {
      response.setDispatchToken(dispatchToken);
    }

    const responseEnvelope = new messagesProto.Envelope();
    responseEnvelope.setDeliveryId(envelope.getDeliveryId());
    responseEnvelope.setPartitionId(envelope.getPartitionId());
    responseEnvelope.setKind(
      messagesProto.MessageKind.MESSAGE_KIND_ACTION_RESULT
    );
    responseEnvelope.setPayload(response.serializeBinary());

    stream.write(responseEnvelope);
  }

  async function sendAck(envelope: typeof messagesProto.Envelope.prototype) {
    const ack = new messagesProto.Ack();
    ack.setAckedDeliveryId(envelope.getDeliveryId());

    const ackEnvelope = new messagesProto.Envelope();
    ackEnvelope.setDeliveryId(envelope.getDeliveryId());
    ackEnvelope.setPartitionId(envelope.getPartitionId());
    ackEnvelope.setKind(messagesProto.MessageKind.MESSAGE_KIND_ACK);
    ackEnvelope.setPayload(ack.serializeBinary());

    stream.write(ackEnvelope);
  }

  function sendWorkerHello() {
    const hello = new messagesProto.WorkerHello();
    hello.setWorkerId(workerId);

    const envelope = new messagesProto.Envelope();
    envelope.setDeliveryId(0);
    envelope.setPartitionId(0);
    envelope.setKind(messagesProto.MessageKind.MESSAGE_KIND_WORKER_HELLO);
    envelope.setPayload(hello.serializeBinary());

    stream.write(envelope);
  }
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number) {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      const error = new Error(`action timed out after ${timeoutMs}ms`);
      error.name = "TimeoutError";
      reject(error);
    }, timeoutMs);

    promise
      .then((result) => {
        clearTimeout(timer);
        resolve(result);
      })
      .catch((error) => {
        clearTimeout(timer);
        reject(error);
      });
  });
}
