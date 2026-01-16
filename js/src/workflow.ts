import crypto from "node:crypto";

import { callUnary, getWorkflowClient } from "./bridge.js";
import { decodeMessage, toProtoMessage } from "./proto.js";
import {
  buildWorkflowArguments,
  deserializeResultPayload,
} from "./serialization.js";
import { resolveWorkflow, registerWorkflow } from "./registry.js";

export { registerWorkflow };

type StartOptions = {
  priority?: number;
  blocking?: boolean;
  pollIntervalSecs?: number;
};

export async function start(
  workflow,
  args: Record<string, unknown> = {},
  options: StartOptions = {}
) {
  const entry = resolveWorkflow(workflow);
  if (!entry) {
    throw new Error(
      "start received an invalid workflow. Ensure the workflow is registered."
    );
  }

  const initialContext = buildWorkflowArguments(args);
  const registration = {
    workflow_name: entry.shortName,
    ir: entry.ir,
    ir_hash: entry.irHash || hashIr(entry.ir),
    concurrent: Boolean(entry.concurrent),
    initial_context: initialContext,
    priority: options.priority,
  };

  const client = await getWorkflowClient();
  const registerRequest = await toProtoMessage(
    "rappel.messages.RegisterWorkflowRequest",
    { registration }
  );
  const registerResponse = (await callUnary(
    client,
    "registerWorkflow",
    registerRequest
  )) as any;

  const instanceId = registerResponse.getWorkflowInstanceId();
  if (options.blocking === false) {
    return instanceId;
  }

  const payload = await waitForInstance(client, instanceId, options);
  if (!payload) {
    throw new Error(`workflow instance ${instanceId} did not complete`);
  }

  return deserializeResultPayload(payload);
}

export function register(definition, fn) {
  return registerWorkflow(definition, fn);
}

export async function runAction(awaitable, _options: Record<string, unknown> = {}) {
  return await awaitable;
}

async function waitForInstance(client, instanceId, options) {
  const pollInterval =
    typeof options.pollIntervalSecs === "number" ? options.pollIntervalSecs : 1.0;
  const request = await toProtoMessage(
    "rappel.messages.WaitForInstanceRequest",
    {
      instance_id: instanceId,
      poll_interval_secs: pollInterval,
    }
  );
  const response = (await callUnary(client, "waitForInstance", request)) as any;

  if (!response) {
    return null;
  }

  const payloadBytes =
    typeof response.getPayload_asU8 === "function"
      ? response.getPayload_asU8()
      : response.getPayload();
  if (!payloadBytes || payloadBytes.length === 0) {
    return null;
  }

  return decodeWorkflowArguments(payloadBytes);
}

async function decodeWorkflowArguments(payloadBytes) {
  return decodeMessage("rappel.messages.WorkflowArguments", payloadBytes);
}

function hashIr(bytes) {
  return crypto.createHash("sha256").update(bytes).digest("hex");
}
