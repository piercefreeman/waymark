import {
  credentials,
  type ChannelCredentials,
  type ClientDuplexStream,
} from "@grpc/grpc-js";

import type { CompiledWorkflow } from "./compiler.js";
import {
  WorkflowArguments,
  type WorkflowRegistration,
  type WorkflowStreamRequest,
  type WorkflowStreamResponse,
  WorkflowServiceClient,
} from "./internal/proto/messages.js";
import { ActionManifest } from "./action-manifest.js";
import { executeActionDispatch } from "./action-runtime.js";
import {
  decodeWorkflowValue,
  encodeWorkflowArguments,
  type WorkflowValue,
} from "./values.js";

export function createWorkflowClient(
  target: string,
  channelCredentials: ChannelCredentials = credentials.createInsecure(),
): WorkflowServiceClient {
  return new WorkflowServiceClient(target, channelCredentials);
}

export interface ExecuteWorkflowOptions {
  readonly client: WorkflowServiceClient;
  readonly compiled: CompiledWorkflow;
  readonly concurrent?: boolean;
  readonly input: Readonly<Record<string, unknown>>;
  readonly manifest: ActionManifest;
  readonly skipSleep?: boolean;
  readonly version?: string;
}

export class WorkflowExecutionError extends Error {
  constructor(readonly details: WorkflowValue) {
    super(`workflow failed: ${JSON.stringify(details)}`);
    this.name = "WorkflowExecutionError";
  }
}

function decodeWorkflowCompletion(payload: Buffer): WorkflowValue {
  const arguments_ = WorkflowArguments.decode(payload);
  const error = arguments_.arguments.find(
    (argument) => argument.key === "error",
  )?.value;
  if (error !== undefined) {
    throw new WorkflowExecutionError(decodeWorkflowValue(error));
  }
  const result = arguments_.arguments.find(
    (argument) => argument.key === "result",
  )?.value;
  if (result === undefined) {
    throw new Error("workflow result payload is missing `result`");
  }
  if (result.kind?.$case !== "basemodel") {
    return decodeWorkflowValue(result);
  }
  if (
    result.kind.value.module !== "waymark.workflow_runtime" ||
    result.kind.value.name !== "WorkflowNodeResult"
  ) {
    throw new Error(
      `unsupported workflow result model ${result.kind.value.module}:${result.kind.value.name}`,
    );
  }
  const variables = result.kind.value.data?.entries.find(
    (entry) => entry.key === "variables",
  )?.value;
  if (variables === undefined) {
    throw new Error("WorkflowNodeResult is missing `variables`");
  }
  const decoded = decodeWorkflowValue(variables);
  if (
    decoded !== null &&
    !Array.isArray(decoded) &&
    typeof decoded === "object" &&
    "result" in decoded
  ) {
    return decoded.result;
  }
  return decoded;
}

function writeRequest(
  stream: ClientDuplexStream<WorkflowStreamRequest, WorkflowStreamResponse>,
  request: WorkflowStreamRequest,
): Promise<void> {
  return new Promise((resolve, reject) => {
    stream.write(request, (error?: Error | null) => {
      if (error === null || error === undefined) {
        resolve();
      } else {
        reject(error);
      }
    });
  });
}

export async function executeWorkflow(
  options: ExecuteWorkflowOptions,
): Promise<WorkflowValue> {
  const registration: WorkflowRegistration = {
    workflowName: options.compiled.workflowName,
    ir: options.compiled.bytes,
    irHash: options.compiled.hash,
    workflowVersion: options.version ?? options.compiled.hash,
    initialContext: encodeWorkflowArguments({
      [options.compiled.inputName]: options.input,
    }),
    concurrent: options.concurrent ?? false,
    priority: undefined,
  };
  const stream = options.client.executeWorkflow();

  return await new Promise<WorkflowValue>((resolve, reject) => {
    let settled = false;
    const fail = (error: unknown) => {
      if (!settled) {
        settled = true;
        stream.cancel();
        reject(error);
      }
    };

    stream.on("data", (response: WorkflowStreamResponse) => {
      switch (response.kind?.$case) {
        case "actionDispatch":
          void executeActionDispatch(response.kind.value, options.manifest)
            .then((result) =>
              writeRequest(stream, {
                kind: { $case: "actionResult", value: result },
                skipSleep: options.skipSleep ?? false,
              }),
            )
            .catch(fail);
          break;
        case "workflowResult":
          if (!settled) {
            try {
              const result = decodeWorkflowCompletion(
                response.kind.value.payload,
              );
              settled = true;
              stream.end();
              resolve(result);
            } catch (error) {
              fail(error);
            }
          }
          break;
      }
    });
    stream.on("error", fail);
    stream.on("end", () => {
      if (!settled) {
        fail(new Error("workflow stream ended without a result"));
      }
    });

    void writeRequest(stream, {
      kind: { $case: "registration", value: registration },
      skipSleep: options.skipSleep ?? false,
    }).catch(fail);
  });
}
