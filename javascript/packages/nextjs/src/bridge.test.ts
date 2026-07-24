import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";

import { action } from "./action.js";
import { createActionManifest } from "./action-manifest.js";
import { executeWorkflow } from "./bridge.js";
import type { CompiledWorkflow } from "./compiler.js";
import { ActionRuntime } from "./internal/proto/action.js";
import {
  WorkflowArguments,
  type WorkflowServiceClient,
  type WorkflowStreamRequest,
  type WorkflowStreamResponse,
} from "./internal/proto/messages.js";
import {
  decodeWorkflowArguments,
  encodeWorkflowArguments,
  encodeWorkflowValue,
} from "./values.js";

class FakeWorkflowStream extends EventEmitter {
  readonly writes: WorkflowStreamRequest[] = [];
  cancelled = false;
  ended = false;

  write(
    request: WorkflowStreamRequest,
    callback: (error?: Error | null) => void,
  ): boolean {
    this.writes.push(request);
    callback();
    if (request.kind?.$case === "registration") {
      queueMicrotask(() => {
        this.emit("data", {
          kind: {
            $case: "actionDispatch",
            value: {
              actionId: "action-1",
              instanceId: "instance-1",
              sequence: 1,
              actionName: "greet",
              moduleName: "src/actions.ts",
              kwargs: encodeWorkflowArguments({ name: "Ada" }),
              timeoutSeconds: undefined,
              maxRetries: undefined,
              attemptNumber: 1,
              dispatchToken: "token-1",
              metadata: Buffer.from([9, 8, 7]),
              runtime: ActionRuntime.ACTION_RUNTIME_JAVASCRIPT,
            },
          },
        } satisfies WorkflowStreamResponse);
      });
    } else if (request.kind?.$case === "actionResult") {
      queueMicrotask(() => {
        const completion = {
          arguments: [
            {
              key: "result",
              value: {
                kind: {
                  $case: "basemodel" as const,
                  value: {
                    module: "waymark.workflow_runtime",
                    name: "WorkflowNodeResult",
                    data: {
                      entries: [
                        {
                          key: "variables",
                          value: encodeWorkflowValue({ result: "complete" }),
                        },
                      ],
                    },
                  },
                },
              },
            },
          ],
        };
        this.emit("data", {
          kind: {
            $case: "workflowResult",
            value: {
              payload: Buffer.from(
                WorkflowArguments.encode(completion).finish(),
              ),
            },
          },
        } satisfies WorkflowStreamResponse);
      });
    }
    return true;
  }

  cancel(): void {
    this.cancelled = true;
  }

  end(): void {
    this.ended = true;
  }
}

test("transient execution registers compiled IR and services action dispatches", async () => {
  const greet = action(async function greet(name: string): Promise<string> {
    return `Hello ${name}`;
  });
  const manifest = createActionManifest([
    {
      actionName: "greet",
      moduleName: "src/actions.ts",
      parameterNames: ["name"],
      implementation: greet,
    },
  ]);
  const compiled: CompiledWorkflow = {
    actions: [
      {
        actionName: "greet",
        moduleName: "src/actions.ts",
        parameterNames: ["name"],
      },
    ],
    bytes: Buffer.from([1, 2, 3]),
    hash: "ir-hash",
    moduleId: "src/workflow.ts",
    program: { functions: [] },
    workflowName: "GreetingWorkflow",
  };
  const stream = new FakeWorkflowStream();
  const client = {
    executeWorkflow: () => stream,
  } as unknown as WorkflowServiceClient;

  const result = await executeWorkflow({
    client,
    compiled,
    input: { requestId: "request-1" },
    manifest,
    skipSleep: true,
  });

  assert.equal(result, "complete");
  assert.equal(stream.ended, true);
  assert.equal(stream.cancelled, false);
  assert.equal(stream.writes.length, 2);
  const registration = stream.writes[0]?.kind;
  assert.equal(registration?.$case, "registration");
  if (registration?.$case === "registration") {
    assert.deepEqual(
      decodeWorkflowArguments(
        registration.value.initialContext ?? { arguments: [] },
      ),
      { requestId: "request-1" },
    );
    assert.deepEqual(registration.value.ir, Buffer.from([1, 2, 3]));
    assert.equal(registration.value.irHash, "ir-hash");
  }
  const actionResult = stream.writes[1]?.kind;
  assert.equal(actionResult?.$case, "actionResult");
  if (actionResult?.$case === "actionResult") {
    assert.equal(actionResult.value.success, true);
    assert.equal(actionResult.value.dispatchToken, "token-1");
    assert.deepEqual(actionResult.value.metadata, Buffer.from([9, 8, 7]));
    assert.deepEqual(
      decodeWorkflowArguments(
        actionResult.value.payload ?? { arguments: [] },
      ),
      { result: "Hello Ada" },
    );
  }
});
