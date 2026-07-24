import { ActionRuntime } from "./internal/proto/action.js";
import type {
  ActionDispatch,
  ActionResult,
} from "./internal/proto/messages.js";
import { ActionManifest } from "./action-manifest.js";
import {
  decodeWorkflowArguments,
  encodeWorkflowArguments,
} from "./values.js";

function errorDetails(error: unknown): {
  readonly message: string;
  readonly stack: string;
  readonly type: string;
} {
  if (error instanceof Error) {
    return {
      message: error.message,
      stack: error.stack ?? "",
      type: error.name,
    };
  }
  return { message: String(error), stack: "", type: "Error" };
}

export async function executeActionDispatch(
  dispatch: ActionDispatch,
  manifest: ActionManifest,
): Promise<ActionResult> {
  const workerStartNs = process.hrtime.bigint();
  try {
    if (dispatch.runtime !== ActionRuntime.ACTION_RUNTIME_JAVASCRIPT) {
      throw new Error(
        `JavaScript worker cannot execute action runtime ${dispatch.runtime}`,
      );
    }
    const entry = manifest.get(dispatch.moduleName, dispatch.actionName);
    if (entry === undefined) {
      throw new Error(
        `action ${dispatch.moduleName}:${dispatch.actionName} is not in the manifest`,
      );
    }
    const kwargs = decodeWorkflowArguments(
      dispatch.kwargs ?? { arguments: [] },
    );
    const expectedNames = new Set(entry.parameterNames);
    for (const name of entry.parameterNames) {
      if (!(name in kwargs)) {
        throw new Error(`action argument ${name} is missing`);
      }
    }
    for (const name of Object.keys(kwargs)) {
      if (!expectedNames.has(name)) {
        throw new Error(`action argument ${name} is unexpected`);
      }
    }
    const result = await entry.implementation(
      ...entry.parameterNames.map((name) => kwargs[name]),
    );
    return {
      actionId: dispatch.actionId,
      success: true,
      payload: encodeWorkflowArguments({ result }),
      workerStartNs,
      workerEndNs: process.hrtime.bigint(),
      dispatchToken: dispatch.dispatchToken,
      errorType: undefined,
      errorMessage: undefined,
      metadata: dispatch.metadata,
    };
  } catch (error) {
    const details = errorDetails(error);
    return {
      actionId: dispatch.actionId,
      success: false,
      payload: encodeWorkflowArguments({ error: details }),
      workerStartNs,
      workerEndNs: process.hrtime.bigint(),
      dispatchToken: dispatch.dispatchToken,
      errorType: details.type,
      errorMessage: details.message,
      metadata: dispatch.metadata,
    };
  }
}
