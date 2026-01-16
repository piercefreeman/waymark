import { callUnary, getWorkflowClient } from "./bridge.js";
import { toProtoMessage } from "./proto.js";
import { resolveWorkflow } from "./registry.js";
import { buildWorkflowArguments } from "./serialization.js";

export type ScheduleInput = {
  schedule_type: "cron" | "interval";
  cron_expression?: string;
  interval_seconds?: number;
  jitter_seconds?: number;
};

export type ScheduleDefinition = {
  type: number;
  cron_expression: string;
  interval_seconds: number;
  jitter_seconds: number;
};

export type RegisterScheduleOptions = {
  scheduleName?: string;
  inputs?: Record<string, unknown> | null;
  priority?: number | null;
};

export type RegisterBatchOptions = {
  count?: number;
  inputs?: Record<string, unknown> | null;
  inputsList?: Array<Record<string, unknown>> | null;
  batchSize?: number;
  priority?: number | null;
  includeInstanceIds?: boolean;
};

export function buildScheduleDefinition(input: ScheduleInput): ScheduleDefinition | null {
  if (input.schedule_type === "cron") {
    if (!input.cron_expression) {
      return null;
    }
    return {
      type: 1,
      cron_expression: input.cron_expression,
      interval_seconds: 0,
      jitter_seconds: input.jitter_seconds ?? 0,
    };
  }

  if (!input.interval_seconds) {
    return null;
  }

  return {
    type: 2,
    interval_seconds: input.interval_seconds,
    cron_expression: "",
    jitter_seconds: input.jitter_seconds ?? 0,
  };
}

export async function registerSchedule(
  workflow,
  schedule: ScheduleDefinition,
  options: RegisterScheduleOptions = {}
) {
  const entry = resolveWorkflowEntry(workflow);
  const registration = buildWorkflowRegistration(entry, options.priority ?? null);

  const requestPayload: Record<string, unknown> = {
    workflow_name: entry.shortName,
    schedule_name: options.scheduleName || "default",
    schedule,
    registration,
  };

  if (options.inputs) {
    requestPayload.inputs = buildWorkflowArguments(options.inputs);
  }

  if (options.priority !== null && options.priority !== undefined) {
    requestPayload.priority = options.priority;
  }

  const request = await toProtoMessage(
    "rappel.messages.RegisterScheduleRequest",
    requestPayload
  );

  const client = await getWorkflowClient();
  const response = (await callUnary(client, "registerSchedule", request)) as any;
  return response.getScheduleId();
}

export async function updateScheduleStatus(
  workflow,
  scheduleName: string,
  status: "active" | "paused"
) {
  if (!scheduleName) {
    throw new Error("scheduleName is required");
  }

  const entry = resolveWorkflowEntry(workflow);
  const statusValue = status === "active" ? 1 : 2;
  const request = await toProtoMessage(
    "rappel.messages.UpdateScheduleStatusRequest",
    {
      workflow_name: entry.shortName,
      schedule_name: scheduleName,
      status: statusValue,
    }
  );

  const client = await getWorkflowClient();
  const response = (await callUnary(client, "updateScheduleStatus", request)) as any;
  return Boolean(response.getSuccess());
}

export async function deleteSchedule(workflow, scheduleName: string) {
  if (!scheduleName) {
    throw new Error("scheduleName is required");
  }

  const entry = resolveWorkflowEntry(workflow);
  const request = await toProtoMessage(
    "rappel.messages.DeleteScheduleRequest",
    {
      workflow_name: entry.shortName,
      schedule_name: scheduleName,
    }
  );

  const client = await getWorkflowClient();
  const response = (await callUnary(client, "deleteSchedule", request)) as any;
  return Boolean(response.getSuccess());
}

export async function registerWorkflowBatch(
  workflow,
  options: RegisterBatchOptions = {}
) {
  const entry = resolveWorkflowEntry(workflow);
  const count = options.count ?? 1;
  const batchSize = options.batchSize ?? 500;

  if (count < 1 && !options.inputsList) {
    throw new Error("count must be >= 1 when inputsList is empty");
  }

  if (batchSize < 1) {
    throw new Error("batchSize must be >= 1");
  }

  const registration = buildWorkflowRegistration(entry, options.priority ?? null);
  const requestPayload: Record<string, unknown> = {
    registration,
    count,
    batch_size: batchSize,
    include_instance_ids: Boolean(options.includeInstanceIds),
  };

  if (options.inputsList) {
    if (options.inputsList.length === 0) {
      throw new Error("inputsList must not be empty");
    }
    requestPayload.inputs_list = options.inputsList.map((inputs) =>
      buildWorkflowArguments(inputs)
    );
  } else if (options.inputs) {
    requestPayload.inputs = buildWorkflowArguments(options.inputs);
  }

  const request = await toProtoMessage(
    "rappel.messages.RegisterWorkflowBatchRequest",
    requestPayload
  );

  const client = await getWorkflowClient();
  const response = (await callUnary(client, "registerWorkflowBatch", request)) as any;
  return {
    workflowVersionId: response.getWorkflowVersionId(),
    workflowInstanceIds: response.getWorkflowInstanceIdsList(),
    queued: response.getQueued(),
  };
}

function resolveWorkflowEntry(workflow) {
  if (
    workflow &&
    typeof workflow === "object" &&
    "shortName" in workflow &&
    "ir" in workflow
  ) {
    return workflow;
  }

  const entry = resolveWorkflow(workflow);
  if (!entry) {
    throw new Error("workflow is not registered");
  }
  return entry;
}

function buildWorkflowRegistration(entry, priority: number | null) {
  const registration: Record<string, unknown> = {
    workflow_name: entry.shortName,
    ir: entry.ir,
    ir_hash: entry.irHash,
    concurrent: Boolean(entry.concurrent),
  };

  if (priority !== null) {
    registration.priority = priority;
  }

  return registration;
}
