import path from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";

import express from "express";
import { Client } from "pg";

import {
  buildScheduleDefinition,
  deleteSchedule,
  registerSchedule,
  registerWorkflowBatch,
  updateScheduleStatus,
} from "@rappel/js/admin";
import { resolveWorkflow } from "@rappel/js/registry";
import { start } from "@rappel/js/workflow";

import "./workflows.js";

import type {
  BranchResult,
  ChainResult,
  ComputationResult,
  ErrorResult,
  GuardFallbackResult,
  KwOnlyLocationResult,
  LoopExceptionResult,
  LoopResult,
  LoopReturnResult,
  SleepResult,
  SpreadEmptyResult,
} from "./workflows.js";

const DEFAULT_SCHEDULE_NAME = "default";

type WorkflowKey = string;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.resolve(__dirname, "..", "public");

const app = express();
app.use(express.json());
app.use(express.static(publicDir));

app.get("/", (_req, res) => {
  res.sendFile(path.join(publicDir, "index.html"));
});

// =============================================================================
// Parallel Execution
// =============================================================================

app.post("/api/parallel", async (req, res) => {
  const payload = req.body as { number: number };
  try {
    const result = (await start("ParallelMathWorkflow", {
      number: payload.number,
    })) as ComputationResult;
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Sequential Chain
// =============================================================================

app.post("/api/chain", async (req, res) => {
  const payload = req.body as { text: string };
  try {
    const result = (await start("SequentialChainWorkflow", {
      text: payload.text,
    })) as ChainResult;
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Conditional Branching
// =============================================================================

app.post("/api/branch", async (req, res) => {
  const payload = req.body as { value: number };
  try {
    const result = (await start("ConditionalBranchWorkflow", {
      value: payload.value,
    })) as BranchResult;
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Loop Processing
// =============================================================================

app.post("/api/loop", async (req, res) => {
  const payload = req.body as { items: string[] };
  try {
    const result = (await start("LoopProcessingWorkflow", {
      items: payload.items,
    })) as LoopResult;
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Return Inside Loop
// =============================================================================

app.post("/api/loop-return", async (req, res) => {
  const payload = req.body as { items: number[]; needle: number };
  try {
    const result = (await start("LoopReturnWorkflow", {
      items: payload.items,
      needle: payload.needle,
    })) as LoopReturnResult;
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Loop with Exception Handling
// =============================================================================

app.post("/api/loop-exception", async (req, res) => {
  const payload = req.body as { items: string[] };
  try {
    const result = (await start("LoopExceptionWorkflow", {
      items: payload.items,
    })) as LoopExceptionResult;
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Error Handling
// =============================================================================

app.post("/api/error", async (req, res) => {
  const payload = req.body as { should_fail: boolean };
  try {
    const result = (await start("ErrorHandlingWorkflow", {
      should_fail: payload.should_fail,
    })) as ErrorResult;
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

app.post("/api/exception-metadata", async (req, res) => {
  const payload = req.body as { should_fail: boolean };
  try {
    const result = (await start("ExceptionMetadataWorkflow", {
      should_fail: payload.should_fail,
    })) as ErrorResult;
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Durable Sleep
// =============================================================================

app.post("/api/sleep", async (req, res) => {
  const payload = req.body as { seconds: number };
  try {
    const result = (await start("DurableSleepWorkflow", {
      seconds: payload.seconds,
    })) as SleepResult;
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Guard Fallback
// =============================================================================

app.post("/api/guard-fallback", async (req, res) => {
  const payload = req.body as { user: string };
  try {
    const result = (await start("GuardFallbackWorkflow", {
      user: payload.user,
    })) as GuardFallbackResult;
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Kw-only Inputs
// =============================================================================

app.post("/api/kw-only", async (req, res) => {
  const payload = req.body as { latitude?: number | null; longitude?: number | null };
  try {
    const result = (await start("KwOnlyLocationWorkflow", {
      latitude: payload.latitude ?? null,
      longitude: payload.longitude ?? null,
    })) as KwOnlyLocationResult;
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Undefined Variable Validation
// =============================================================================

app.post("/api/undefined-variable", async (req, res) => {
  const payload = req.body as { input_text: string };
  try {
    const result = await start("UndefinedVariableWorkflow", {
      input_text: payload.input_text,
    });
    res.json({ result });
  } catch (error) {
    res.status(400).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Early Return + Loop
// =============================================================================

app.post("/api/early-return-loop", async (req, res) => {
  const payload = req.body as { input_text: string };
  try {
    const result = await start("EarlyReturnLoopWorkflow", {
      input_text: payload.input_text,
    });
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Spread Empty Collection
// =============================================================================

app.post("/api/spread-empty", async (req, res) => {
  const payload = req.body as { items: string[] };
  try {
    const result = (await start("SpreadEmptyCollectionWorkflow", {
      items: payload.items,
    })) as SpreadEmptyResult;
    res.json(result);
  } catch (error) {
    res.status(500).json({ detail: errorMessage(error) });
  }
});

// =============================================================================
// Scheduled Workflows
// =============================================================================

app.post("/api/schedule", async (req, res) => {
  const payload = req.body as {
    workflow_name: WorkflowKey;
    schedule_type: "cron" | "interval";
    cron_expression?: string;
    interval_seconds?: number;
    inputs?: Record<string, unknown> | null;
    priority?: number | null;
  };

  const entry = resolveWorkflow(payload.workflow_name);
  if (!entry) {
    res.json({ success: false, message: `Unknown workflow: ${payload.workflow_name}` });
    return;
  }

  const schedule = buildScheduleDefinition(payload);
  if (!schedule) {
    res.json({ success: false, message: "Invalid schedule configuration" });
    return;
  }

  try {
    const scheduleId = await registerSchedule(entry, schedule, {
      scheduleName: DEFAULT_SCHEDULE_NAME,
      inputs: payload.inputs ?? null,
      priority: payload.priority ?? null,
    });

    res.json({
      success: true,
      schedule_id: scheduleId,
      message: `Schedule registered for ${payload.workflow_name}`,
    });
  } catch (error) {
    res.json({ success: false, message: errorMessage(error) });
  }
});

app.post("/api/schedule/pause", async (req, res) => {
  await handleScheduleStatusUpdate(req, res, "paused");
});

app.post("/api/schedule/resume", async (req, res) => {
  await handleScheduleStatusUpdate(req, res, "active");
});

app.post("/api/schedule/delete", async (req, res) => {
  const payload = req.body as { workflow_name: WorkflowKey };
  const entry = resolveWorkflow(payload.workflow_name);
  if (!entry) {
    res.json({ success: false, message: `Unknown workflow: ${payload.workflow_name}` });
    return;
  }

  try {
    const success = await deleteSchedule(entry, DEFAULT_SCHEDULE_NAME);
    if (success) {
      res.json({
        success: true,
        message: `Schedule deleted for ${payload.workflow_name}`,
      });
      return;
    }

    res.json({
      success: false,
      message: `No schedule found for ${payload.workflow_name}`,
    });
  } catch (error) {
    res.json({ success: false, message: errorMessage(error) });
  }
});

// =============================================================================
// Batch Run
// =============================================================================

app.post("/api/batch-run", async (req, res) => {
  const payload = req.body as {
    workflow_name: WorkflowKey;
    count?: number;
    inputs?: Record<string, unknown> | null;
    inputs_list?: Array<Record<string, unknown>> | null;
    batch_size?: number;
    priority?: number | null;
    include_instance_ids?: boolean;
  };

  const entry = resolveWorkflow(payload.workflow_name);
  if (!entry) {
    res.status(404).json({ detail: `Unknown workflow: ${payload.workflow_name}` });
    return;
  }

  const inputsList = payload.inputs_list ?? null;
  const total = inputsList ? inputsList.length : payload.count ?? 1;
  if (total < 1) {
    res.status(400).json({ detail: "count must be >= 1" });
    return;
  }

  if (inputsList && inputsList.length === 0) {
    res.status(400).json({ detail: "inputs_list must not be empty" });
    return;
  }

  const startTime = performance.now();
  const batchSize = payload.batch_size ?? 500;

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  sendEvent(res, "start", {
    workflow_name: payload.workflow_name,
    total,
    batch_size: batchSize,
  });

  try {
    const response = await registerWorkflowBatch(entry, {
      count: total,
      inputs: payload.inputs ?? null,
      inputsList: inputsList ?? null,
      batchSize,
      priority: payload.priority ?? null,
      includeInstanceIds: payload.include_instance_ids,
    });

    const elapsedMs = Math.round(performance.now() - startTime);
    sendEvent(res, "complete", {
      workflow_version_id: response.workflowVersionId,
      queued: response.queued,
      total,
      elapsed_ms: elapsedMs,
      instance_ids: payload.include_instance_ids
        ? response.workflowInstanceIds
        : null,
    });
  } catch (error) {
    sendEvent(res, "error", { message: errorMessage(error) });
  } finally {
    res.end();
  }
});

// =============================================================================
// Database Reset
// =============================================================================

app.post("/api/reset", async (_req, res) => {
  const databaseUrl = process.env.RAPPEL_DATABASE_URL;
  if (!databaseUrl) {
    res.json({ success: false, message: "RAPPEL_DATABASE_URL not configured" });
    return;
  }

  const client = new Client({ connectionString: databaseUrl });
  try {
    await client.connect();
    await client.query("DELETE FROM daemon_action_ledger");
    await client.query("DELETE FROM workflow_instances");
    await client.query("DELETE FROM workflow_versions");
    res.json({ success: true, message: "All workflow data cleared" });
  } catch (error) {
    res.json({ success: false, message: errorMessage(error) });
  } finally {
    await client.end();
  }
});

const port = Number(process.env.PORT || 8001);
app.listen(port, () => {
  console.log(`Rappel JS example app listening on http://localhost:${port}`);
});

function errorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

async function handleScheduleStatusUpdate(
  req: express.Request,
  res: express.Response,
  status: "active" | "paused"
) {
  const payload = req.body as { workflow_name: WorkflowKey };
  const entry = resolveWorkflow(payload.workflow_name);
  if (!entry) {
    res.json({ success: false, message: `Unknown workflow: ${payload.workflow_name}` });
    return;
  }

  try {
    const success = await updateScheduleStatus(
      entry,
      DEFAULT_SCHEDULE_NAME,
      status
    );
    if (success) {
      const verb = status === "active" ? "resumed" : "paused";
      res.json({
        success: true,
        message: `Schedule ${verb} for ${payload.workflow_name}`,
      });
      return;
    }

    res.json({
      success: false,
      message: `No ${status} schedule found for ${payload.workflow_name}`,
    });
  } catch (error) {
    res.json({ success: false, message: errorMessage(error) });
  }
}

function sendEvent(res: express.Response, event: string, data: Record<string, unknown>) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}
