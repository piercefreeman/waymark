import {
  type ActionManifest,
  type CompiledWorkflow,
  createWorkflowClient,
  executeWorkflow,
} from "@waymark/nextjs";
import { NextResponse } from "next/server";

import generatedManifest from "../../../.waymark/actions.mjs";
import generatedWorkflow from "../../../.waymark/workflow.mjs";

export const runtime = "nodejs";

interface MathRequest {
  number?: unknown;
}

export async function POST(request: Request): Promise<NextResponse> {
  const payload = (await request.json()) as MathRequest;
  if (
    typeof payload.number !== "number" ||
    !Number.isInteger(payload.number) ||
    payload.number < 1 ||
    payload.number > 10
  ) {
    return NextResponse.json(
      { error: "number must be an integer from 1 through 10" },
      { status: 400 },
    );
  }

  const client = createWorkflowClient(
    process.env.WAYMARK_BRIDGE ?? "127.0.0.1:24117",
  );
  try {
    const result = await executeWorkflow({
      client,
      compiled: generatedWorkflow as unknown as CompiledWorkflow,
      input: { number: payload.number },
      manifest: generatedManifest as unknown as ActionManifest,
    });
    return NextResponse.json(result);
  } finally {
    client.close();
  }
}
