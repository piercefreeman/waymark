import '../../../.waymark/actions-bootstrap.mjs';

import { NextResponse } from 'next/server';

import { ExampleMathWorkflow } from '@/lib/workflows/example-math-workflow';

export const runtime = 'nodejs';

if (shouldDefaultToInMemoryBridge()) {
  process.env.WAYMARK_BRIDGE_IN_MEMORY ??= '1';
}

export async function POST(request: Request): Promise<Response> {
  try {
    const body = (await request.json()) as { number?: number };
    const workflow = new ExampleMathWorkflow();
    const result = await workflow.run(body.number ?? 5);
    return NextResponse.json({ result: unwrapWorkflowResult(result) });
  } catch (error) {
    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : String(error)
      },
      { status: 500 }
    );
  }
}

function unwrapWorkflowResult(value: unknown): unknown {
  if (!value || typeof value !== 'object') {
    return value;
  }

  const resultEnvelope = value as {
    __type?: string;
    data?: {
      variables?: {
        result?: unknown;
      };
    };
    name?: string;
  };

  if (
    resultEnvelope.__type === 'basemodel' &&
    resultEnvelope.name === 'WorkflowNodeResult' &&
    resultEnvelope.data?.variables &&
    Object.prototype.hasOwnProperty.call(resultEnvelope.data.variables, 'result')
  ) {
    return resultEnvelope.data.variables.result;
  }

  return value;
}

function shouldDefaultToInMemoryBridge(): boolean {
  return !(
    process.env.WAYMARK_DATABASE_URL ||
    process.env.WAYMARK_BRIDGE_GRPC_ADDR ||
    process.env.WAYMARK_BRIDGE_GRPC_HOST ||
    process.env.WAYMARK_BRIDGE_GRPC_PORT
  );
}
