import { NextResponse } from 'next/server';

import { ExampleMathWorkflow } from '@/lib/workflows/example-math-workflow';

export async function POST(request: Request): Promise<Response> {
  const body = (await request.json()) as { number?: number };
  const workflow = new ExampleMathWorkflow();
  const result = await workflow.run(body.number ?? 5);
  return NextResponse.json({ result });
}
