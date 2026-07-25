import { Workflow } from "@waymark/nextjs";

import {
  computeFactorial,
  computeFibonacci,
  type MathResult,
  summarizeMath,
} from "./actions";

export interface MathInput {
  number: number;
}

export class ExampleMathWorkflow extends Workflow<MathInput, MathResult> {
  async run(input: MathInput): Promise<MathResult> {
    const [factorial, fibonacci] = await Promise.all([
      computeFactorial(input.number),
      computeFibonacci(input.number),
    ]);

    return await summarizeMath(input.number, factorial, fibonacci);
  }
}
