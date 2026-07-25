import { action } from "@waymark/nextjs";

export const computeFactorial = action(async function computeFactorial(
  number: number,
): Promise<number> {
  let total = 1;
  for (let value = 2; value <= number; value += 1) {
    total *= value;
  }
  return total;
});

export const computeFibonacci = action(async function computeFibonacci(
  number: number,
): Promise<number> {
  let previous = 0;
  let current = 1;
  for (let index = 0; index < number; index += 1) {
    [previous, current] = [current, previous + current];
  }
  return previous;
});

export const summarizeMath = action(async function summarizeMath(
  number: number,
  factorial: number,
  fibonacci: number,
): Promise<MathResult> {
  return {
    number,
    factorial,
    fibonacci,
    summary: `${number}! is ${factorial}; Fibonacci(${number}) is ${fibonacci}.`,
  };
});

export interface MathResult {
  factorial: number;
  fibonacci: number;
  number: number;
  summary: string;
}
