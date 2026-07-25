export type Duration = `${number}s` | `${number}m` | `${number}h`;

export interface RetryPolicy {
  readonly attempts: number;
  readonly backoffSeconds?: number;
}

export interface ActionPolicies {
  readonly retry?: RetryPolicy;
  readonly timeout?: Duration;
}

export abstract class Workflow<Input = unknown, Output = unknown> {
  abstract run(input: Input): Promise<Output>;

  protected async runAction<Result>(
    result: Promise<Result>,
    _policies?: ActionPolicies,
  ): Promise<Result> {
    return await result;
  }
}
