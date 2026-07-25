const actionMarker = Symbol.for("@waymark/nextjs/action");

export type Action<Arguments extends readonly unknown[], Result> = ((
  ...arguments_: Arguments
) => Promise<Result>) & {
  readonly [actionMarker]: true;
};

export function action<Arguments extends readonly unknown[], Result>(
  implementation: (...arguments_: Arguments) => Promise<Result>,
): Action<Arguments, Result> {
  Object.defineProperty(implementation, actionMarker, { value: true });
  return implementation as Action<Arguments, Result>;
}

export function isAction(value: unknown): value is Action<readonly unknown[], unknown> {
  return typeof value === "function" && actionMarker in value;
}
