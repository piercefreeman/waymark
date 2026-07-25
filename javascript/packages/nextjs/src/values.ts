import { NullValue } from "./internal/proto/google/protobuf/struct.js";
import type {
  PrimitiveWorkflowArgument,
  WorkflowArguments,
  WorkflowArgumentValue,
} from "./internal/proto/messages.js";

export type WorkflowValue =
  | null
  | boolean
  | string
  | number
  | readonly WorkflowValue[]
  | { readonly [key: string]: WorkflowValue };

function primitive(
  kind: PrimitiveWorkflowArgument["kind"],
): WorkflowArgumentValue {
  return { kind: { $case: "primitive", value: { kind } } };
}

function encodeValue(
  value: unknown,
  path: string,
  ancestors: WeakSet<object>,
): WorkflowArgumentValue {
  if (value === null) {
    return primitive({ $case: "nullValue", value: NullValue.NULL_VALUE });
  }
  if (typeof value === "boolean") {
    return primitive({ $case: "boolValue", value });
  }
  if (typeof value === "string") {
    return primitive({ $case: "stringValue", value });
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError(`${path} must be a finite number`);
    }
    if (Number.isInteger(value)) {
      if (!Number.isSafeInteger(value)) {
        throw new TypeError(`${path} must be a safe integer`);
      }
      return primitive({ $case: "intValue", value: BigInt(value) });
    }
    return primitive({ $case: "doubleValue", value });
  }
  if (typeof value !== "object") {
    throw new TypeError(`${path} contains unsupported ${typeof value}`);
  }
  if (ancestors.has(value)) {
    throw new TypeError(`${path} contains a cycle`);
  }

  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      return {
        kind: {
          $case: "listValue",
          value: {
            items: Array.from(value, (item, index) =>
              encodeValue(item, `${path}[${index}]`, ancestors),
            ),
          },
        },
      };
    }
    if (Object.getPrototypeOf(value) !== Object.prototype) {
      throw new TypeError(`${path} must be a plain object`);
    }
    if (Object.getOwnPropertySymbols(value).length > 0) {
      throw new TypeError(`${path} cannot contain symbol keys`);
    }

    const entries = Object.keys(value)
      .sort()
      .map((key) => {
        const descriptor = Object.getOwnPropertyDescriptor(value, key);
        if (descriptor?.get !== undefined || descriptor?.set !== undefined) {
          throw new TypeError(`${path}.${key} must be a data property`);
        }
        return {
          key,
          value: encodeValue(
            (value as Record<string, unknown>)[key],
            `${path}.${key}`,
            ancestors,
          ),
        };
      });
    return { kind: { $case: "dictValue", value: { entries } } };
  } finally {
    ancestors.delete(value);
  }
}

export function encodeWorkflowValue(value: unknown): WorkflowArgumentValue {
  return encodeValue(value, "$", new WeakSet());
}

export function encodeWorkflowArguments(
  values: Readonly<Record<string, unknown>>,
): WorkflowArguments {
  return {
    arguments: Object.keys(values)
      .sort()
      .map((key) => ({ key, value: encodeWorkflowValue(values[key]) })),
  };
}

export function decodeWorkflowValue(value: WorkflowArgumentValue): WorkflowValue {
  switch (value.kind?.$case) {
    case "primitive":
      switch (value.kind.value.kind?.$case) {
        case "nullValue":
          return null;
        case "boolValue":
        case "stringValue":
        case "doubleValue":
          return value.kind.value.kind.value;
        case "intValue": {
          const decoded = Number(value.kind.value.kind.value);
          if (!Number.isSafeInteger(decoded)) {
            throw new TypeError("received an unsafe integer");
          }
          return decoded;
        }
        case undefined:
          throw new TypeError("received a primitive without a value");
      }
      break;
    case "listValue":
    case "tupleValue":
      return value.kind.value.items.map(decodeWorkflowValue);
    case "dictValue":
      return Object.fromEntries(
        value.kind.value.entries.map((entry) => {
          if (entry.value === undefined) {
            throw new TypeError(`received object key ${entry.key} without a value`);
          }
          return [entry.key, decodeWorkflowValue(entry.value)];
        }),
      );
    case "basemodel":
      throw new TypeError("Python model values are not supported by JavaScript actions");
    case "exception":
      throw new TypeError("workflow exception values cannot be decoded as action arguments");
    case undefined:
      throw new TypeError("received an empty workflow value");
  }

  throw new TypeError("received an unknown workflow value");
}

export function decodeWorkflowArguments(
  values: WorkflowArguments,
): Record<string, WorkflowValue> {
  return Object.fromEntries(
    values.arguments.map((argument) => {
      if (argument.value === undefined) {
        throw new TypeError(`received argument ${argument.key} without a value`);
      }
      return [argument.key, decodeWorkflowValue(argument.value)];
    }),
  );
}
