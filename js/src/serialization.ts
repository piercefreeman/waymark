const NULL_VALUE = 0;

export function buildWorkflowArguments(input) {
  const entries = Object.entries(input ?? {});
  return {
    arguments: entries.map(([key, value]) => ({
      key,
      value: serializeValue(value),
    })),
  };
}

export function serializeResultPayload(value) {
  return buildWorkflowArguments({ result: value });
}

export function serializeErrorPayload(error) {
  const payload =
    error && typeof error === "object"
      ? {
          type: error.name || "Error",
          module: error.constructor?.name || "Error",
          message: error.message || String(error),
          traceback: error.stack || "",
          values: buildExceptionValues(error),
          type_hierarchy: buildTypeHierarchy(error),
        }
      : {
          type: "Error",
          module: "Error",
          message: String(error),
          traceback: "",
          values: { entries: [] },
          type_hierarchy: ["Error"],
        };
  return {
    arguments: [
      {
        key: "error",
        value: {
          exception: payload,
        },
      },
    ],
  };
}

export function deserializeResultPayload(argumentsPayload) {
  const values = deserializeWorkflowArguments(argumentsPayload);
  if (Object.prototype.hasOwnProperty.call(values, "error")) {
    const error = values.error;
    const errorValue =
      error && typeof error === "object"
        ? (error as Record<string, unknown>)
        : null;
    const message =
      errorValue && typeof errorValue.message === "string"
        ? errorValue.message
        : "workflow failed";
    const err = new Error(message);
    err.name =
      errorValue && typeof errorValue.type === "string"
        ? errorValue.type
        : "Error";
    err.stack =
      errorValue && typeof errorValue.traceback === "string"
        ? errorValue.traceback
        : err.stack;
    throw err;
  }

  if (!Object.prototype.hasOwnProperty.call(values, "result")) {
    throw new Error("result payload missing 'result' field");
  }

  return values.result;
}

export function deserializeWorkflowArguments(
  argumentsPayload
): Record<string, unknown> {
  const entries =
    argumentsPayload?.arguments ?? argumentsPayload?.argumentsList ?? [];
  if (!Array.isArray(entries)) {
    return {};
  }

  const output: Record<string, unknown> = {};
  for (const entry of entries) {
    output[entry.key] = deserializeValue(entry.value);
  }
  return output;
}

export function serializeValue(value) {
  if (value === null || value === undefined) {
    return {
      primitive: {
        null_value: NULL_VALUE,
      },
    };
  }

  if (typeof value === "string") {
    return { primitive: { string_value: value } };
  }

  if (typeof value === "number") {
    if (Number.isInteger(value)) {
      return { primitive: { int_value: value } };
    }
    return { primitive: { double_value: value } };
  }

  if (typeof value === "boolean") {
    return { primitive: { bool_value: value } };
  }

  if (Array.isArray(value)) {
    return {
      list_value: {
        items: value.map(serializeValue),
      },
    };
  }

  if (isPlainObject(value)) {
    return {
      dict_value: {
        entries: Object.entries(value).map(([key, item]) => ({
          key,
          value: serializeValue(item),
        })),
      },
    };
  }

  throw new Error(`unsupported workflow argument type: ${typeof value}`);
}

export function deserializeValue(value) {
  if (!value || typeof value !== "object") {
    return null;
  }

  if (value.primitive) {
    const primitive = value.primitive;
    if (Object.prototype.hasOwnProperty.call(primitive, "string_value")) {
      return primitive.string_value;
    }
    if (Object.prototype.hasOwnProperty.call(primitive, "stringValue")) {
      return primitive.stringValue;
    }
    if (Object.prototype.hasOwnProperty.call(primitive, "double_value")) {
      return primitive.double_value;
    }
    if (Object.prototype.hasOwnProperty.call(primitive, "doubleValue")) {
      return primitive.doubleValue;
    }
    if (Object.prototype.hasOwnProperty.call(primitive, "int_value")) {
      return Number(primitive.int_value);
    }
    if (Object.prototype.hasOwnProperty.call(primitive, "intValue")) {
      return Number(primitive.intValue);
    }
    if (Object.prototype.hasOwnProperty.call(primitive, "bool_value")) {
      return Boolean(primitive.bool_value);
    }
    if (Object.prototype.hasOwnProperty.call(primitive, "boolValue")) {
      return Boolean(primitive.boolValue);
    }
    if (Object.prototype.hasOwnProperty.call(primitive, "null_value")) {
      return null;
    }
    if (Object.prototype.hasOwnProperty.call(primitive, "nullValue")) {
      return null;
    }
  }

  const listValue = value.list_value ?? value.listValue;
  if (listValue) {
    return (listValue.items ?? listValue.itemsList ?? []).map(deserializeValue);
  }

  const tupleValue = value.tuple_value ?? value.tupleValue;
  if (tupleValue) {
    return (tupleValue.items ?? tupleValue.itemsList ?? []).map(deserializeValue);
  }

  const dictValue = value.dict_value ?? value.dictValue;
  if (dictValue) {
    const obj = {};
    const entries = dictValue.entries ?? dictValue.entriesList ?? [];
    for (const entry of entries) {
      obj[entry.key] = deserializeValue(entry.value);
    }
    return obj;
  }

  if (value.exception) {
    return value.exception;
  }

  if (value.basemodel ?? value.baseModel) {
    const basemodel = value.basemodel ?? value.baseModel;
    const data = basemodel.data ?? basemodel.dataMap;
    return data
      ? deserializeValue({ dict_value: data })
      : {};
  }

  return null;
}

function buildExceptionValues(error) {
  const entries = [];
  for (const [key, value] of Object.entries(error)) {
    if (key === "message" || key === "name" || key === "stack") {
      continue;
    }
    if (value === undefined) {
      continue;
    }
    let serialized;
    try {
      serialized = serializeValue(value);
    } catch (err) {
      serialized = serializeValue(String(value));
    }
    entries.push({
      key,
      value: serialized,
    });
  }
  return { entries };
}

function buildTypeHierarchy(error) {
  const hierarchy = [];
  if (!error || typeof error !== "object") {
    return ["Error"];
  }
  let cursor = error;
  while (cursor) {
    const name = cursor.name || cursor.constructor?.name;
    if (name && name !== "Object" && !hierarchy.includes(name)) {
      hierarchy.push(name);
    }
    cursor = Object.getPrototypeOf(cursor);
  }
  if (!hierarchy.length) {
    hierarchy.push("Error");
  }
  return hierarchy;
}

function isPlainObject(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    Object.getPrototypeOf(value) === Object.prototype
  );
}
