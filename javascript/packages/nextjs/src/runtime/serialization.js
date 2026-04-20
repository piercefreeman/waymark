'use strict';

const messages = require('../generated/messages_pb.js');

function serializeWorkflowArguments(values) {
  const args = new messages.WorkflowArguments();
  for (const [key, value] of Object.entries(values)) {
    const entry = new messages.WorkflowArgument();
    entry.setKey(key);
    entry.setValue(serializeWorkflowArgumentValue(value));
    args.addArguments(entry);
  }
  return args;
}

function serializeWorkflowArgumentValue(value) {
  const argument = new messages.WorkflowArgumentValue();

  if (value === null || value === undefined) {
    const primitive = new messages.PrimitiveWorkflowArgument();
    primitive.setNullValue(0);
    argument.setPrimitive(primitive);
    return argument;
  }

  if (typeof value === 'string') {
    const primitive = new messages.PrimitiveWorkflowArgument();
    primitive.setStringValue(value);
    argument.setPrimitive(primitive);
    return argument;
  }

  if (typeof value === 'number') {
    const primitive = new messages.PrimitiveWorkflowArgument();
    if (Number.isInteger(value)) {
      primitive.setIntValue(value);
    } else {
      primitive.setDoubleValue(value);
    }
    argument.setPrimitive(primitive);
    return argument;
  }

  if (typeof value === 'boolean') {
    const primitive = new messages.PrimitiveWorkflowArgument();
    primitive.setBoolValue(value);
    argument.setPrimitive(primitive);
    return argument;
  }

  if (Array.isArray(value)) {
    const listValue = new messages.WorkflowListArgument();
    for (const item of value) {
      listValue.addItems(serializeWorkflowArgumentValue(item));
    }
    argument.setListValue(listValue);
    return argument;
  }

  if (isPlainObject(value)) {
    const dictValue = new messages.WorkflowDictArgument();
    for (const [entryKey, entryValue] of Object.entries(value)) {
      const entry = new messages.WorkflowArgument();
      entry.setKey(entryKey);
      entry.setValue(serializeWorkflowArgumentValue(entryValue));
      dictValue.addEntries(entry);
    }
    argument.setDictValue(dictValue);
    return argument;
  }

  throw new TypeError(`Unsupported workflow argument type: ${typeof value}`);
}

function deserializeWorkflowResultPayload(bytes) {
  const argumentsMessage = messages.WorkflowArguments.deserializeBinary(bytes);
  const entries = Object.create(null);

  for (const entry of argumentsMessage.getArgumentsList()) {
    entries[entry.getKey()] = deserializeWorkflowArgumentValue(entry.getValue());
  }

  if (Object.prototype.hasOwnProperty.call(entries, 'error')) {
    const errorValue = entries.error;
    const message = errorValue && typeof errorValue === 'object' && errorValue.message
      ? errorValue.message
      : 'workflow failed';
    const error = new Error(message);
    error.waymark = errorValue;
    throw error;
  }

  return entries.result;
}

function deserializeWorkflowArgumentValue(argument) {
  const kind = argument.getKindCase();

  switch (kind) {
    case messages.WorkflowArgumentValue.KindCase.PRIMITIVE:
      return deserializePrimitive(argument.getPrimitive());
    case messages.WorkflowArgumentValue.KindCase.LIST_VALUE:
      return argument.getListValue().getItemsList().map(deserializeWorkflowArgumentValue);
    case messages.WorkflowArgumentValue.KindCase.TUPLE_VALUE:
      return argument.getTupleValue().getItemsList().map(deserializeWorkflowArgumentValue);
    case messages.WorkflowArgumentValue.KindCase.DICT_VALUE: {
      const result = {};
      for (const entry of argument.getDictValue().getEntriesList()) {
        result[entry.getKey()] = deserializeWorkflowArgumentValue(entry.getValue());
      }
      return result;
    }
    case messages.WorkflowArgumentValue.KindCase.EXCEPTION: {
      const exception = argument.getException();
      return {
        type: exception.getType(),
        module: exception.getModule(),
        message: exception.getMessage(),
        traceback: exception.getTraceback()
      };
    }
    case messages.WorkflowArgumentValue.KindCase.BASEMODEL: {
      const model = argument.getBasemodel();
      const data = {};
      for (const entry of model.getData().getEntriesList()) {
        data[entry.getKey()] = deserializeWorkflowArgumentValue(entry.getValue());
      }
      return {
        __type: 'basemodel',
        module: model.getModule(),
        name: model.getName(),
        data
      };
    }
    case messages.WorkflowArgumentValue.KindCase.KIND_NOT_SET:
      return undefined;
    default:
      throw new Error(`Unsupported workflow argument kind: ${kind}`);
  }
}

function deserializePrimitive(primitive) {
  const kind = primitive.getKindCase();

  switch (kind) {
    case messages.PrimitiveWorkflowArgument.KindCase.STRING_VALUE:
      return primitive.getStringValue();
    case messages.PrimitiveWorkflowArgument.KindCase.DOUBLE_VALUE:
      return primitive.getDoubleValue();
    case messages.PrimitiveWorkflowArgument.KindCase.INT_VALUE:
      return primitive.getIntValue();
    case messages.PrimitiveWorkflowArgument.KindCase.BOOL_VALUE:
      return primitive.getBoolValue();
    case messages.PrimitiveWorkflowArgument.KindCase.NULL_VALUE:
      return null;
    case messages.PrimitiveWorkflowArgument.KindCase.KIND_NOT_SET:
      return undefined;
    default:
      throw new Error(`Unsupported primitive kind: ${kind}`);
  }
}

function isPlainObject(value) {
  return Object.prototype.toString.call(value) === '[object Object]';
}

module.exports = {
  deserializeWorkflowResultPayload,
  serializeWorkflowArguments,
  serializeWorkflowArgumentValue
};
