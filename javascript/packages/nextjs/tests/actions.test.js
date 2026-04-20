'use strict';

const messages = require('../src/generated/messages_pb.js');
const {
  __waymarkRegisterAction,
  executeActionDispatch,
  resetActionRegistry
} = require('../src/runtime/actions.js');
const {
  deserializeWorkflowResultPayload,
  serializeWorkflowArguments
} = require('../src/runtime/serialization.js');

describe('runtime action registry', () => {
  beforeEach(() => {
    resetActionRegistry();
  });

  test('executes a registered action from the in-memory bridge dispatcher', async () => {
    __waymarkRegisterAction('actions.ts', 'double', async (value) => value * 2);

    const dispatch = new messages.ActionDispatch();
    dispatch.setModuleName('actions.ts');
    dispatch.setActionName('double');
    dispatch.setKwargs(serializeWorkflowArguments({ value: 5 }));

    const result = await executeActionDispatch(dispatch);

    expect(result.success).toBe(true);
    expect(deserializeWorkflowResultPayload(result.payload.serializeBinary())).toBe(10);
  });

  test('returns a structured error when an action is missing from the registry', async () => {
    const dispatch = new messages.ActionDispatch();
    dispatch.setModuleName('missing.ts');
    dispatch.setActionName('double');

    const result = await executeActionDispatch(dispatch);
    const errorPayload = messages.WorkflowArguments.deserializeBinary(result.payload.serializeBinary());
    const errorEntry = errorPayload.getArgumentsList().find((entry) => entry.getKey() === 'error');

    expect(result.success).toBe(false);
    expect(result.errorMessage).toContain('is not registered');
    expect(errorEntry).toBeDefined();
  });
});
