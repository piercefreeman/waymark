'use strict';

const { deserializeWorkflowArguments, serializeErrorPayload, serializeResultPayload } = require('./serialization.js');

const actionRegistry = new Map();

function __waymarkRegisterAction(moduleName, actionName, fn) {
  actionRegistry.set(actionKey(moduleName, actionName), fn);
}

async function executeActionDispatch(dispatch) {
  const moduleName = dispatch.getModuleName();
  const actionName = dispatch.getActionName();

  try {
    const action = await resolveAction(moduleName, actionName);
    const kwargs = dispatch.hasKwargs() ? deserializeWorkflowArguments(dispatch.getKwargs()) : {};
    const result = await action(...mapKwargsToArgs(action, kwargs));
    return {
      errorMessage: '',
      errorType: '',
      payload: serializeResultPayload(result),
      success: true
    };
  } catch (error) {
    return {
      errorMessage: error instanceof Error ? error.message : String(error),
      errorType: error instanceof Error && error.name ? error.name : 'Error',
      payload: serializeErrorPayload(error),
      success: false
    };
  }
}

async function resolveAction(moduleName, actionName) {
  const existing = actionRegistry.get(actionKey(moduleName, actionName));
  if (existing) {
    return existing;
  }

  throw new Error(
    `Action ${actionName} from module ${moduleName} is not registered. ` +
      'Ensure the module was transformed by the Waymark plugin and that .waymark/actions-bootstrap.mjs was loaded before running workflows.'
  );
}

function mapKwargsToArgs(action, kwargs) {
  const source = action.toString();
  const groupedParameterSource = source.match(/^[^(]*\(([^)]*)\)/);
  const singleParameterSource = source.match(/^(?:async\s*)?([A-Za-z_$][\w$]*)\s*=>/);

  if (!groupedParameterSource && !singleParameterSource) {
    return [];
  }

  const names = groupedParameterSource
    ? groupedParameterSource[1]
      .split(',')
      .map((entry) => entry.trim())
      .filter(Boolean)
      .map((entry) => entry.replace(/=.*$/, '').trim())
    : [singleParameterSource[1]];

  return names.map((name) => kwargs[name]);
}

function actionKey(moduleName, actionName) {
  return `${moduleName}:${actionName}`;
}

function resetActionRegistry() {
  actionRegistry.clear();
}

module.exports = {
  __waymarkRegisterAction,
  executeActionDispatch,
  resetActionRegistry
};
