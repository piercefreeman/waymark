const ACTION_METADATA_FIELD = "__rappelAction";

type ActionEntry = {
  id: string | null;
  moduleName: string;
  actionName: string;
  params: string[] | null;
  sourcePath: string | null;
  fn: ((...args: unknown[]) => unknown) | null;
};

class ActionRegistry {
  private _actions: Map<string, ActionEntry>;

  constructor() {
    this._actions = new Map();
  }

  register(definition, fn) {
    if (!definition || typeof definition !== "object") {
      throw new Error("action definition must be an object");
    }
    const moduleName = definition.moduleName;
    const actionName = definition.actionName;
    if (!moduleName || !actionName) {
      throw new Error("action definition requires moduleName and actionName");
    }
    const id = typeof definition.id === "string" ? definition.id : null;
    const key = `${moduleName}:${actionName}`;
    const existing = this._actions.get(key);
    if (existing && fn && existing.fn && existing.fn !== fn) {
      throw new Error(`action '${key}' already registered`);
    }
    const entry: ActionEntry = {
      id: id ?? existing?.id ?? null,
      moduleName,
      actionName,
      params: Array.isArray(definition.params) ? definition.params : null,
      sourcePath:
        typeof definition.sourcePath === "string" ? definition.sourcePath : null,
      fn: fn || existing?.fn || null,
    };
    this._actions.set(key, entry);
    if (fn) {
      attachActionMetadata(fn, entry);
    }
    return entry;
  }

  get(moduleName, actionName) {
    if (!moduleName || !actionName) {
      return null;
    }
    return this._actions.get(`${moduleName}:${actionName}`) ?? null;
  }

  list() {
    return Array.from(this._actions.values());
  }

  reset() {
    this._actions.clear();
  }
}

export const actionRegistry = new ActionRegistry();

export function registerAction(definition, fn) {
  return actionRegistry.register(definition, fn);
}

export function action(moduleName, actionName) {
  if (!moduleName || !actionName) {
    throw new Error("action requires moduleName and actionName");
  }

  const handler = async function actionProxy() {
    throw new Error(
      `action '${moduleName}:${actionName}' can only be invoked inside a workflow`
    );
  };

  registerAction({ moduleName, actionName }, handler);
  return handler;
}

export function actionProxy(moduleName, actionName, kwargs) {
  const proxy = globalThis.__rappelActionProxy;
  if (typeof proxy === "function") {
    return proxy({ moduleName, actionName, kwargs });
  }
  throw new Error(
    `action '${moduleName}:${actionName}' can only be executed by a worker`
  );
}

export function getActionMetadata(fn) {
  if (!fn || typeof fn !== "function") {
    return null;
  }
  return fn[ACTION_METADATA_FIELD] ?? null;
}

function attachActionMetadata(fn, definition) {
  Object.defineProperty(fn, ACTION_METADATA_FIELD, {
    value: {
      id: definition.id ?? null,
      moduleName: definition.moduleName,
      actionName: definition.actionName,
      params: definition.params,
      sourcePath: definition.sourcePath,
    },
    configurable: true,
  });
}
