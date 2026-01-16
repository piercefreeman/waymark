import crypto from "node:crypto";

const WORKFLOW_ID_FIELD = "__rappelWorkflowId";
const WORKFLOW_SHORT_NAME_FIELD = "__rappelWorkflowShortName";

type WorkflowEntry = {
  id: string | null;
  shortName: string;
  ir: Buffer;
  irHash: string;
  sourcePath: string | null;
  concurrent: boolean;
};

export class WorkflowRegistry {
  private _byShortName: Map<string, WorkflowEntry>;
  private _byId: Map<string, WorkflowEntry>;

  constructor() {
    this._byShortName = new Map();
    this._byId = new Map();
  }

  register(definition, fn) {
    const normalized = normalizeDefinition(definition);
    const existing = this._byShortName.get(normalized.shortName);

    if (existing && existing.irHash !== normalized.irHash) {
      throw new Error(`workflow '${normalized.shortName}' already registered`);
    }

    this._byShortName.set(normalized.shortName, normalized);
    if (normalized.id) {
      this._byId.set(normalized.id, normalized);
    }

    if (fn) {
      attachWorkflowMetadata(fn, normalized);
    }

    return normalized;
  }

  get(nameOrId) {
    if (!nameOrId) {
      return null;
    }

    if (this._byShortName.has(nameOrId)) {
      return this._byShortName.get(nameOrId) ?? null;
    }

    if (this._byId.has(nameOrId)) {
      return this._byId.get(nameOrId) ?? null;
    }

    return null;
  }

  list() {
    return Array.from(this._byShortName.values());
  }

  reset() {
    this._byShortName.clear();
    this._byId.clear();
  }
}

export const workflowRegistry = new WorkflowRegistry();

export function registerWorkflow(definition, fn) {
  return workflowRegistry.register(definition, fn);
}

export function resolveWorkflow(input) {
  if (!input) {
    return null;
  }

  if (typeof input === "string") {
    const direct = workflowRegistry.get(input);
    if (direct) {
      return direct;
    }
    const lowered = input.toLowerCase();
    if (lowered !== input) {
      return workflowRegistry.get(lowered);
    }
    return null;
  }

  if (typeof input === "function") {
    const id = input[WORKFLOW_ID_FIELD];
    if (id) {
      return workflowRegistry.get(id);
    }
    const shortName = input[WORKFLOW_SHORT_NAME_FIELD];
    if (shortName) {
      return workflowRegistry.get(shortName);
    }
    return null;
  }

  return null;
}

function normalizeDefinition(definition) {
  if (!definition || typeof definition !== "object") {
    throw new Error("workflow definition must be an object");
  }

  const shortName = definition.shortName;
  if (!shortName || typeof shortName !== "string") {
    throw new Error("workflow shortName must be a non-empty string");
  }

  const id = typeof definition.id === "string" ? definition.id : null;

  const irBytes = normalizeIr(definition.ir);
  const irHash = definition.irHash || hashIr(irBytes);

  return {
    id,
    shortName,
    ir: irBytes,
    irHash,
    sourcePath: typeof definition.sourcePath === "string" ? definition.sourcePath : null,
    concurrent: Boolean(definition.concurrent),
  };
}

function normalizeIr(ir) {
  if (!ir) {
    throw new Error("workflow ir is required");
  }
  if (typeof ir === "string") {
    return Buffer.from(ir, "base64");
  }
  if (Buffer.isBuffer(ir)) {
    return ir;
  }
  if (ir instanceof Uint8Array) {
    return Buffer.from(ir);
  }
  throw new Error("workflow ir must be a base64 string or byte array");
}

function hashIr(bytes) {
  return crypto.createHash("sha256").update(bytes).digest("hex");
}

function attachWorkflowMetadata(fn, definition) {
  if (definition.id) {
    Object.defineProperty(fn, WORKFLOW_ID_FIELD, {
      value: definition.id,
      configurable: true,
    });
    Object.defineProperty(fn, "workflowId", {
      value: definition.id,
      configurable: true,
    });
  }

  Object.defineProperty(fn, WORKFLOW_SHORT_NAME_FIELD, {
    value: definition.shortName,
    configurable: true,
  });
}
