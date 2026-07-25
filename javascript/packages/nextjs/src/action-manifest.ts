import path from "node:path";

import { isAction, type Action } from "./action.js";
import type { CompiledActionReference } from "./compiler.js";

export interface ActionManifestEntry extends CompiledActionReference {
  readonly implementation: Action<readonly any[], any>;
}

export class ActionManifest {
  readonly #entries: ReadonlyMap<string, ActionManifestEntry>;

  constructor(entries: readonly ActionManifestEntry[]) {
    const indexed = new Map<string, ActionManifestEntry>();
    for (const entry of entries) {
      const key = actionManifestKey(entry.moduleName, entry.actionName);
      if (indexed.has(key)) {
        throw new Error(`duplicate action manifest entry ${key}`);
      }
      if (!isAction(entry.implementation)) {
        throw new TypeError(`${key} was not declared with action(...)`);
      }
      indexed.set(key, entry);
    }
    this.#entries = indexed;
  }

  get(moduleName: string, actionName: string): ActionManifestEntry | undefined {
    return this.#entries.get(actionManifestKey(moduleName, actionName));
  }
}

export function actionManifestKey(
  moduleName: string,
  actionName: string,
): string {
  return `${moduleName}:${actionName}`;
}

export function createActionManifest(
  entries: readonly ActionManifestEntry[],
): ActionManifest {
  return new ActionManifest(entries);
}

export function createActionManifestSource(
  actions: readonly CompiledActionReference[],
  entryModuleId = "actions.mjs",
): string {
  const ordered = [...actions].sort((left, right) =>
    actionManifestKey(left.moduleName, left.actionName).localeCompare(
      actionManifestKey(right.moduleName, right.actionName),
    ),
  );
  const imports = ordered.map((entry, index) => {
    const relative = path.posix.relative(
      path.posix.dirname(entryModuleId),
      entry.moduleName,
    );
    const specifier = relative.startsWith(".") ? relative : `./${relative}`;
    return `import { ${entry.actionName} as action${index} } from ${JSON.stringify(specifier)};`;
  });
  const entries = ordered.map(
    (entry, index) =>
      `  { actionName: ${JSON.stringify(entry.actionName)}, moduleName: ${JSON.stringify(entry.moduleName)}, parameterNames: ${JSON.stringify(entry.parameterNames)}, implementation: action${index} },`,
  );
  return [
    'import { createActionManifest } from "@waymark/nextjs";',
    ...imports,
    "",
    "export default createActionManifest([",
    ...entries,
    "]);",
    "",
  ].join("\n");
}
