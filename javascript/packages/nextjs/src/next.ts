import path from "node:path";

import {
  compileWorkflow,
  type CompiledWorkflow,
  type ModuleResolver,
} from "./compiler.js";
import { createActionManifestSource } from "./action-manifest.js";

export interface NextBuildAdapter {
  readonly bundleEsm: (entry: {
    readonly filePath: string;
    readonly source: string;
  }) => Promise<{ readonly path: string }>;
  readonly readFile: (filePath: string) => Promise<Buffer | string>;
  readonly resolve: (context: string, specifier: string) => Promise<string>;
}

export interface CompileNextWorkflowOptions {
  readonly adapter: NextBuildAdapter;
  readonly filePath: string;
  readonly projectRoot: string;
  readonly source: string;
  readonly workflowName?: string;
}

export interface CompiledNextWorkflow {
  readonly actionBundlePath: string;
  readonly workflow: CompiledWorkflow;
}

export function createNextModuleResolver(
  adapter: Pick<NextBuildAdapter, "readFile" | "resolve">,
  projectRoot: string,
): ModuleResolver {
  const root = path.resolve(projectRoot);
  return async (specifier, importer) => {
    const resolvedPath = await adapter.resolve(path.dirname(importer), specifier);
    const relative = path.relative(root, resolvedPath);
    if (
      relative === ".." ||
      relative.startsWith(`..${path.sep}`) ||
      path.isAbsolute(relative)
    ) {
      return { external: true, path: resolvedPath };
    }
    const source = await adapter.readFile(resolvedPath);
    return { path: resolvedPath, source: source.toString() };
  };
}

export async function compileNextWorkflow(
  options: CompileNextWorkflowOptions,
): Promise<CompiledNextWorkflow> {
  const workflow = await compileWorkflow({
    filePath: options.filePath,
    projectRoot: options.projectRoot,
    resolveModule: createNextModuleResolver(
      options.adapter,
      options.projectRoot,
    ),
    source: options.source,
    workflowName: options.workflowName,
  });
  const bundle = await options.adapter.bundleEsm({
    filePath: path.join(
      options.projectRoot,
      ".waymark",
      `${workflow.workflowName}.actions.mjs`,
    ),
    source: createActionManifestSource(workflow.actions),
  });
  return { actionBundlePath: bundle.path, workflow };
}
