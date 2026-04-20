export type TransformResult = {
  code: string;
  dependencies?: string[];
  transformed: boolean;
  workflows?: Array<{
    className: string;
    concurrent: boolean;
    inputNames: string[];
    irHash: string;
    programBase64: string;
    workflowName: string;
    workflowVersion: string;
  }>;
};

export type TransformOptions = {
  projectRoot?: string;
  resourcePath?: string;
};

export type WaymarkPluginOptions = {
  projectRoot?: string;
};

export declare class Workflow {
  run(...args: unknown[]): Promise<unknown>;
  runAction<T>(awaitable: PromiseLike<T>, options?: unknown): Promise<T>;
}

export declare function __waymarkRunCompiled(
  workflowCtor: typeof Workflow,
  args: unknown[]
): Promise<unknown>;

export declare function __waymarkRegisterAction(
  moduleName: string,
  actionName: string,
  fn: (...args: unknown[]) => unknown
): void;

export declare function withWaymark<T extends Record<string, unknown>>(
  nextConfig?: T,
  pluginOptions?: WaymarkPluginOptions
): T;

export declare const __internal: {
  transformSource(source: string, options?: TransformOptions): TransformResult;
};
