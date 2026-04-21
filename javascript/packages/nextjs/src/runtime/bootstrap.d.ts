export type BootstrapOptions = {
  bootstrapPath?: string;
  cwd?: string;
  env?: Record<string, string | undefined>;
};

export declare const BOOTSTRAP_ENV_VAR: string;

export declare const BOOTSTRAP_RELATIVE_PATH: string;

export declare function findBootstrapPath(startDirectory: string): string | null;

export declare function loadBootstrap(options?: BootstrapOptions): Promise<string>;

export declare function projectRootForBootstrap(bootstrapPath: string): string;

export declare function registerProjectModuleHooks(projectRoot: string): void;

export declare function resolveBootstrapPath(options?: BootstrapOptions): string;
