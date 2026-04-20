import type { BootstrapOptions } from './bootstrap.js';

export type WorkerRunOptions = BootstrapOptions & {
  bridge: string;
  client?: unknown;
  workerId: number;
};

export declare function createAckEnvelope(envelope: unknown): unknown;

export declare function createActionResultEnvelope(
  envelope: unknown,
  dispatch: unknown,
  execution: {
    errorMessage?: string;
    errorType?: string;
    payload: unknown;
    success: boolean;
  },
  workerStartNs: number,
  workerEndNs: number
): unknown;

export declare function createEnvelopeWriter(stream: {
  write(envelope: unknown, callback: (error?: Error | null) => void): void;
}): (envelope: unknown) => Promise<void>;

export declare function createHelloEnvelope(workerId: number): unknown;

export declare function handleDispatchEnvelope(
  envelope: unknown,
  writeEnvelope: (envelope: unknown) => Promise<void>
): Promise<void>;

export declare function handleIncomingEnvelope(
  envelope: unknown,
  writeEnvelope: (envelope: unknown) => Promise<void>
): Promise<void>;

export declare function main(argv?: string[]): Promise<void>;

export declare function parseWorkerArgs(argv: string[]): {
  bootstrapPath?: string;
  bridge?: string;
  help?: boolean;
  workerId?: number;
};

export declare function runWorker(options: WorkerRunOptions): Promise<{
  bootstrapPath: string;
}>;

export declare function workerUsage(): string;
