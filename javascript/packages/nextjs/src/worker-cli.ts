#!/usr/bin/env node

import path from "node:path";
import { parseArgs } from "node:util";

import { loadActionManifest, runNodeWorker } from "./worker.js";

const { values } = parseArgs({
  options: {
    bridge: { type: "string" },
    bundle: { type: "string" },
    "worker-id": { type: "string" },
  },
});
if (
  values.bridge === undefined ||
  values.bundle === undefined ||
  values["worker-id"] === undefined
) {
  throw new Error(
    "usage: waymark-worker-node --bridge HOST:PORT --worker-id ID --bundle FILE",
  );
}

const controller = new AbortController();
process.once("SIGINT", () => controller.abort());
process.once("SIGTERM", () => controller.abort());

const manifest = await loadActionManifest(path.resolve(values.bundle));
await runNodeWorker({
  bridge: values.bridge,
  manifest,
  signal: controller.signal,
  workerId: BigInt(values["worker-id"]),
});
