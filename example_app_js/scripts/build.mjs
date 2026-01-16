import path from "node:path";
import { fileURLToPath } from "node:url";

import { build } from "esbuild";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(__dirname, "..");
const protoRoot = path.resolve(projectRoot, "..", "proto");

if (!process.env.RAPPEL_PROTO_ROOT) {
  process.env.RAPPEL_PROTO_ROOT = protoRoot;
}

const { workflowPlugin } = await import("@rappel/js/compiler/esbuild");

const externals = ["@rappel/js", "@rappel/js/*", "@grpc/grpc-js", "express", "pg"];

await build({
  entryPoints: [path.join(projectRoot, "src/server.ts")],
  outfile: path.join(projectRoot, "dist/server.js"),
  bundle: true,
  platform: "node",
  format: "esm",
  sourcemap: true,
  plugins: [workflowPlugin({ mode: "workflow" })],
  external: externals,
});

await build({
  entryPoints: [path.join(projectRoot, "src/worker.ts")],
  outfile: path.join(projectRoot, "dist/worker.js"),
  bundle: true,
  platform: "node",
  format: "esm",
  sourcemap: true,
  plugins: [workflowPlugin({ mode: "action" })],
  external: externals,
});
