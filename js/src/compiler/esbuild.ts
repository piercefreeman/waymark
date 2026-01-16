import fs from "node:fs/promises";
import path from "node:path";

import { compileSource } from "./index.js";

const JS_EXTENSIONS = new Set([
  ".js",
  ".jsx",
  ".mjs",
  ".cjs",
  ".ts",
  ".tsx",
  ".mts",
  ".cts",
]);

export function workflowPlugin(options: { mode?: "workflow" | "action" } = {}) {
  const mode = options.mode ?? "workflow";
  return {
    name: "rappel-workflow",
    setup(build) {
      build.onLoad({ filter: /\.[mc]?[jt]sx?$/ }, async (args) => {
        if (!JS_EXTENSIONS.has(path.extname(args.path))) {
          return null;
        }

        const source = await fs.readFile(args.path, "utf8");
        const result = await compileSource(source, args.path, { mode });

        return {
          contents: result.code,
          loader: guessLoader(args.path),
        };
      });
    },
  };
}

function guessLoader(filename) {
  const ext = path.extname(filename);
  if (ext === ".tsx") {
    return "tsx";
  }
  if (ext === ".ts" || ext === ".mts" || ext === ".cts") {
    return "ts";
  }
  if (ext === ".jsx") {
    return "jsx";
  }
  return "js";
}
