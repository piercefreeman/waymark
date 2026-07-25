import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { compileNextWorkflow } from "@waymark/nextjs";
import { build } from "esbuild";

const exampleRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
const generatedDirectory = path.join(exampleRoot, ".waymark");
const workflowPath = path.join(
  exampleRoot,
  "src",
  "waymark",
  "math.workflow.ts",
);

await mkdir(generatedDirectory, { recursive: true });

let compiled;
await build({
  absWorkingDir: exampleRoot,
  bundle: false,
  entryPoints: [workflowPath],
  logLevel: "silent",
  outdir: generatedDirectory,
  plugins: [
    {
      name: "waymark-workflow",
      setup(bundler) {
        bundler.onStart(async () => {
          compiled = await compileNextWorkflow({
            adapter: {
              async resolve(context, specifier) {
                const result = await bundler.resolve(specifier, {
                  kind: "import-statement",
                  resolveDir: context,
                });
                if (result.errors.length > 0 || result.path.length === 0) {
                  const details = result.errors
                    .map((error) => error.text)
                    .join("\n");
                  throw new Error(
                    `esbuild could not resolve ${specifier} from ${context}${details.length > 0 ? `:\n${details}` : ""}`,
                  );
                }
                return result.path;
              },
              readFile,
              async bundleEsm(entry) {
                const outputPath = path.join(
                  generatedDirectory,
                  "actions.mjs",
                );
                await build({
                  absWorkingDir: exampleRoot,
                  bundle: true,
                  format: "esm",
                  logLevel: "silent",
                  outfile: outputPath,
                  packages: "external",
                  platform: "node",
                  sourcemap: true,
                  stdin: {
                    contents: entry.source,
                    loader: "js",
                    resolveDir: path.dirname(entry.filePath),
                    sourcefile: entry.filePath,
                  },
                  target: "node20",
                });
                return { path: outputPath };
              },
            },
            filePath: workflowPath,
            projectRoot: exampleRoot,
            source: await readFile(workflowPath, "utf8"),
          });
        });
      },
    },
  ],
  write: false,
});

if (compiled === undefined) {
  throw new Error("Waymark workflow compilation did not run");
}

const workflow = compiled.workflow;
const generatedWorkflow = [
  'import { Buffer } from "node:buffer";',
  "",
  "export default {",
  `  actions: ${JSON.stringify(workflow.actions)},`,
  `  bytes: Buffer.from(${JSON.stringify(workflow.bytes.toString("base64"))}, "base64"),`,
  `  hash: ${JSON.stringify(workflow.hash)},`,
  `  inputName: ${JSON.stringify(workflow.inputName)},`,
  `  moduleId: ${JSON.stringify(workflow.moduleId)},`,
  "  program: undefined,",
  `  workflowName: ${JSON.stringify(workflow.workflowName)},`,
  "};",
  "",
].join("\n");
await writeFile(
  path.join(generatedDirectory, "workflow.mjs"),
  generatedWorkflow,
);

process.stdout.write(
  `Compiled ${workflow.workflowName} (${workflow.hash.slice(0, 12)})\n`,
);
