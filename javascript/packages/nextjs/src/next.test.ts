import assert from "node:assert/strict";
import test from "node:test";

import { compileNextWorkflow } from "./next.js";

test("Next adapter delegates resolution and ESM bundling to the bundler", async () => {
  const workflowPath = "/workspace/app/onboard.workflow.ts";
  const actionPath = "/workspace/app/actions.ts";
  const resolutions: Array<[string, string]> = [];
  const bundles: Array<{ filePath: string; source: string }> = [];

  const compiled = await compileNextWorkflow({
    adapter: {
      async resolve(context, specifier) {
        resolutions.push([context, specifier]);
        return actionPath;
      },
      async readFile(filePath) {
        assert.equal(filePath, actionPath);
        return `
          import { action } from "@waymark/nextjs";
          export const greet = action(async function greet(name: string) {
            return "Hello " + name;
          });
        `;
      },
      async bundleEsm(entry) {
        bundles.push(entry);
        return { path: "/workspace/.next/waymark/onboard.actions.mjs" };
      },
    },
    filePath: workflowPath,
    projectRoot: "/workspace",
    source: `
      import { Workflow } from "@waymark/nextjs";
      import { greet } from "./actions";
      export class Onboard extends Workflow<string, string> {
        async run(name: string) {
          return await greet(name);
        }
      }
    `,
  });

  assert.deepEqual(resolutions, [["/workspace/app", "./actions"]]);
  assert.equal(
    compiled.actionBundlePath,
    "/workspace/.next/waymark/onboard.actions.mjs",
  );
  assert.equal(bundles.length, 1);
  assert.match(bundles[0]?.source ?? "", /from "\.\/app\/actions\.ts"/);
  assert.match(bundles[0]?.source ?? "", /createActionManifest/);
  assert.equal(
    bundles[0]?.filePath,
    "/workspace/.waymark/Onboard.actions.mjs",
  );
});
