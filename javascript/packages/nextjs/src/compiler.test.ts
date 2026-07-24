import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import { compileWorkflow, WorkflowCompileError } from "./compiler.js";
import { ActionRuntime } from "./internal/proto/action.js";
import {
  type ActionCall,
  Program,
  type Statement,
} from "./internal/proto/ast.js";

const projectRoot = "/workspace";
const workflowPath = "/workspace/src/workflow.ts";
const actionPath = "/workspace/src/actions.ts";

const actionsSource = `
import { action as defineAction } from "@waymark/nextjs";

export const getCustomer = defineAction(async function getCustomer(
  input: { customerId: string },
) {
  return { id: input.customerId, active: true };
});

export const sendWelcomeEmail = defineAction(async function sendWelcomeEmail(
  customer: { id: string },
) {
  return { status: "sent", id: customer.id };
});

export const recordAudit = defineAction(async function recordAudit(
  customer: { id: string },
) {
  return customer.id;
});
`;

const workflowSource = `
import { Workflow as BaseWorkflow } from "@waymark/nextjs";
import { getCustomer, recordAudit, sendWelcomeEmail } from "./actions";

export class OnboardCustomer extends BaseWorkflow<
  { customerId: string; customerIds: string[] },
  { status: string }
> {
  async run(input: { customerId: string; customerIds: string[] }) {
    let attempts = 0;
    const customer = await getCustomer({ customerId: input.customerId });
    const [audit, welcome] = await Promise.all([
      recordAudit(customer),
      sendWelcomeEmail(customer),
    ]);
    const related = await Promise.all(
      input.customerIds.map(customerId => getCustomer({ customerId })),
    );

    while (attempts < 1) {
      attempts = attempts + 1;
    }

    try {
      if (customer.active === false) {
        return { status: "skipped", audit };
      }
    } catch (error) {
      return { status: "error" };
    }

    return await this.runAction(sendWelcomeEmail(customer), {
      retry: { attempts: 3, backoffSeconds: 5 },
      timeout: "30s",
    });
  }
}
`;

function collectActionCalls(statements: readonly Statement[]): ActionCall[] {
  const calls: ActionCall[] = [];
  for (const statement of statements) {
    switch (statement.kind?.$case) {
      case "assignment": {
        const expression = statement.kind.value.value;
        if (expression?.kind?.$case === "actionCall") {
          calls.push(expression.kind.value);
        } else if (expression?.kind?.$case === "parallelExpr") {
          for (const call of expression.kind.value.calls) {
            if (call.kind?.$case === "action") {
              calls.push(call.kind.value);
            }
          }
        } else if (expression?.kind?.$case === "spreadExpr") {
          if (expression.kind.value.action !== undefined) {
            calls.push(expression.kind.value.action);
          }
        }
        break;
      }
      case "actionCall":
        calls.push(statement.kind.value);
        break;
      case "returnStmt":
        if (statement.kind.value.value?.kind?.$case === "actionCall") {
          calls.push(statement.kind.value.value.kind.value);
        }
        break;
      case "conditional":
        if (statement.kind.value.ifBranch?.blockBody !== undefined) {
          calls.push(
            ...collectActionCalls(
              statement.kind.value.ifBranch.blockBody.statements,
            ),
          );
        }
        if (statement.kind.value.elseBranch?.blockBody !== undefined) {
          calls.push(
            ...collectActionCalls(
              statement.kind.value.elseBranch.blockBody.statements,
            ),
          );
        }
        break;
      case "tryExcept":
        if (statement.kind.value.tryBlock !== undefined) {
          calls.push(
            ...collectActionCalls(statement.kind.value.tryBlock.statements),
          );
        }
        for (const handler of statement.kind.value.handlers) {
          if (handler.blockBody !== undefined) {
            calls.push(...collectActionCalls(handler.blockBody.statements));
          }
        }
        break;
    }
  }
  return calls;
}

test("compiler lowers a full TypeScript workflow through the resolver boundary", async () => {
  const resolutions: Array<[string, string]> = [];
  const compiled = await compileWorkflow({
    filePath: workflowPath,
    projectRoot,
    source: workflowSource,
    resolveModule(specifier, importer) {
      resolutions.push([specifier, importer]);
      return { path: actionPath, source: actionsSource };
    },
  });

  assert.deepEqual(resolutions, [["./actions", workflowPath]]);
  assert.equal(compiled.workflowName, "OnboardCustomer");
  assert.equal(compiled.moduleId, "src/workflow.ts");
  assert.deepEqual(Program.decode(compiled.bytes), compiled.program);
  assert.equal(
    compiled.hash,
    createHash("sha256").update(compiled.bytes).digest("hex"),
  );

  const main = compiled.program.functions[0];
  assert.deepEqual(main?.io?.inputs, ["input"]);
  assert.ok(main?.span);
  assert.ok(main.body?.span);
  assert.ok(main.body?.statements.every((statement) => statement.span !== undefined));

  const calls = collectActionCalls(main?.body?.statements ?? []);
  assert.equal(calls.length, 5);
  assert.ok(
    calls.every(
      (call) =>
        call.runtime === ActionRuntime.ACTION_RUNTIME_JAVASCRIPT &&
        call.moduleName === "src/actions.ts",
    ),
  );
  const policyCall = calls.find((call) => call.policies.length === 2);
  const retry = policyCall?.policies.find(
    (policy) => policy.kind?.$case === "retry",
  );
  const timeout = policyCall?.policies.find(
    (policy) => policy.kind?.$case === "timeout",
  );
  assert.equal(
    retry?.kind?.$case === "retry" ? retry.kind.value.maxRetries : undefined,
    2,
  );
  assert.equal(
    timeout?.kind?.$case === "timeout"
      ? timeout.kind.value.timeout?.seconds
      : undefined,
    30n,
  );
});

test("compiler reports unsupported syntax with a source code frame", async () => {
  const source = `
import { Workflow } from "@waymark/nextjs";

export class InvalidWorkflow extends Workflow<{ active: boolean }, string> {
  async run(input: { active: boolean }) {
    if (input.active) {
      return "active";
    }
    return "inactive";
  }
}
`;

  await assert.rejects(
    compileWorkflow({
      filePath: workflowPath,
      projectRoot,
      source,
      resolveModule() {
        throw new Error("unexpected resolver call");
      },
    }),
    (error: unknown) => {
      assert.ok(error instanceof WorkflowCompileError);
      assert.match(error.message, /implicit truthiness is unsupported/);
      assert.match(error.message, /if \(input\.active\)/);
      assert.match(error.message, /\^/);
      assert.deepEqual(error.span, {
        startLine: 6,
        startCol: 8,
        endLine: 6,
        endCol: 20,
      });
      return true;
    },
  );
});

test("compiler rejects comment-era or arrow action declarations", async () => {
  const invalidActions = `
import { action } from "@waymark/nextjs";
export const getCustomer = action(async (input: { customerId: string }) => input);
`;
  const source = `
import { Workflow } from "@waymark/nextjs";
import { getCustomer } from "./actions";
export class InvalidActionWorkflow extends Workflow<{ customerId: string }, unknown> {
  async run(input: { customerId: string }) {
    return await getCustomer(input);
  }
}
`;

  await assert.rejects(
    compileWorkflow({
      filePath: workflowPath,
      projectRoot,
      source,
      resolveModule() {
        return { path: actionPath, source: invalidActions };
      },
    }),
    /actions must use `export const name = action\(async function name/,
  );
});

test("compiler rejects Next.js client and Edge modules", async () => {
  for (const source of [
    `"use client";\n${workflowSource}`,
    `export const runtime = "edge";\n${workflowSource}`,
  ]) {
    await assert.rejects(
      compileWorkflow({
        filePath: workflowPath,
        projectRoot,
        source,
        resolveModule() {
          return { path: actionPath, source: actionsSource };
        },
      }),
      /server modules|Node\.js runtime/,
    );
  }
});
