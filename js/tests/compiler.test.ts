import { compileSource } from "../src/compiler/index.js";
import { loadAstRoot } from "../src/proto.js";

async function decodeProgram(base64) {
  const root = await loadAstRoot();
  const Program = root.lookupType("rappel.ast.Program");
  const decoded = Program.decode(Buffer.from(base64, "base64"));
  return Program.toObject(decoded, {
    longs: String,
    arrays: true,
  });
}

async function compileProgram(source) {
  const { workflows } = await compileSource(source, "example.ts", {
    mode: "workflow",
  });
  expect(workflows).toHaveLength(1);
  return decodeProgram(workflows[0].irBase64);
}

function getStatements(program) {
  const fn = program.functions?.[0];
  return fn?.body?.statements ?? [];
}

function findStatement(statements, key) {
  const match = statements.find((stmt) => stmt?.[key]);
  return match ? match[key] : null;
}

describe("JS workflow compiler", () => {
  test("injects workflow registration and rewrites action bodies", async () => {
    const source = `
      export async function addOne(x: number) {
        "use action";
        return x + 1;
      }

      export async function MyWorkflow(x: number) {
        "use workflow";
        const result = await addOne(x);
        return result;
      }
    `;

    const { code, workflows } = await compileSource(source, "example.ts", {
      mode: "workflow",
    });

    expect(code).toContain("__rappelRegisterWorkflow");
    expect(code).toContain("__rappelActionProxy");
    expect(code).toContain("/**__rappel_internal");
    expect(code).not.toContain("__rappelRegisterAction");
    expect(code).not.toContain("return x + 1");
    expect(workflows).toHaveLength(1);

    const program = await decodeProgram(workflows[0].irBase64);
    const statements = getStatements(program);
    const assignment = findStatement(statements, "assignment");
    expect(assignment?.value?.action_call?.action_name).toBe("addOne");
  });

  test("action mode registers actions without proxy stubs", async () => {
    const source = `
      export async function addOne(x: number) {
        "use action";
        return x + 1;
      }

      export async function MyWorkflow(x: number) {
        "use workflow";
        const result = await addOne(x);
        return result;
      }
    `;

    const { code } = await compileSource(source, "example.ts", {
      mode: "action",
    });

    expect(code).toContain("__rappelRegisterAction");
    expect(code).not.toContain("__rappelActionProxy");
    expect(code).toContain("return x + 1");
  });

  test("resolves action bindings declared via action()", async () => {
    const source = `
      export const external = action("pkg.actions", "doThing");

      export async function BindingWorkflow(input: string) {
        "use workflow";
        const result = await external({ input });
        return result;
      }
    `;

    const program = await compileProgram(source);
    const statements = getStatements(program);
    const assignment = findStatement(statements, "assignment");
    const actionCall = assignment?.value?.action_call;

    expect(actionCall?.action_name).toBe("doThing");
    expect(actionCall?.module_name).toBe("pkg.actions");
  });

  test("translates runAction metadata into policy brackets", async () => {
    const source = `
      export async function doThing(message: string) {
        "use action";
        return message;
      }

      export async function PolicyWorkflow(message: string) {
        "use workflow";
        const result = await runAction(doThing(message), {
          retry: { attempts: 2 }
        });
        return result;
      }
    `;

    const program = await compileProgram(source);
    const statements = getStatements(program);
    const assignment = findStatement(statements, "assignment");
    const policies = assignment?.value?.action_call?.policies ?? [];

    expect(policies).toHaveLength(1);
    expect(policies[0]?.retry?.max_retries).toBe(2);
  });

  test("compiles conditionals with else branches", async () => {
    const source = `
      export async function absValue(value: number) {
        "use action";
        return value;
      }

      export async function ConditionalWorkflow(value: number) {
        "use workflow";
        let result = 0;
        if (value > 0) {
          result = await absValue(value);
        } else {
          result = await absValue(-value);
        }
        return result;
      }
    `;

    const program = await compileProgram(source);
    const statements = getStatements(program);
    const conditional = findStatement(statements, "conditional");

    expect(conditional?.if_branch?.condition?.binary_op).toBeTruthy();
    expect(conditional?.else_branch).toBeTruthy();
  });

  test("compiles for-of loops with loop vars and iterable", async () => {
    const source = `
      export async function logItem(item: string) {
        "use action";
        return item;
      }

      export async function LoopWorkflow(items: string[]) {
        "use workflow";
        for (const item of items) {
          await logItem(item);
        }
        return items.length;
      }
    `;

    const program = await compileProgram(source);
    const statements = getStatements(program);
    const loop = findStatement(statements, "for_loop");

    expect(loop?.loop_vars).toEqual(["item"]);
    expect(loop?.iterable?.variable?.name).toBe("items");
  });

  test("captures try/catch exception variables", async () => {
    const source = `
      export async function riskyAction() {
        "use action";
        return "ok";
      }

      export async function TryWorkflow() {
        "use workflow";
        let result = "init";
        try {
          result = await riskyAction();
        } catch (err) {
          result = "failed";
        }
        return result;
      }
    `;

    const program = await compileProgram(source);
    const statements = getStatements(program);
    const tryExcept = findStatement(statements, "try_except");
    const handler = tryExcept?.handlers?.[0];

    expect(handler?.exception_var).toBe("err");
  });

  test("translates .length checks into len() calls", async () => {
    const source = `
      export async function fetchNotes(user: string) {
        "use action";
        return [];
      }

      export async function summarizeNotes(notes: string[]) {
        "use action";
        return "";
      }

      export async function GuardWorkflow(user: string) {
        "use workflow";
        const notes = await fetchNotes(user);
        let summary = "no notes";
        if (notes.length) {
          summary = await summarizeNotes(notes);
        }
        return summary;
      }
    `;

    const program = await compileProgram(source);
    const statements = getStatements(program);
    const conditional = findStatement(statements, "conditional");
    const condition = conditional?.if_branch?.condition;

    expect(condition?.function_call?.name).toBe("len");
  });
});
