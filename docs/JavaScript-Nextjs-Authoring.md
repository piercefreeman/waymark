# JavaScript / Next.js Authoring API

Waymark's first JavaScript target should be Next.js.

The goal is not a new runtime model. The goal is a new compiler front-end that lets users write workflows and actions in TypeScript or JavaScript, then lowers that source into the same Waymark IR and runtime behavior we already use for Python.

## Design goals

- Keep the same split as Python:
  - workflows define control flow
  - actions define distributed work
- Compile from raw source AST before Next.js finishes transforming the module.
- Treat authored workflow code as compile-time input, not runtime business logic.
- Emit rewritten JavaScript that embeds compiled Waymark IR and submits it to the live bridge on `run()`.
- Keep the authoring model close to normal server-side Next.js code.
- Stay intentionally ad hoc for the first pass. We do not need a language-neutral WASM compiler layer yet.

## Proposed user-facing API

### Actions

Actions are regular exported async functions, marked by a comment immediately above the declaration:

```ts
import { db } from "@/lib/db";
import { emailClient } from "@/lib/email";

// use action
export async function fetchUsers(userIds: string[]): Promise<User[]> {
  return await db.user.findMany({
    where: {
      id: {
        in: userIds,
      },
    },
  });
}

// use action
export async function sendWelcomeEmail(input: {
  to: string;
}): Promise<EmailResult> {
  return await emailClient.send({
    to: input.to,
    subject: "Welcome",
  });
}
```

Authoring rules for the first pass:

- The `// use action` comment must be directly above the exported function.
- Actions should be top-level named exports.
- Actions should be `async`.
- Actions should live in server-only modules.

We should start with named `async function` declarations for the MVP. Arrow functions, class methods, and re-exported aliases can come later if we want them.

### Workflows

Workflows are normal classes that extend a Waymark base class:

```ts
import { Workflow } from "@waymark/nextjs";

import { fetchUsers, sendWelcomeEmail } from "@/lib/actions/users";

export class WelcomeEmailWorkflow extends Workflow {
  async run(userIds: string[]): Promise<EmailResult[]> {
    const users = await fetchUsers(userIds);
    const activeUsers = users.filter((user) => user.active);

    return await Promise.all(
      activeUsers.map((user) =>
        this.runAction(sendWelcomeEmail({ to: user.email }), {
          retry: {
            attempts: 5,
            backoffSeconds: 30,
          },
          timeout: "10m",
        }),
      ),
    );
  }
}
```

This keeps the same mental model as Python:

- `run()` is where durable control flow is authored.
- Direct action calls inside `run()` are the intended default authoring shape.
- `this.runAction(...)` is the explicit form when a call needs retry or timeout metadata, and it is an acceptable fallback if the MVP needs stricter syntax before plain-call rewriting is complete.
- Plain language constructs like `if`, `for...of`, `while`, `try/catch`, local assignments, and `Promise.all(...)` remain the way users express control flow.

Workflow authoring rules for the first pass:

- A workflow must be an exported class extending `Workflow`.
- The durable entrypoint is `async run(...)`.
- Workflow inputs belong on `run(...)`, not on `constructor(...)`.
- The compiler only needs to understand `run(...)` for the MVP.

Good:

```ts
export class BillingWorkflow extends Workflow {
  async run(accountId: string): Promise<void> {
    const account = await fetchAccount(accountId);
    if (account.pastDue) {
      await notifyBilling({ accountId });
    }
  }
}
```

Bad:

```ts
export class BillingWorkflow extends Workflow {
  constructor(private accountId: string) {
    super();
  }

  async run(): Promise<void> {
    await notifyBilling({ accountId: this.accountId });
  }
}
```

### Starting a workflow from Next.js

The invocation site should stay normal and unsurprising:

```ts
import { WelcomeEmailWorkflow } from "@/lib/workflows/welcome-email";

export async function POST(request: Request): Promise<Response> {
  const { userIds } = await request.json();

  const workflow = new WelcomeEmailWorkflow();
  const result = await workflow.run(userIds);

  return Response.json({ result });
}
```

The authored call site stays `await workflow.run(...)`.

The important implementation detail is that the user-authored `run()` body is not what executes at runtime. The plugin should consume that body at build time, compile it to Waymark IR, embed the serialized IR into the emitted module, and replace `run()` with generated bridge submission code.

Conceptually, authored source:

```ts
export class WelcomeEmailWorkflow extends Workflow {
  async run(userIds: string[]): Promise<EmailResult[]> {
    const users = await fetchUsers(userIds);
    return await Promise.all(users.map((user) => sendWelcomeEmail({ to: user.email })));
  }
}
```

becomes emitted code in the shape of:

```ts
export class WelcomeEmailWorkflow extends Workflow {
  static __waymarkWorkflowName = "welcomeemailworkflow";
  static __waymarkWorkflowVersion = "sha256:...";
  static __waymarkProgram = "...serialized-ir...";

  async run(userIds: string[]): Promise<EmailResult[]> {
    return await __waymarkBridge.runWorkflow({
      workflowName: WelcomeEmailWorkflow.__waymarkWorkflowName,
      workflowVersion: WelcomeEmailWorkflow.__waymarkWorkflowVersion,
      program: WelcomeEmailWorkflow.__waymarkProgram,
      args: { userIds },
    });
  }
}
```

The exact helper names are internal. The contract is:

- author `run()` once in normal TypeScript
- consume that source during compilation
- emit a class whose runtime `run()` method queues the precompiled IR to Waymark
- never rely on re-executing the original control-flow body during durable workflow execution

## How the compiler should treat authored code

The JavaScript implementation should mirror the Python pipeline as closely as possible:

1. Read the raw module AST before comment information is lost.
2. Collect action definitions by finding `// use action` comments attached to exported async functions.
3. Collect workflow definitions by finding classes that extend `Workflow`.
4. Compile workflow `run()` bodies into Waymark IR using JavaScript syntax instead of Python syntax.
5. Serialize the compiled IR into a stable string or byte payload embedded in the emitted JavaScript module.
6. Replace workflow `run()` implementations with generated code that registers or submits that embedded IR to the live gRPC bridge and then waits for completion when requested.
7. Keep the original action exports available as normal server functions for worker-side execution and local testing; the workflow compiler only consumes their signatures and call sites when building IR.
8. Add significant Jest coverage for the compilation stage so different supported syntaxes produce the expected IR payload and runtime rewrite.

The core constraint is simpler than per-call stub execution: the original authored `run()` body should not be the runtime implementation after compilation. It is compiler input.

## What should feel the same as Python

- Actions are the retryable, distributed work units.
- Workflows are the durable control-flow layer.
- The workflow definition is compiled once, not re-run during recovery.
- Retry and timeout policy attach to action calls, not the workflow as a whole.
- Parallel fan-out should use normal language primitives. In JavaScript that means `Promise.all(...)`, not a Waymark-specific parallel API.

## gRPC transport and code generation

The JavaScript transport layer should follow the same philosophy as the Python SDK in this repo:

- use the checked-in protobuf definitions in `proto/messages.proto` and `proto/ast.proto`
- generate client bindings from those proto files using official gRPC/protobuf code generation
- do not rely on schema sniffing, reflection-only clients, or implicit runtime descriptor loading

Python already does this explicitly in [Makefile](/Users/piercefreeman/projects/waymark/Makefile:7) via `grpc_tools.protoc`, generating `messages_pb2.py`, `messages_pb2_grpc.py`, and related files before use. The JavaScript/Next.js SDK should adopt the same explicit build step with the official JS/TS gRPC toolchain.

That means the Next.js implementation should have a generated-client flow in the shape of:

1. Run official proto generation against `proto/messages.proto` and `proto/ast.proto`.
2. Emit versioned JS/TS protobuf message types and gRPC client stubs into the JS package.
3. Import those generated files from the rewritten workflow runtime.
4. Use those generated request/response types when `run()` submits the embedded IR to the bridge.

We should not build a dynamic client that infers request shapes from the schema at runtime. The build should fail fast if the proto contract changes and generated code is stale.

## Initial restrictions

For the first Next.js implementation, we should keep the surface tight:

- Server-side modules only. No Client Components.
- Top-level exported async functions only for actions.
- `// use action` only, directly above the declaration.
- Exported `class ... extends Workflow` only for workflows.
- `run(...)` is the only compiled workflow method.
- Inputs go through `run(...)`, not constructors.
- Imported helper functions are fine, but intra-class workflow helper methods can wait until later.

## Deferred questions

These do not need to block the first authoring surface:

- Whether the MVP ships plain `await actionFn(...)` immediately or temporarily requires `this.runAction(...)` everywhere until JavaScript-to-IR lowering is stable.
- Whether the action marker comment grows extra syntax like `// use action name=...`.
- Whether we later support additional JavaScript front-ends outside Next.js.
- How JavaScript worker bootstrapping is packaged once action execution moves beyond Python-only workers.
