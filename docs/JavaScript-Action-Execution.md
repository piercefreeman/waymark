# JavaScript Action Execution and Packaging

This document is the follow-on to [JavaScript-Nextjs-Authoring.md](/Users/piercefreeman/projects/waymark/docs/JavaScript-Nextjs-Authoring.md).

That first doc answers how users should author workflows and actions in JavaScript.

This doc answers the next set of questions:

- how we should test JavaScript actions actually running
- how we should eventually ship a JavaScript SDK with bundled Waymark binaries, analogous to the Python wheel
- how much of the JavaScript runtime environment we can auto-detect versus what should be explicit

## Short answer

- We should support two JavaScript action-execution modes:
  - in-memory action execution through the real bridge coordinator, mirroring Python test execution
  - out-of-process worker execution for parity with the Python worker model
- The JavaScript "wheel equivalent" should be an npm package family, not a single universal tarball:
  - a top-level SDK package
  - platform-specific packages that carry `waymark-bridge`, `waymark-boot-singleton`, and `waymark-start-workers`
  - a Node worker entrypoint such as `waymark-worker-node`
- We can auto-detect the JavaScript runtime executable in a constrained way.
- We should not rely on auto-detecting arbitrary user module paths or bundler semantics at runtime.
- For Next.js, the first generated artifact should be a bootstrap module, not a manifest.
- The worker runtime and test harness should load that bootstrap module to register actions.

## Goals

- Let JavaScript actions execute under a real Waymark worker model, not only inside a Next.js request process.
- Preserve the same bridge and protobuf contracts already used by Python.
- Keep authoring ergonomics simple:
  - `// use action`
  - `class ... extends Workflow`
  - `await workflow.run(...)`
- Avoid requiring users to manually type module paths in normal cases.
- Make local testing flow through the real bridge coordinator, not a coordinator bypass.

## Non-goals

- Do not solve every JavaScript runtime at once.
- Do not build a language-neutral worker runtime yet.
- Do not depend on reflection-only schema loading.
- Do not require the Rust side to understand framework-specific module resolution rules.

## The problem we actually have

Today we already have:

- JavaScript authoring
- AST lowering into Waymark IR
- rewritten `run()` methods that queue IR to the bridge
- an in-process registry that can execute action dispatches in the same Node process

That is enough for:

- compiler tests
- real coordinator-backed in-memory execution tests
- example app demos

That is not enough for:

- testing a real worker lifecycle
- exercising the same dispatch/result path that Python workers use
- shipping a framework-independent JavaScript action runtime

The missing pieces are:

- a stable bootstrap mechanism for registering JavaScript actions in a host Node process
- a Node worker runtime that can load that same bootstrap and serve action dispatches out-of-process

## Proposed model

### 1. Make in-memory action execution through the real coordinator the first-class test path

The important distinction is:

- we do want in-memory JavaScript action execution
- we do not want to bypass the coordinator

So the JavaScript equivalent of the Python test path should be:

- compile the workflow
- submit it through the real `ExecuteWorkflow` bridge path
- execute JavaScript actions in-memory inside the host Node process when the bridge dispatches them

This mode should be the default for:

- `javascript/packages/nextjs` unit tests that validate end-to-end execution
- Next.js example apps
- smoke tests that validate compile + queue + dispatch + result handling

Conceptually:

```text
host Node process
  -> rewritten run()
  -> bridge ExecuteWorkflow stream
  -> real coordinator / runloop in memory mode
  -> in-memory JavaScript action registry
  -> action function execution
```

This is the JavaScript equivalent of Python's in-memory execution path: the worker pool is not started, but the coordinator is still real.

### 2. Add a real Node worker runtime as the second test layer

For parity with Python, we should add a dedicated worker entrypoint:

- `waymark-worker-node`

Its job is the same as [python/src/waymark/worker.py](/Users/piercefreeman/projects/waymark/python/src/waymark/worker.py:1):

- connect to the Rust `WorkerBridge`
- preload user action modules
- receive `ActionDispatch`
- execute the matching action
- return `ActionResult`

Conceptually:

```text
Rust runloop
  -> WorkerBridge gRPC stream
  -> waymark-worker-node process
  -> user action module
```

This should be a Node-targeted runtime first. Bun and Deno can come later if we decide they matter.

## Module identity

The compiler already emits canonical module names in the shape of project-relative source paths:

- `lib/actions/math.ts`
- `lib/actions/users.ts`

We should keep that as the IR-level identity.

That gives us a stable key that:

- is independent of bundler chunk names
- is understandable in logs and debugging
- matches what the compiler actually sees

The runtime should not guess how `lib/actions/math.ts` maps to loadable code at worker startup.

## The bootstrap module should be the source of truth for initial Next.js support

For Next.js, a generated bootstrap module is a better first contract than a manifest.

Example:

- `.waymark/actions-bootstrap.mjs`

That module can be generated by the plugin/build and should do one thing:

- import every discovered action module so its `__waymarkRegisterAction(...)` side effects run

Conceptually:

```js
import "../lib/actions/users.ts";
```

If we need more structure later, we can evolve this into generated wrappers or a manifest. We do not need to start there.

The important property is simpler:

- the compiler/plugin owns a single bootstrap artifact
- tests and workers load that artifact once
- action registration then happens the same way in tests, example apps, and real workers

## How bootstrap loading should work

For Next.js specifically, we should not depend on reaching into `.next/server/...` internals as our long-term contract.

Instead, the plugin should emit a Waymark-owned bootstrap file in a stable location such as:

- `.waymark/actions-bootstrap.mjs`

That output should be:

- plain Node-loadable JavaScript
- generated from the action source modules
- stable across local dev and CI

This gives us a contract that is much easier to ship with Next.js:

- generate the bootstrap before or during `next build`
- keep it outside Next's private server bundle format
- let the runtime load that stable file directly

The important nuance is that the bootstrap is just the stable entrypoint. It does not need to be the full worker bundle.

For the first proposal:

- the generated file is `.mjs`
- it imports the authored action source modules by project-relative path
- the host JavaScript environment is responsible for being able to evaluate those source modules

That is already true for:

- Next.js server execution
- the coordinator-backed in-memory test harness we should add for this repo

For the future out-of-process worker, `waymark-worker-node` will need to own the same source-loading semantics instead of assuming plain `node` can import arbitrary `.ts` files by itself.

## Concrete Next.js example

This should use the exact same motivating example as the authoring doc.

### Authored files

Project layout:

```text
my-app/
  next.config.js
  app/
    api/
      welcome/
        route.ts
  lib/
    actions/
      users.ts
    workflows/
      welcome-email.ts
```

`next.config.js` in authored form:

```js
const { withWaymark } = require("@waymark/nextjs");

module.exports = withWaymark({
  reactStrictMode: true,
});
```

[lib/actions/users.ts](/Users/piercefreeman/projects/waymark/docs/JavaScript-Nextjs-Authoring.md:28) in authored form:

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

[lib/workflows/welcome-email.ts](/Users/piercefreeman/projects/waymark/docs/JavaScript-Nextjs-Authoring.md:64) in authored form:

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

`app/api/welcome/route.ts` in authored form:

```ts
import { WelcomeEmailWorkflow } from "@/lib/workflows/welcome-email";

export async function POST(request: Request): Promise<Response> {
  const { userIds } = await request.json();

  const workflow = new WelcomeEmailWorkflow();
  const result = await workflow.run(userIds);

  return Response.json({ result });
}
```

### Files produced on disk

For the first proposal, the project-local generated output should stay intentionally small:

```text
my-app/
  .waymark/
    actions-bootstrap.mjs
  next.config.js
  app/
    api/
      welcome/
        route.ts
  lib/
    actions/
      users.ts
    workflows/
      welcome-email.ts
```

The key point is:

- we do generate a stable `.mjs` file on disk
- we do not need a manifest for the first pass
- we do not need a second workflow definition file on disk for the first pass

The compiled workflow metadata still lives in the transformed workflow module, exactly as described in the first doc. The new on-disk artifact in this proposal is the bootstrap.

### What `withWaymark(...)` owns in this example

In this concrete example, `withWaymark(...)` is responsible for all Waymark-specific build work.

Given:

- `lib/actions/users.ts`
- `lib/workflows/welcome-email.ts`
- `app/api/welcome/route.ts`

`withWaymark(...)` should:

1. Attach the server-side source transform to Next.js.
2. Parse `lib/actions/users.ts` and detect:
   - `fetchUsers`
   - `sendWelcomeEmail`
3. Parse `lib/workflows/welcome-email.ts` and detect:
   - `WelcomeEmailWorkflow`
4. Rewrite `lib/actions/users.ts` so importing it registers both actions.
5. Rewrite `lib/workflows/welcome-email.ts` so `run(userIds)` submits compiled IR instead of executing the authored body directly.
6. Generate `.waymark/actions-bootstrap.mjs` containing the imports needed to register all discovered actions.

`withWaymark(...)` should not:

- boot `waymark-bridge`
- open gRPC connections
- execute `fetchUsers`
- execute `sendWelcomeEmail`
- start `waymark-worker-node`

Those are runtime concerns. `withWaymark(...)` is specifically the compile-time owner of:

- source discovery
- AST rewriting
- generated bootstrap maintenance
- rebuild invalidation when those source files change

### The generated `.mjs` file

`.waymark/actions-bootstrap.mjs` should look like:

```mjs
// generated by withWaymark(...)
// imports are project-relative from the bootstrap file

import "../lib/actions/users.ts";
```

That file is deliberately boring. Its only job is to make sure the action modules execute once so their registration side effects run.

### What the imported action module looks like after transform

The action source file itself stays user-authored, but the transformed module seen by the host runtime is conceptually:

```ts
import { __waymarkRegisterAction } from "@waymark/nextjs";
import { db } from "@/lib/db";
import { emailClient } from "@/lib/email";

export async function fetchUsers(userIds: string[]): Promise<User[]> {
  return await db.user.findMany({
    where: {
      id: {
        in: userIds,
      },
    },
  });
}

export async function sendWelcomeEmail(input: {
  to: string;
}): Promise<EmailResult> {
  return await emailClient.send({
    to: input.to,
    subject: "Welcome",
  });
}

__waymarkRegisterAction("lib/actions/users.ts", "fetchUsers", fetchUsers);
__waymarkRegisterAction("lib/actions/users.ts", "sendWelcomeEmail", sendWelcomeEmail);
```

That is why the bootstrap works: importing the module once is enough to populate the action registry with the canonical module id that the workflow IR already references.

### What does not need a new file

For this first proposal, the workflow side does not need a separate `.mjs` file on disk.

`lib/workflows/welcome-email.ts` is still compiled in-place by the plugin into a runtime shape like:

```ts
export class WelcomeEmailWorkflow extends Workflow {
  static __waymarkCompiledWorkflow = {
    workflowName: "welcomeemailworkflow",
    workflowVersion: "sha256:...",
    irHash: "sha256:...",
    programBase64: "...",
    inputNames: ["userIds"],
  };

  async run(userIds: string[]): Promise<EmailResult[]> {
    return await __waymarkRunCompiled(this.constructor, [userIds]);
  }
}
```

So the concrete proposal is:

- one generated bootstrap file on disk
- transformed action modules that self-register when imported
- transformed workflow modules that embed compiled IR

If you want the short version of the magic:

- `withWaymark(...)` makes the source tree Waymark-aware at build time
- `.waymark/actions-bootstrap.mjs` is the single explicit artifact it leaves behind
- everything else remains normal app source files that are transformed in memory by the Next.js build

### How the test path should look

A coordinator-backed in-memory JavaScript test should conceptually do:

```ts
import "../.waymark/actions-bootstrap.mjs";
import { WelcomeEmailWorkflow } from "../lib/workflows/welcome-email";

test("welcome workflow executes actions through the real coordinator", async () => {
  const workflow = new WelcomeEmailWorkflow();
  const result = await workflow.run(["user-1", "user-2"]);

  expect(result).toEqual([
    { ok: true, to: "a@example.com" },
    { ok: true, to: "b@example.com" },
  ]);
});
```

The important behavior is:

1. the bootstrap imports `lib/actions/users.ts`
2. `fetchUsers` and `sendWelcomeEmail` register under `lib/actions/users.ts`
3. `workflow.run(...)` submits compiled IR through `ExecuteWorkflow`
4. the real in-memory coordinator dispatches `fetchUsers` then `sendWelcomeEmail`
5. the in-memory JavaScript action runtime resolves those dispatches from the registry

So the test is not bypassing the coordinator. It is only avoiding an external worker process.

### How the future worker path should look

The same bootstrap file should also be the worker preload entrypoint.

Conceptually:

```bash
waymark-worker-node \
  --bridge 127.0.0.1:24118 \
  --worker-id 7 \
  --bootstrap /abs/path/to/my-app/.waymark/actions-bootstrap.mjs
```

At startup:

1. `waymark-worker-node` loads `.waymark/actions-bootstrap.mjs`
2. that imports `lib/actions/users.ts`
3. those actions register with the runtime
4. the worker begins serving `ActionDispatch` messages from the Rust bridge

That is why this single bootstrap artifact matters: it gives tests, example apps, and real workers the same registration path.

## Testing strategy

We should formalize JavaScript action testing into three layers.

### Layer 1: compiler tests

These already exist and should keep growing.

They verify:

- comments are recognized
- workflows are rewritten
- action call sites become IR
- return-action normalization works

### Layer 2: in-process execution tests

These should use:

- `ExecuteWorkflow`
- a real bridge stream in memory mode
- the in-memory JavaScript action registry loaded from the generated bootstrap

They verify:

- compilation actually happened
- the real coordinator actually dispatched actions
- JavaScript actions are actually invoked
- result and error serialization is correct
- `run()` returns the expected payload

This should be the main JavaScript test mode, not a secondary convenience mode.

### Layer 3: out-of-process worker tests

These should start:

- a repo-local `waymark-bridge`
- a repo-local Node worker process
- a workflow registration/execution request

They verify:

- worker boot
- module preload
- dispatch/result roundtrip
- timeouts and retries
- process shutdown and reconnect behavior

This is the JavaScript equivalent of our Python worker integration path and should become the canonical parity test.

## Packaging: the JavaScript wheel equivalent

In Python we ship a wheel that bundles:

- Python library code
- `waymark-bridge`
- `waymark-boot-singleton`
- `waymark-start-workers`

The JavaScript equivalent should be an npm package family.

### Recommended package shape

- `@waymark/sdk`
  - runtime helpers
  - bridge client
  - bootstrap loader
  - binary resolution logic
- `@waymark/nextjs`
  - compiler plugin / loader
  - authoring API
- `@waymark/worker-node`
  - worker entrypoint
  - action execution runtime
- platform binary packages, similar to `esbuild` or `@next/swc`
  - `@waymark/runtime-darwin-arm64`
  - `@waymark/runtime-darwin-x64`
  - `@waymark/runtime-linux-x64-gnu`
  - and so on

The top-level package should expose CLI shims:

- `waymark-bridge`
- `waymark-boot-singleton`
- `waymark-start-workers`
- `waymark-worker-node`

The library should resolve its own binaries from the installed package directory first, not require them to already be on `PATH`.

That is the npm equivalent of how the Python wheel can find its bundled executables.

## Runtime detection: what is realistic

### What we can auto-detect

We can reliably auto-detect:

- whether we are running under Node
- the current executable path via `process.execPath`
- the current working directory
- the nearest `package.json`
- common package-manager hints like `npm_config_user_agent`

That is enough to choose a default JavaScript runtime executable.

### What we should not auto-detect

We should not depend on runtime sniffing for:

- the correct user action entry modules
- the correct bootstrap file contents
- tsconfig path alias semantics
- framework-private server bundle paths
- whether a random source file is safe to import directly in a worker process

That problem is qualitatively different from Python.

In Python, import resolution is already the runtime contract.
In JavaScript, authored modules may pass through:

- TypeScript
- ESM/CJS boundaries
- path aliases
- framework bundling
- server-only transforms

So the safe answer is:

- auto-detect the executable when possible
- use a generated bootstrap module for action loading
- allow explicit overrides for non-standard setups

## Proposed resolution order

For a Node worker startup:

1. If `WAYMARK_JS_BOOTSTRAP` is set, use it.
2. Otherwise, walk upward from `process.cwd()` to find `.waymark/actions-bootstrap.mjs`.
3. If not found, fail with a clear error.

For runtime executable selection:

1. If `WAYMARK_JS_EXECUTABLE` is set, use it.
2. Otherwise, use `process.execPath` when launching from a Node-hosted SDK.
3. Otherwise, fall back to `node` from `PATH`.

For runtime kind:

1. If `WAYMARK_JS_RUNTIME` is set, use it.
2. Otherwise, default to `node`.

## Do users need to provide paths?

In the normal case: no.

The build/plugin should generate:

- the bootstrap module

and the runtime should auto-discover them from the project root.

We should still support explicit overrides for:

- CI
- monorepos with unusual working directories
- embedding Waymark into another host process

But those should be escape hatches, not the primary user experience.

## Rust-side configuration impact

Today Rust worker startup takes Python-oriented config:

- `WAYMARK_USER_MODULE`
- Python worker runner defaults
- `PYTHONPATH` augmentation

The JavaScript equivalent should become a sibling config path, not a special case hidden inside Python settings.

Conceptually:

```rust
pub enum WorkerLanguage {
    Python(PythonWorkerConfig),
    JavaScript(JavaScriptWorkerConfig),
}
```

Where `JavaScriptWorkerConfig` carries:

- runtime kind: `node`
- executable path
- bootstrap path
- optional project root
- optional extra env

That keeps the worker model language-specific where it needs to be, while preserving the same Rust runloop and bridge contracts.

## Suggested immediate roadmap

### Phase 1: formalize the current coordinator-backed in-memory path

- Keep the action registry model.
- Add a small public test helper that:
  - loads the generated bootstrap
  - executes compiled workflows against the in-memory bridge
- Add repo tests that assert JavaScript actions actually run through the real coordinator and return values.

### Phase 2: add generated bootstrap output

- Emit `.waymark/actions-bootstrap.mjs`.
- Make `withWaymark(...)` responsible for keeping it up to date for Next.js projects.

### Phase 3: add `waymark-worker-node`

- Mirror the protocol shape of the Python worker.
- Load the bootstrap at startup.
- Preload action modules by importing the bootstrap once.
- Execute dispatches and return serialized results.

### Phase 4: add Rust-side JavaScript worker pool config

- Add `JavaScriptWorkerConfig`.
- Add a `start-workers` mode that launches Node workers instead of Python workers.
- Reuse the same `WorkerBridgeServer`.

### Phase 5: package it as npm-installable runtime

- Publish SDK packages plus platform runtime packages.
- Resolve bundled bridge binaries from the installed package, not from ambient PATH.

## Recommended decision

We should not require users to hand-maintain JavaScript module paths.

For the first Next.js implementation, we should not require a manifest.

We should require a generated bootstrap module, and that bootstrap should be produced automatically by the plugin/build and auto-discovered by the runtime.

So the practical stance is:

- executable detection: mostly automatic
- action loading: bootstrap-driven
- explicit paths: supported, but only as overrides

That gives us something close to the Python experience without pretending JavaScript module loading is as uniform as Python imports.

## Open questions

- Whether the first out-of-process worker should support only Node, or whether Bun is worth first-class support early.
- Whether the bootstrap should import source action modules directly or import generated wrapper modules later.
- Whether the bootstrap should live under `.waymark/` or another project-local generated path.
- Whether `waymark-start-workers` should accept a language selector or split into language-specific binaries later.
