# Waymark JavaScript example app

This Next.js app demonstrates the JavaScript VM path end to end:

- typed actions declared with `action(async function …)`
- durable control flow in `Workflow.run()`
- parallel actions with `Promise.all`
- build-time compilation through esbuild's resolver callback
- exact-byte workflow hashing and a generated ESM action manifest
- server-only execution through the Waymark bridge

The compiler writes generated artifacts to `.waymark/`; they are always rebuilt
from `src/waymark/math.workflow.ts` and are not committed.

## Run locally

From the repository root, install and build the JavaScript workspaces:

```bash
npm ci --ignore-scripts
npm run build --workspace @waymark/nextjs
```

Start an in-memory Waymark bridge:

```bash
WAYMARK_BRIDGE_IN_MEMORY=true cargo run -p waymark-bridge
```

Then start Next.js:

```bash
WAYMARK_BRIDGE=127.0.0.1:24117 \
  npm run dev --workspace @waymark/example-javascript
```

Open http://localhost:3000 and submit an integer from 1 through 10.

## Docker

```bash
cd examples/javascript
make up
```

Docker Compose starts the in-memory bridge and the Next.js app. Run
`make down` when finished.

## Tests

```bash
npm test --workspace @waymark/example-javascript
```

The test recompiles the workflow, verifies that its hash matches the serialized
bytes, loads the generated action bundle, and executes all three action
implementations.
