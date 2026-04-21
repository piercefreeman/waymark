# Waymark JavaScript Example

This app is the Next.js example for JavaScript workflows.

It shows:

- `// use action` markers on exported async functions
- `class ... extends Workflow` authoring
- `withWaymark(...)` wiring in `next.config.js`
- generated `.waymark/actions-bootstrap.mjs` registration
- generated `.waymark/actions/*.mjs` wrappers for standalone workers
- a route handler that invokes `await workflow.run(...)`

## Full-stack mode

Like [example-app/python](/Users/piercefreeman/projects/waymark/example-app/python/README.md), this example now runs as a full Waymark deployment:

- Postgres for persistence
- `waymark-start-workers` for the scheduler, worker bridge, remote JavaScript workers, and the dashboard
- the dashboard on `http://localhost:24119/`
- the Next.js example app on `http://localhost:3000/`

The `daemons` container runs JavaScript workers by pointing `WAYMARK_JS_BOOTSTRAP` at the generated `.waymark/actions-bootstrap.mjs` file and `WAYMARK_JS_EXECUTABLE` at `waymark-worker-node`. Those workers execute the registered action handlers outside the Next.js server process, which is the same deployment shape we use for the Python example.

## Running locally

```bash
cd example-app/javascript
make up
make docker-test
make down
```

Visits to `http://localhost:3000/` render the example UI. Each submission invokes `ExampleMathWorkflow`, which calls three JavaScript actions and returns the combined result. The workflow state and execution history are visible in the dashboard at `http://localhost:24119/`.

Environment notes:

- `webapp` relies on the default Waymark behavior of booting a singleton `waymark-bridge` inside the container on first workflow execution. Because `WAYMARK_DATABASE_URL` is set, that bridge uses the database-backed path instead of the in-memory bridge stream.
- `daemons` runs `waymark-start-workers` with `WAYMARK_WORKER_LANGUAGE=javascript`, plus explicit bootstrap, executable, and working-directory paths for `waymark-worker-node`.

## Zero-config dev mode

If you run `npm run dev` or `npm start` outside Docker and do not set `WAYMARK_DATABASE_URL` or any `WAYMARK_BRIDGE_*` variables, the route falls back to `WAYMARK_BRIDGE_IN_MEMORY=1`. That mode is useful for local iteration, but it does not start the dashboard on `24119` and it does not use the remote worker deployment path.

## Files to look at

- [docker-compose.yml](/Users/piercefreeman/projects/waymark/example-app/javascript/docker-compose.yml)
- [app/api/run/route.ts](/Users/piercefreeman/projects/waymark/example-app/javascript/app/api/run/route.ts)
- [lib/actions/math.ts](/Users/piercefreeman/projects/waymark/example-app/javascript/lib/actions/math.ts)
- [lib/workflows/example-math-workflow.ts](/Users/piercefreeman/projects/waymark/example-app/javascript/lib/workflows/example-math-workflow.ts)
- [tests/integration.mjs](/Users/piercefreeman/projects/waymark/example-app/javascript/tests/integration.mjs)
