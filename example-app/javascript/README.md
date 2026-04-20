# Waymark JavaScript Example

This app is the first Next.js example for the JavaScript workflow compiler.

It shows:

- `// use action` markers on exported async functions
- `class ... extends Workflow` authoring
- `withWaymark(...)` wiring in `next.config.js`
- generated `.waymark/actions-bootstrap.mjs` registration
- generated `.waymark/actions/*.mjs` wrappers for standalone workers
- a route handler that invokes `await workflow.run(...)`

## Current status

This example is runnable through the live bridge path. The Next.js plugin rewrites `run()` into IR submission, and the example executes those compiled action dispatches through the in-memory bridge stream.

The important constraint still holds: action call sites inside the workflow are not executed as normal JavaScript. They are compiled into IR-owned stubs, and the runtime only executes registered action handlers when the bridge dispatches them.

The example route imports the generated `.waymark/actions-bootstrap.mjs` file so the server process registers all action modules before the workflow runs. The same bootstrap now works for the standalone `waymark-worker-node` runtime because it imports generated wrapper modules under `.waymark/actions/` instead of depending on Next's loader to rewrite the original action source at worker startup.

Treat this app as the source-of-truth example for the current JavaScript authoring surface and bridge integration.

## Files to look at

- [next.config.js](/Users/piercefreeman/projects/waymark/example-app/javascript/next.config.js)
- [app/api/run/route.ts](/Users/piercefreeman/projects/waymark/example-app/javascript/app/api/run/route.ts)
- [lib/actions/math.ts](/Users/piercefreeman/projects/waymark/example-app/javascript/lib/actions/math.ts)
- [lib/workflows/example-math-workflow.ts](/Users/piercefreeman/projects/waymark/example-app/javascript/lib/workflows/example-math-workflow.ts)
