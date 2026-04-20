# Waymark JavaScript Example

This app is the first Next.js example for the JavaScript workflow compiler.

It shows:

- `// use action` markers on exported async functions
- `class ... extends Workflow` authoring
- `withWaymark(...)` wiring in `next.config.js`
- a route handler that invokes `await workflow.run(...)`

## Current status

This example demonstrates the compiler and client-side bridge integration, but it is not yet end-to-end runnable against the existing worker runtime.

Today, Waymark still executes actions in Python workers. The JavaScript compiler can lower workflows into IR and queue them to the bridge, but JavaScript action execution has not been implemented yet.

Use this app as the source-of-truth example for the JavaScript authoring surface while the worker/runtime side catches up.

## Files to look at

- [next.config.js](/Users/piercefreeman/projects/waymark/example-app/javascript/next.config.js)
- [app/api/run/route.ts](/Users/piercefreeman/projects/waymark/example-app/javascript/app/api/run/route.ts)
- [lib/actions/math.ts](/Users/piercefreeman/projects/waymark/example-app/javascript/lib/actions/math.ts)
- [lib/workflows/example-math-workflow.ts](/Users/piercefreeman/projects/waymark/example-app/javascript/lib/workflows/example-math-workflow.ts)
