## Rappel JS example app

`example_app_js` mirrors the Python example app, but defines workflows + actions in TypeScript
and serves the UI with an Express server. It also ships a small Node worker that executes
JS actions via the Rappel bridge.

### Prerequisites

- A running Rappel bridge + Postgres (use the root docker-compose or your local stack).
- `RAPPEL_BRIDGE_GRPC_ADDR` or `RAPPEL_BRIDGE_GRPC_HOST`/`RAPPEL_BRIDGE_GRPC_PORT` set.
- `RAPPEL_DATABASE_URL` set if you want the reset endpoint to work.

### Install + build

```bash
cd js
npm install
npm run build

cd ../example_app_js
npm install
npm run build
```

### Run

Start the worker in one terminal:

```bash
cd example_app_js
npm run worker
```

Start the web server in another terminal:

```bash
cd example_app_js
npm run start
```

Then visit http://localhost:8001/.

Notes:
- The JS worker executes actions defined in `src/workflows.ts`.
- Schedule endpoints use the default schedule name `default`.

### Docker

```bash
cd example_app_js
make up
```

Visit http://localhost:8001/ and tear down with:

```bash
make down
```
