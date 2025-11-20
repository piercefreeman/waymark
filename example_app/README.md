## Rappel example app

`example_app` contains a minimal FastAPI + Jinja application that dispatches a
rappel workflow. This is intended to show in mineature what it would take to actually deploy something to production:

`docker-compose.yml` starts Postgres, a `daemons` container (running
`start_workers`), and a `webapp` container that serves the FastAPI UI and boots
its own `rappel-server` automatically via the Python client bridge.

Our Dockerfile is a bit more complicated than you would need, because we actually run it against our locally build rappel wheel. In your project you can accomplish this by just `uv add rappel`.

## Running locally

We're effectively just wrapping the docker-compose within our make file, but it makes it a bit easier to apply tests that should execute within the container.

```bash
cd example_app
make up             # docker compose up --build -d
make docker-test    # run uv run pytest -vvv inside the built image
make down           # stop and clean up
```

Visits to http://localhost:8000/ will render the HTML form. Each submission
invokes `ExampleMathWorkflow`, which uses two actions (factorial + Fibonacci)
in parallel via `asyncio.gather` and a third action to merge the results into a
summary payload before responding to the browser.

Environment notes:

- `webapp` relies on the default rappel behavior of booting a singleton server
  inside the container whenever a workflow is invoked, so no extra env vars are
  required.
- `daemons` runs `start_workers` with `CARABINER_USER_MODULE=example_app.workflows`
  so the worker dispatcher preloads the module that defines the sample actions.

## Tests

The FastAPI endpoint is covered by a pytest case that exercises the entire
workflow end-to-end. Run the suite inside the docker image (ensuring that the
wheel install and runtime environment match production) via:

```bash
make docker-test
```
