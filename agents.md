## Coding Conventions

- Never add optional-import fallbacks for core dependencies (e.g., wrapping `pydantic` imports in `try/except`). Import them directly and let the program fail fast if they're missing.
- Always run "make lint" and clear the outstanding linting errors before yielding back. Only on very difficult lints where fixing the lint would corrupt the logic should you yield to me for expert intervention. Never yourself write code that ignores the lints on a per line basis. Linting errors should be respected.
- Any python code that you run should be called with `uv` since this is the environment that will have the python dependencies we need. Also make sure you're in the appropriate directory where our pyproject.toml is defined.
- When writing code that uses WhichOneof in Python, use a switch statement to make sure that every value is handed and add a default case for assert_never.
- NEVER write `getattr` in your own code unless I explicitly mention it. You should just be able to call it directly.

## Workflow Conventions

- NEVER modify the protobuf python files directly, instead modify the base messages if you have to and run `make build-proto`

## Unit Tests

- Run python tests with `uv run pytest`
- To run the rust integration tests you'll have to do something like: source .env && cargo test ...
