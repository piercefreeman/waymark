# Waymark

## Code Review

When there are TODOs in the code that indicate issues with the code written (versus areas that we want to implement in the future), you should fix them one by one. You should also generalize the feedback that you receive into advice that applies to our code style guides. For every piece of this feedback, consult the AGENTS.md file. Is the feedback still within the document? If not, add it as a new bullet or nuance a bullet that is already there to be more specific it it overlaps significantly in scope. Feel free to include code examples inline of the good/bad way of how to handle it.

Once you are done fixing the TODO, double check your work. Have we really taken care of the TODO? Remove the comment if so.

Follow this syntax:

<code_feedback>
<rule>Keep substantial frontend build orchestration in a testable library consumed through `build-dependencies`, leaving `build.rs` to supply paths. Use `xshell` for command interpolation and contextual filesystem/process errors; test argument boundaries, failed commands, and missing build outputs.</rule>
<rule>Temporary CI guards for jobs that check out an older baseline must explain why they exist and name the condition for removing them.</rule>
<rule>Document non-obvious HTTP combinators by the behavior they preserve, including accurate status codes. Use a GET/HEAD method router for static fallback handling so other methods receive 405 rather than HTML.</rule>
<rule>Name shared setup actions for the language workspace they prepare (for example, `setup-js`), rather than for a single application that consumes them.</rule>
<rule>Embed and serve the complete compiled frontend bundle with correct content types, including in debug builds. Keep missing assets as 404 responses rather than returning the SPA entry point; packaged binaries must not depend on source directories or a Node runtime.</rule>
<rule>Keep route mounting and fallback isolation in the HTTP composition layer. API modules may describe their external mount path in OpenAPI, but return relative routes without depending on other HTTP surfaces.</rule>
<rule>Organize JavaScript applications under `js/app/*` and libraries under `js/lib/*`, with one root npm workspace, lockfile, and Prettier ignore file. Install dependencies once at the workspace root, including in CI and Docker.</rule>
<rule>Pin the default Node.js toolchain in the repository-root `.node-version`, shared by all JavaScript workspaces and build environments.</rule>
<rule></rule>
</code_feedback>

## UI Design Conventions

The SPA's tokens, type scale, screen anatomy, and data-honesty rules live in [js/app/web/DESIGN.md](js/app/web/DESIGN.md). That document is the source of truth; the summary here exists so the rules travel with the rest of this file.

**Identity: a surveyor's ledger.** Warm graphite dark theme, warm paper light theme. IBM Plex Sans for UI text, IBM Plex Mono with tabular numerals for every value. One-pixel rules, no cards, no shadows except on floating overlays, no decorative gradients or hatching except to mark an unavailable interval.

**Color is state, never decoration.**
- Semantic tokens only (`bg-surface`, `text-fg-muted`, `border-line`, `text-danger`). Raw hex values live in `src/styles/tokens.css` and nowhere else; charts read the same CSS variables.
- Running/open = teal, success = green, waiting/stale/inferred = ochre, failure = vermilion, cancelled/unknown = neutral. Blue is reserved for interaction and load (links, focus, meters); a busy node is not a failed node.
- Status is ink: a glyph plus colored text. No pills. The only fill is the 2px left rule on a row whose instance has an error. Selection is neutral (ink rule + raised surface).
- Tints come from opacity modifiers on the semantic token (`bg-danger/10`), not from extra tokens.

**Type and density.**
- Scale: `text-title` 18/24, `text-section` 14/20, `text-body` 13/18, `text-label` 12/16, `text-micro` 11/14, `text-metric` 20/24. Nothing larger than 20px; no hero numerals. Headers are quieter than the values under them. Prefer one plain sentence over a row of raw fields: "Running charge_payment for 3.6 s" beats a kind string, a node id, and three counts.
- Rows 36px (`h-row`), controls 28px (`h-control`), global bar 40px, icon rail 48px, gutter 20px. Radius 4px controls / 6px panels.
- Every number has a unit and a scope caption ("2 fresh nodes · 1 excluded"). Every live view shows freshness.

**Structure.**
- One 40px global bar carries identity, environment, page title, time window, Live/Paused with freshness, jump box, and theme. No page-header band, no taglines, no eyebrow labels, no stack credits.
- Detail is a route. A peek panel overlays the workspace and never pushes columns. Filters, selection, and the time window live in the URL.
- Only show what the API reports. Names, payloads, attempt numbers, and hostnames are not reported; render "Not recorded", "derived", "inferred", or a short id instead. Rejections are counted, never promoted to a workflow state.
- Empty, filtered-empty, unavailable, and error states must never look alike. A missing value must never look like a recorded null or a loading state.

## Coding Conventions

- Never add optional-import fallbacks for core dependencies (e.g., wrapping `pydantic` imports in `try/except`). Import them directly and let the program fail fast if they're missing.
- Always run "make lint" and clear the outstanding linting errors before yielding back. Only on very difficult lints where fixing the lint would corrupt the logic should you yield to me for expert intervention. Never yourself write code that ignores the lints on a per line basis. Linting errors should be respected.
- Any python code that you run should be called with `uv` since this is the environment that will have the python dependencies we need. Also make sure you're in the appropriate directory where our pyproject.toml is defined.
- When writing code that uses WhichOneof in Python, use a switch statement to make sure that every value is handed and add a default case for assert_never.
- NEVER write `getattr` in your own code unless I explicitly mention it. You should just be able to call it directly.
- Unless we explicitly mention backwards compatibility, don't implement logic that assumes how the backwards spec is compatible. We will specify it if it's relevant.
- There's no need to ever import `annotations` `from __future__`. We always run on a Python version with the latest typehinting where this will be supported.

## Workflow Conventions

- NEVER modify the protobuf python files directly, instead modify the base messages if you have to and run `make build-proto`
- When defining Workflow classes, pass arguments directly to the `run()` method, NOT to `__init__()`. The workflow decorator automatically handles serialization and deserialization of run() arguments.
  - Correct: `async def run(self, user_id: str) -> Result:`
  - Incorrect: `def __init__(self, user_id: str):` with `self.user_id = user_id`

## Unit Tests

- Run python tests with `uv run pytest`
- To run the rust integration tests you'll have to do something like: source .env && cargo test ...
- Unless there's a compelling reason, you should construct synthetic programs by writing code in our IR language and then parsing with `ir_parser.rs`. This makes it much easier for people to add additional IRs in the future. It also guarantees that the compiled program matches what's actually produced at runtime. If you _really_ need control at the AST/bytecode level, write a detailed comment justifying why we need to do it manually versus just using the parser.
- If there is common logic/helpers that is shared by a bunch of different rust tests, we should extract a test harness class and place it in a local test_helpers.rs. This file serves as our conventional rust equivalent for conftest.py in Python where we can dump these helpers instead of interrupting the flow of the file under test.

## AI Controlled

This section is used for the scratch updates, driven by our Agents.

<code_feedback>
<rule>Ship UI routes backed only by production APIs; keep synthetic fixtures in tests, never in application imports, query-selectable demo modes, or fallback paths. Show explicit empty and unavailable states when real data is absent. Test fixtures should use the API's event or sample types and production derivation code so they cannot invent facts the API does not report.</rule>
<rule>Keep domain state vocabularies in `src/domain`, derived from wire types, and let UI components import them; never define a status union inside a component. Good: `domain/status.ts` exports `InstanceState` with the rule that produces each value. Bad: `type Status = keyof typeof statuses` living in `status-badge.tsx` and imported by fixtures.</rule>
<rule>When a fixture helper returns negative offsets (time ago), do not add raw positive milliseconds to it for "later" steps without a comment; jumbled event order silently produces wrong derived state. Good: `minutes(6) + seconds(2)` for 6m02s ago, with the sign convention documented beside the helper. Bad: mixing `minutes(6) - 40` and `minutes(6) + seconds(0.02)` in one scenario.</rule>
<rule>Keep process-global tracing setup in `waymark-fn-main-common`. Tests that assert emitted tracing events may use scoped subscribers through a dev-dependency, with a documented crate-specific wrapper entry in `deny.toml`.</rule>
<rule>Avoid webapp tests that assert rendered HTML contains route or API URL strings; test behavior, data wiring, or stable UI semantics instead. Good: assert a handler returns the expected redirect payload or a page renders the expected domain data. Bad: `assert!(rendered.contains("/api/instance/"));`.</rule>
<rule>Centralize environment parsing in shared config modules and build sub-configs inside `from_env`. Good: `let cfg = WorkerConfig::from_env()?; let webapp = cfg.webapp.clone();` Bad: `let cfg = WorkerConfig::from_env()?; let webapp = WebappConfig::from_env();`</rule>
<rule>Prefer `?` (with `context` when needed) over wrapping simple errors with `map_err(|err| anyhow!(err))`. Good: `PostgresBackend::connect(dsn).await?;` Bad: `PostgresBackend::connect(dsn).await.map_err(|err| anyhow!(err))?;`</rule>
<rule>Use SQLx migrations for schema creation instead of ad-hoc `CREATE TABLE` blocks in binaries. Good: `db::run_migrations(&pool).await?;` Bad: `sqlx::query("CREATE TABLE...").execute(&pool).await?;`</rule>
<rule>Own and shut down exclusive dependencies in the component that uses them (e.g., worker pools own their bridge servers). Good: `PythonWorkerPool::new_with_bridge_addr(...)` Bad: `let bridge = WorkerBridgeServer::start(...); PythonWorkerPool::new(..., bridge, ...)`</rule>
<rule>Promote shared runtime helpers into their owning modules rather than duplicating them in binaries. Good: `waymark_execution_bringup::start(...)` Bad: hand-wiring the same subsystem startup inside a bin.</rule>
<rule>Prefer injecting shared database pools into backends/services; run migrations in the owning binary/config instead of creating pools and defaults inside backend modules. Good: `let pool = PgPool::connect(&cfg.database_url).await?; db::run_migrations(&pool).await?; let backend = PostgresBackend::new(pool);` Bad: `let backend = PostgresBackend::connect(DEFAULT_DSN).await?;`</rule>
<rule>In-memory backends used for tests should retain persisted updates in-memory for assertions instead of only logging side effects. Good: `stored.extend(actions.iter().cloned());` Bad: `for action in actions { println!("INSERT {:?}", action); }`</rule>
<rule>Avoid pass-through module stubs that only re-export another module; import from the source module or re-export at the top-level instead. Good: `use crate::workers::InlineWorkerPool;` Bad: `pub mod workers { pub use crate::workers::*; }`</rule>
<rule>Prefer async trait methods for backend interfaces instead of BoxFuture-based signatures. Good: `trait WorkerStatusBackend { async fn upsert_worker_status(&self, status: &WorkerStatusUpdate) -> BackendResult<()>; }` Bad: `fn upsert_worker_status<'a>(&'a self, status: &'a WorkerStatusUpdate) -> BoxFuture<'a, BackendResult<()>>;`</rule>
<rule>Name a persistence trait by domain (e.g., `WorkloadPinningBackend`) rather than a generic `BaseBackend` to make scope explicit.</rule>
<rule>Prefer exhaustive `match` handling in Rust over exporting a generic `assert_never` helper. Good: `match status { Status::Queued => ..., Status::Running => ..., Status::Completed => ..., Status::Failed => ... }` Bad: `assert_never(status)`.</rule>
<rule>Centralize worker pool metrics in shared helpers so pools don't duplicate tracking logic. Good: `WorkerPoolMetrics::new(worker_ids, window, samples); metrics.record_completion(idx);` Bad: per-pool `WorkerThroughputTracker`/`LatencyTracker` structs.</rule>
<rule>Add a minimal happy-path test for formatting/serialization helpers. Good: parse IR then `assert_eq!(format_program(&program), source);` Bad: leaving formatting logic untested.</rule>
<rule>Centralize external test harness setup (e.g., Postgres via docker compose) in shared test fixtures instead of ad-hoc per-test DSN probing. Good: `let pool = test_support::postgres_setup().await;` Bad: each test loops through env vars and fallback DSNs independently.</rule>
<rule>Avoid redundant private accessors for private fields when standard data access communicates intent clearly. Good: `let Some(shared) = guard.as_mut() else { return Err(...); };` Bad: `if shared.is_closed() { ... }` on a private `SharedState`.</rule>
<rule>Avoid trivial inherent constructors on public wrapper or enum types when direct construction or an existing generic helper is already clear. Good: `RegisterHandle::Existing(register)` or `Marked::mark(handle)`. Bad: `RegisterHandle::existing(register)` or `PromiseHandle::new(handle)`.</rule>
<rule>Use distinct error variants for distinct failure states instead of reusing a nearby transport error. Good: `return Err(SendActionError::WorkerProtocolClosed);` Bad: `return Err(SendActionError::ChannelClosed);` when protocol state was already closed before enqueue.</rule>
<rule>Model closed or unavailable shared state with `Option<T>` under the lock instead of parallel boolean flags. Good: `Mutex<Option<SharedState>>` with `guard.take()` on shutdown. Bad: `SharedState { closed: bool, ... }` plus manual checks.</rule>
<rule>When shared async state must be closed during unwind or normal teardown, prefer a synchronous mutex plus a drop guard if the lock is never held across `.await`. Good: `let _guard = SharedStateDropGuard::new(Arc::clone(&shared));` with `Arc<std::sync::Mutex<Option<SharedState>>>`. Bad: async-only teardown paths that skip cleanup on panic.</rule>
<rule>Never abbreviate type parameter names, struct names, enum names, trait names, or enum variant names. Spell words out in full — the extra characters cost nothing and eliminate ambiguity. Good: `SpawningFactory<Backend, Codec, ExecutableProvider, Interpreter, Effector, Value>`, `ReviveError`. Bad: `ExeProvider`, `ReviveErr`, `VmId` as a type param (use `VmIdentifier` or keep `VmId` only if the domain term itself is abbreviated).</rule>
<rule>Never `use` trait names for impl blocks; always use the full path. Good: `impl waymark_state_manager_core::Factory for ...`. Bad: `use waymark_state_manager_core::Factory; impl Factory for ...`.</rule>
<rule>When lowering emits a sequence of writes that all read one expression's result, run that result through `ValueCompiler::unalias_source` first — `compile_expr` hands back whatever register already holds the value, so a variable source can be one of the destinations (`a, b = a`) and the first write clobbers what the later ones read. Good: `let value_register = self.value_compiler().unalias_source(value_register, targets.iter().map(|target| target.register()));` Bad: writing into registers that may be the source with no alias check.</rule>
<rule>Audit and repair Linux binary wheels with `auditwheel` before publishing them to PyPI; interpreter compatibility tags do not prove that bundled executables satisfy a manylinux ABI. Pin the Linux build image to the intended glibc baseline so runner upgrades cannot silently reduce compatibility. Good: build on a pinned image and publish the `manylinux_*` wheel produced by `auditwheel repair`. Bad: build on `ubuntu-latest` and publish a wheel tagged `linux_x86_64` or `linux_aarch64`.</rule>
<rule>Pin `MACOSX_DEPLOYMENT_TARGET` to the oldest supported macOS release when building binary wheels, and derive the wheel tag from that same value so the tag matches the bundled binaries. Good: compile with `MACOSX_DEPLOYMENT_TARGET=11.0` and publish a `macosx_11_0_arm64` wheel. Bad: derive compatibility from the current CI runner and silently publish a `macosx_26_0_arm64` wheel.</rule>
</code_feedback>
