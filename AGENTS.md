# Waymark

## Code Review

When there are TODOs in the code that indicate issues with the code written (versus areas that we want to implement in the future), you should fix them one by one. You should also generalize the feedback that you receive into advice that applies to our code style guides. For every piece of this feedback, consult the AGENTS.md file. Is the feedback still within the document? If not, add it as a new bullet or nuance a bullet that is already there to be more specific it it overlaps significantly in scope. Feel free to include code examples inline of the good/bad way of how to handle it.

Once you are done fixing the TODO, double check your work. Have we really taken care of the TODO? Remove the comment if so.

Follow this syntax:

<code_feedback>
<rule></rule>
</code_feedback>

## UI Design Conventions

Follow a modern, developer-focused design language. The design prioritizes clarity, information density, and professional polish.

### Theme & Color System

**Dual Theme Support**
- Every component must support both light and dark modes
- Dark mode: Near-black backgrounds (#0a0a0a to #1a1a1a), light text
- Light mode: White/off-white backgrounds, dark text
- Use CSS variables or Tailwind's dark: prefix for all color values

**Semantic Color Palette**
- **Green** (#22c55e / emerald): Success, completed, active states, running processes
- **Blue** (#3b82f6): Primary actions, parent workflows, links, interactive elements
- **Yellow/Amber** (#eab308): Waiting, pending, in-progress states
- **Red** (#ef4444): Errors, failures, destructive actions
- **Gray** (#6b7280): Secondary text, metadata, timestamps, disabled states

### Visual Elements

**Background Treatment**
- Use subtle vertical or grid lines on dark backgrounds for depth and structure
- Lines should be very low contrast (e.g., #1f1f1f on #0a0a0a)
- Diagonal hatching patterns for "idle" or inactive regions

**Cards & Containers**
- Minimize card usage - prefer flat layouts with subtle borders
- When cards are needed: thin 1px borders, no shadows in dark mode
- Light mode cards: subtle shadows allowed, clean white backgrounds
- Border radius: consistent rounded-lg (8px) or rounded-xl (12px)

**Glassmorphism (Floating Elements Only)**
- Apply to modals, dropdowns, popovers, and floating UI
- Use backdrop-blur with semi-transparent backgrounds
- Dark mode: rgba(0,0,0,0.8) with backdrop-blur-lg
- Light mode: rgba(255,255,255,0.9) with backdrop-blur-lg

### Component Patterns

**Status Badges/Pills**
- Rounded-full pill shape with semantic border colors
- Transparent or semi-transparent fill with colored border
- Include status dot indicator when appropriate
- Example: Green border + "Completed" text for success states

**Timeline/Waterfall Visualizations**
- Horizontal bars showing duration and timing
- Color-coded by status (green=success, blue=running, yellow=waiting)
- Show function names and durations inline
- Use grid lines to indicate time intervals

**Code & Technical Text**
- Monospace font (font-mono) for: IDs, function names, code snippets, durations
- Syntax highlighting in code blocks with muted, readable colors
- Inline code: subtle background tint with rounded corners

**Typography Hierarchy**
- Headlines: Bold, larger size, high contrast
- Body text: Regular weight, comfortable reading size
- Metadata: Smaller size, muted gray color
- Use font-medium sparingly for emphasis

### Layout Principles

**Spacing**
- Generous whitespace between sections
- Consistent padding within components (p-4, p-6)
- Use gap utilities for flex/grid layouts

**Information Density**
- Dense data displays (tables, timelines) are acceptable
- Balance density with clear visual hierarchy
- Group related information visually

**Responsive Behavior**
- Mobile-first approach
- Stack horizontal layouts vertically on small screens
- Maintain readability at all breakpoints

### Interaction States

**Hover**
- Subtle background color shift
- Don't rely on hover for essential information

**Focus**
- Clear focus rings for accessibility
- Use ring-2 with brand/accent color

**Active/Selected**
- Distinct visual treatment from hover
- Consider using filled backgrounds instead of just borders

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
<rule>Route webapp dashboard queries through a `WebappBackend` implementation instead of a standalone database wrapper. Good: `impl WebappBackend for PostgresBackend { ... }` Bad: `struct WebappDatabase { pool: PgPool }` with duplicate query logic.</rule>
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
<rule>Explain parser lookahead when indentation-sensitive sibling syntax maps to a nested AST field. Good: comment that a same-indent `finally:` header is consumed before its indented body becomes `TryExcept.finally_block`. Bad: leave index, indentation, and header checks without explaining the syntax-to-AST mapping.</rule>
<rule>Lower compound control-flow syntax through one scope helper instead of interleaving optional behavior throughout the lowering routine. Good: `compile_try_scope` registers handlers and an optional finalizer once and returns the flows reaching its continuation. Bad: scatter `finally_block.is_some()` checks through handler construction and flow joins.</rule>
<rule>Emit shared control-flow bodies once and resume through VM-supported continuations instead of copying their instructions at every exit. Good: one `finally` state entered with a pending jump, return, or raise continuation. Bad: recompiling the `finally` block for normal flow, each return, each loop exit, and exception propagation.</rule>
<rule>Route every scope-leaving jump, return, and raise through one VM unwind stack. Good: exception-handler registration includes its finalizer state and the VM suspends the pending transfer while that state runs. Bad: compiler-generated catch-all handlers, explicit finalizer call chains, or parallel stacks whose depths must be synchronized in bytecode.</rule>
</code_feedback>
