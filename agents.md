# Rappel

## Terminology

- A "node_id" is only true for the ground truth DAG node that come from the workflow definition in the run() workflow. This is converted from Python->IR->DAG. These are the `nodes` that are referenced.
- An "execution_id" comes from the state graph of the program that is currently running. Loops are unrolled in this state, for example. They also maintain the state of the actions that are actually pushed into the cluster like the current attempt number of the actions.

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

## Workflow Conventions

- NEVER modify the protobuf python files directly, instead modify the base messages if you have to and run `make build-proto`
- When defining Workflow classes, pass arguments directly to the `run()` method, NOT to `__init__()`. The workflow decorator automatically handles serialization and deserialization of run() arguments.
  - Correct: `async def run(self, user_id: str) -> Result:`
  - Incorrect: `def __init__(self, user_id: str):` with `self.user_id = user_id`

## Unit Tests

- Run python tests with `uv run pytest`
- To run the rust integration tests you'll have to do something like: source .env && cargo test ...

## AI Controlled

This section is used for the scratch updates, driven by our Agents.

<code_feedback>
<rule>Centralize environment parsing in shared config modules and build sub-configs inside `from_env`. Good: `let cfg = WorkerConfig::from_env()?; let webapp = cfg.webapp.clone();` Bad: `let cfg = WorkerConfig::from_env()?; let webapp = WebappConfig::from_env();`</rule>
<rule>Prefer `?` (with `context` when needed) over wrapping simple errors with `map_err(|err| anyhow!(err))`. Good: `PostgresBackend::connect(dsn).await?;` Bad: `PostgresBackend::connect(dsn).await.map_err(|err| anyhow!(err))?;`</rule>
<rule>Use SQLx migrations for schema creation instead of ad-hoc `CREATE TABLE` blocks in binaries. Good: `db::run_migrations(&pool).await?;` Bad: `sqlx::query("CREATE TABLE...").execute(&pool).await?;`</rule>
<rule>Own and shut down exclusive dependencies in the component that uses them (e.g., worker pools own their bridge servers). Good: `PythonWorkerPool::new_with_bridge_addr(...)` Bad: `let bridge = WorkerBridgeServer::start(...); PythonWorkerPool::new(..., bridge, ...)`</rule>
<rule>Promote shared runtime helpers into their owning modules rather than duplicating them in binaries. Good: `runloop::runloop_supervisor(...)` Bad: `async fn runloop_supervisor(...) { ... }` in a bin.</rule>
<rule>Prefer injecting shared database pools into backends/services; run migrations in the owning binary/config instead of creating pools and defaults inside backend modules. Good: `let pool = PgPool::connect(&cfg.database_url).await?; db::run_migrations(&pool).await?; let backend = PostgresBackend::new(pool);` Bad: `let backend = PostgresBackend::connect(DEFAULT_DSN).await?;`</rule>
<rule>In-memory backends used for tests should retain persisted updates in-memory for assertions instead of only logging side effects. Good: `stored.extend(actions.iter().cloned());` Bad: `for action in actions { println!("INSERT {:?}", action); }`</rule>
<rule>Avoid pass-through module stubs that only re-export another module; import from the source module or re-export at the top-level instead. Good: `use crate::workers::InlineWorkerPool;` Bad: `pub mod workers { pub use crate::workers::*; }`</rule>
</code_feedback>
