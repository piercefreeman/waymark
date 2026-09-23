# Waymark UI design system

## Direction: a surveyor's ledger

Waymark marks trails; the UI is the engineer's field ledger for them. Ruled,
precise, printed. Warm graphite in the dark and plain white in the light,
one sans for labels, one monospace for every value, and status
rendered as ink rather than paint. Nothing on screen is decorative: every
line is a rule, every color is a state, every number has a unit and a scope.

The preview at `make webapp-dev` runs against fixtures authored as event
streams in `src/data/fixtures.ts`. Everything visible is derived from those
events by the same code that will consume the live API, so the preview
cannot show a fact the API does not report.

## Foundations

### Color

Raw values live only in `src/styles/tokens.css`, keyed by `data-theme`.
Components read semantic tokens through `src/styles/theme.css` (`bg-surface`,
`text-fg-muted`, `border-line`, `text-danger`, …). Charts read the same
variables (`var(--running)`), so the light theme is a token swap.

| Role                | Dark                              | Light                             | Use                                           |
| ------------------- | --------------------------------- | --------------------------------- | --------------------------------------------- |
| canvas              | `#0f0f0e`                         | `#ffffff`                         | Page                                          |
| surface             | `#151514`                         | `#ffffff`                         | Bars, rails, tables, panels                   |
| surface-raised      | `#1c1c1a`                         | `#f4f4f5`                         | Hover, chips, run strata                      |
| surface-selected    | `#202019`                         | `#efeff1`                         | The selected row, with a 2px ink left rule    |
| line                | `#292926`                         | `#e4e4e7`                         | Every rule                                    |
| fg / muted / subtle | `#ebeae4` / `#a3a199` / `#6f6d66` | `#18181b` / `#52525b` / `#86868f` | Three text levels, no more                    |
| accent              | `#7ea6f5`                         | `#2d5fcf`                         | Links, focus, primary button, capacity meters |
| running             | `#3fc1b7`                         | `#0d8a82`                         | Active, open promises, live indicator         |
| success             | `#86c26f`                         | `#3d7a2a`                         | Completed, resolved, fresh                    |
| waiting             | `#dba640`                         | `#8a5d00`                         | Suspended, stale, inferred retries, gaps      |
| danger              | `#ff5f49`                         | `#c3301c`                         | Unhandled exception, run error, rejected      |
| neutral             | `#8f8d85`                         | `#6b6961`                         | Cancelled, unknown, retired                   |

Rules:

- Blue means interaction and load. It is never a status. A busy node is not
  a failed node.
- Status is a glyph plus colored text (`StatusInk`). Pills are gone. The only
  fill is the 2px left rule on a row whose instance has an error.
- Selection is neutral (ink rule + raised surface), never a status color.
- Tints come from opacity modifiers on the semantic token (`bg-danger/10`),
  not from extra tokens.

### Type

- UI text: IBM Plex Sans. Data: IBM Plex Mono with tabular numerals
  (`mono-data`). Both are bundled through `@fontsource`.
- Scale, as Tailwind utilities: `text-title` 18/24, `text-section` 14/20,
  `text-body` 13/18, `text-label` 12/16, `text-micro` 11/14, `text-metric`
  20/24. Nothing is larger than 20px; there are no hero numerals.
- Table headers are the quietest text on the page (10px, subtle). Values are
  louder than their labels.

### Density

- Rows: 40px in the ledger, two lines (identity above, evidence below). Controls:
  `h-control` 28px. Global bar 40px. Icon rail 48px. Gutter 20px.
- Radius 4px for controls, 6px for panels. One-pixel rules everywhere; no
  shadows except on floating overlays.
- Motion 120ms for hover and 180ms for panels; nothing continuous except
  the live dot, which respects `prefers-reduced-motion`.

## Screen anatomy

- **Global bar**: mark, environment, sample banner, page title, time window
  (15m/1h/6h/24h), Live/Paused with freshness, jump box (`⌘K` or `/`), theme.
- **Rail**: Instances, Fleet. The component gallery is pinned at the bottom
  and is not product navigation.
- **Workspace**: one `minmax(0,1fr)` column. The peek panel (480px) overlays
  it and never pushes columns. Detail is a route, not a sidebar.
- Filters, the selected instance, the selected promise, and the time window
  live in the URL (`?state=…&q=…&vm=…`, `/instances/:vm_id?promise=…&tab=…`).

### Instances (`/instances`)

1. Title, count, window, and a text filter on one line. State chips with
   page-local counts below it; chips only appear for states present.
2. The ledger, 40px rows: state ink · the first action the VM called (the
   closest reported fact to a workflow name) over the short `vm_id` and its
   module · one plain sentence about now ("Running charge_payment for
   3.6 s", "Sleeping until 00:36", "Unhandled PaymentMismatch 6m ago",
   "Effect handling failed: worker reservation timed out") with node and
   notable counts beneath · a 160px mini-timeline · elapsed time. Rows with
   an error carry a red left rule. Nothing else: raw event kinds, promise
   counts, and node columns moved into the peek.
3. `j`/`k` move, `Enter` peeks, `o` opens, `y` copies the id.

### Instance (`/instances/:vm_id`)

1. Identity bar: full id with copy, state ink.
2. The same sentence as the list, the rule that produced the state, and a
   quiet key–value grid: started, elapsed, promises, driver runs, events.
3. **Waterfall** (the hero): one row per promise the VM actually called,
   grouped into strata per driver run. Bars run from call to settlement.
   Open promises run to now with a hatched tail. Rejections get a red end
   tick. Snapshots are ◆ on the run band. Inferred retries are marked, never
   numbered.
4. Docked drawer: the selected promise (details, an honest "Not recorded"
   arguments block, events involving it), the raw event log with gap rows,
   and the driver runs with their stop reasons and error text.

### Fleet (`/fleet`)

1. Four numbers with their scope: in flight of capacity, queued (with
   dequeue p95), completions per second, handling p50 (with p95).
2. One row per node boot that reads as a sentence: identity and sample age,
   an in-flight meter, "8 workers · 19 resident VMs · 4 queued · 31.5 done/s"
   with latency percentiles beneath. Stale boots dim and are excluded from
   the numbers above.
3. Two charts on one shared axis with gaps for missing samples: in flight
   against capacity, and queued dispatches.

### Paging and search

The ledger follows the list endpoint's cursor, 100 per page. Paging past the
head freezes the window's `to` bound (kept in the URL) so the older pages
stay put; the bar shows "Frozen" with a way back to live. Search and state
chips walk the cursor across the window page by page and keep the matches,
bounded at 20 pages with the footer saying how far the walk got. An exact
`vm_id` reads the instance directly. Search matches ids, node ids, and
states, the fields the list endpoint reports; action names would need a
server-side search.

### Sources and refresh

Pages poll `/api` every 5 s while Live; `?paused=1` stops polling and
`?source=sample` swaps in the authored fixtures (marked with a banner). A
refresh failure never empties a view: the previous data stays, with a notice
that names the error and the last successful time. Nothing falls back to
sample data silently.

## Data honesty

The UI shows only what `/api/observability-state`, `/api/observability-events`
and `/api/essential-metrics` report (types in `src/domain/api.ts`).

| Fact                  | Source                                               | Treatment                                   |
| --------------------- | ---------------------------------------------------- | ------------------------------------------- |
| Instance state        | outcome, then latest run stop reason, then freshness | `deriveInstanceState`; rule shown on hover  |
| "Failing"             | not a state                                          | rejections counted separately               |
| Workflow name         | not reported                                         | short `vm_id`; first action labeled derived |
| Node                  | id per boot, no hostname                             | short id, boot time, retired marker         |
| Arguments and results | not recorded                                         | explicit "Not recorded" block               |
| Exception             | type only                                            | the type, nowhere a message                 |
| Retries               | no attempt number                                    | "retry of #n, inferred"                     |
| Duration per promise  | call → settlement                                    | labeled as including queueing               |
| Completion rate       | counter deltas per boot                              | never the raw counter                       |
| Percentiles           | bucket counts                                        | summed across nodes, never averaged         |
| Missing events        | gaps in `run_sequence`                               | hatched rows and counts                     |

## Components

`src/components/ui` holds the retained shadcn primitives (button, input,
tabs, tooltip), stripped of `dark:` overrides so tokens do all theming.
`src/components/patterns` holds Waymark patterns; each takes values and
callbacks and never fetches.

| Pattern                                            | Responsibility                                         |
| -------------------------------------------------- | ------------------------------------------------------ |
| `StatusInk`, `InstanceStateInk`, `PromiseStateInk` | State vocabulary with the rule on hover                |
| `SectionHeader`                                    | Title, quiet count, description, actions               |
| `MetricStrip`, `MetricTile`                        | Flat tiles; label, ≤20px value, unit, scope            |
| `Meter`                                            | Used-of-capacity; unavailable is not 0%                |
| `Identifier`, `CopyButton`                         | Middle-truncated ids with copy                         |
| `TimeAgo`, `Clock`, `Duration`                     | Time with the absolute value on hover                  |
| `KeyValueList`                                     | Rows or grid, on the 4px grid                          |
| `FilterChips`                                      | Multi-select toggles with counts and a scope caption   |
| `MiniTimeline`                                     | An instance's life at 160px                            |
| `Waterfall`                                        | Promise rows in driver-run strata with a time axis     |
| `EventLog`                                         | Monospace observation log with gap rows                |
| `TimeSeriesChart`                                  | Shared-axis small multiple; gaps stay gaps; log option |
| `HistogramBars`                                    | Bucket counts with percentile markers                  |
| `PayloadViewer`                                    | Recorded / not recorded / pending, visibly distinct    |
| `EmptyState`                                       | empty / filtered / unavailable / error                 |
| `AppShell`, `PeekPanel`                            | Global bar, rail, and the overlay peek                 |

Domain logic lives in `src/domain` (`derive.ts` turns events into runs,
promises, snapshots and state; `metrics.ts` turns samples into rates and
percentiles) and is unit tested in Node without React.

## Live API

`src/api/client.ts` wraps the routes the server mounts at `/api`
(`observability-state/instances`, `observability-events`, `essential-metrics/nodes`).
`src/data/live.ts` polls them, keeps the previous response while refreshing,
pauses in hidden tabs, and cancels superseded requests. `src/app.tsx` derives
view models with `deriveFromInstance` (the state endpoint is authoritative for
state; events add promises and runs). The list reads one cursor page of instances and, for the rows on screen,
each instance's timeline, cached by its last event so a poll refetches
only rows that changed. A per-instance summary endpoint and server-side
filtering would remove those reads.

The compiled SPA is embedded in `waymark-start-workers` by the
`waymark-http-webapp` build script and served next to `/api`; `make webapp-dev`
proxies `/api` to a running server at `127.0.0.1:24119`.
