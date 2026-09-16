# Waymark UI design system

## Direction

A calm, precise workspace for understanding workflows under load. Borrow Linear's
typography, restrained surfaces, and consistent interaction patterns; borrow the
monitoring reference's dense tables, inline meters, and persistent inspector.
Make exceptional behavior easy to find without making every screen look alarming.

This first pass implements the shared components and an interactive sample
workspace. Run `make webapp-dev` to explore Workflows, Workers, and Components.
The API guidance below defines the next integration step; the preview uses only
explicit fixtures from `src/preview/`.

- Dark by default, with an explicit light theme. Theme choice persists locally.
- Flat layouts, thin dividers, quiet navigation. Save containers for meaningful
  groupings; avoid a dashboard made entirely of cards.
- Color communicates state, selection, or a named metric. Decoration stays neutral.
- Every number has a unit, time range, and scope. Every live view exposes freshness.
- Use actual domain labels: workflows, actions, workers. Keep VM terminology in
  technical details where it helps explain an event.
- Build on React, Tailwind, and locally owned shadcn/ui primitives. Reuse these
  components throughout the app; feature pages compose them and own data fetching.

## Foundations

### Color

Use CSS semantic tokens throughout components, including SVG charts. The light
theme changes token values rather than changing each component's styling.

| Role           | Dark      | Light     | Meaning                             |
| -------------- | --------- | --------- | ----------------------------------- |
| Canvas         | `#0b0d10` | `#f6f7f9` | Page background                     |
| Surface        | `#111419` | `#ffffff` | Navigation, tables, inspector       |
| Raised surface | `#191d24` | `#eef1f5` | Hover, menus, inputs                |
| Divider        | `#262c35` | `#dce1e8` | Layout structure                    |
| Primary text   | `#edf0f5` | `#18202c` | Names, values, headings             |
| Secondary text | `#9aa5b5` | `#596579` | Metadata, supporting text           |
| Blue           | `#79aaff` | `#245cce` | Running, links, focus, selection    |
| Green          | `#52d6a0` | `#16734d` | Success, healthy connection         |
| Amber          | `#f2c46d` | `#8a5900` | Waiting, pending, retrying          |
| Red            | `#fb8793` | `#bb3044` | Failed, failing, destructive action |
| Neutral        | `#a5adba` | `#626c7c` | Unknown, cancelled, stale           |

Status badges pair a readable label with an icon; color is never the only signal.
Use a subtle tinted fill and border. Running indicators may animate only while
the view is actively refreshing; respect reduced motion. Charts use the same
colors as the corresponding labels and legends.

Keep load magnitude separate from workflow outcome: a busy worker is not a failed
worker. Use blue for capacity usage; reserve amber/red for an explicitly defined
warning or error. Show unknown capacity as unavailable, never `0%`.

### Type, spacing, and surfaces

- System sans-serif for navigation and prose; system monospace for identifiers,
  function names, payloads, durations, and numeric data. Use tabular numerals.
- Page title: 24 px / 32 px, semibold. Section title: 14 px / 20 px, medium.
  Body and table values: 13 px / 20 px. Supporting labels: 12 px / 16 px.
- A 4 px spacing grid. Controls: 32–36 px tall; tables: 44–48 px rows. Give touch
  targets at least 44 px on mobile. Page gutters: 24 px desktop, 16 px mobile.
- Radius: 6 px controls, 8 px panels, full radius only for status pills.
- One-pixel borders. No dark-mode panel shadows. Only floating menus/dialogs may
  use translucent surfaces, blur, and restrained shadows.
- Use fine chart gridlines; keep decorative grids and hatching out of dense text.
  Hatching can identify an unavailable or idle interval alongside a text label.

## Navigation and screen anatomy

Desktop: a 208 px navigation rail, flexible central workspace, and a roughly 360 px
inspector when an item is selected. One compact global header holds environment
context, connection freshness, and theme selection. Keep the main workspace wide.

Tablet: collapse navigation and let detail content occupy a full page. Mobile:
stack metrics, offer horizontal scrolling inside tables, and open details as a
full-screen sheet. Never require precision clicking on a tiny timeline bar.

### Workflows

1. Heading, time window, last update, and refresh/pause control.
2. A flat summary strip: observed running workflows, completed, failed, and worker
   action concurrency. Label aggregates with their scope.
3. Search and state filters. Filter state and selected instance belong in the URL
   so inspection can be shared and browser Back restores context.
4. Compact rows: workflow name when available, instance ID, status, duration,
   worker/node, and last activity. Keep the status and selected row easy to scan.
5. Selecting a workflow opens its inspector without losing list filters or scroll.

### Workflow inspector

- Identity, status, last observed time, node, and copyable instance ID.
- Overview / Actions / Events tabs. Show an action waterfall where timings can be
  established, with a readable chronological list as the keyboard-accessible view.
- Selecting an action exposes input, result, or error; raw event details stay
  available for diagnosis. Keep distinct action attempts separate.
- Call-to-settlement elapsed time includes queueing and handling; label it honestly.
  Do not call it worker execution time without worker timing data.
- Payload panels support wrapping, scrolling, and explicit copy. Plain text remains
  selectable. Render payload content as text, never executable HTML.
- Missing input/result data says "Not recorded" and explains what is available.
  It must not look like an empty object, a successful null result, or a loading state.

### Workers and load

- Overall and per-node concurrency: in-flight actions / maximum in-flight actions.
- Worker pool size, queued dispatches, driven workflow runtimes, completion rate,
  dequeue latency, and action handling latency.
- Time series share the selected time window, units, and x-axis. Show gaps for
  missing samples; never interpolate across node restarts or label missing as zero.
- Completion rate comes from counter differences over elapsed time, per node boot.
  Aggregate histogram buckets before computing an aggregate percentile; never
  average node medians. Don't claim CPU/RAM utilization from action concurrency.
- Include last sample age and dropped-observation counts. Stale nodes retain their
  last values with a stale marker and are excluded from claims about current load.

## Common components

`src/components/ui/` contains shadcn primitives. `src/components/` contains shared
Waymark patterns. Feature-specific table columns and API calls stay with features.
Components accept values and callbacks; they do not fetch or invent domain data.

| Component                   | Responsibility                                                |
| --------------------------- | ------------------------------------------------------------- |
| `Button`, `Input`, `Select` | Consistent controls, focus, disabled states                   |
| `Badge`, `Tooltip`          | Supporting labels and optional explanations                   |
| `Tabs`, `Table`, `Sheet`    | Accessible navigation, data rows, mobile inspection           |
| `Skeleton`                  | Preserve layout while the first response loads                |
| `StatusBadge`               | One state vocabulary, icon, and semantic color everywhere     |
| `Metric`                    | Label, value, unit, and scope/freshness caption               |
| `CapacityBar`               | Used / available capacity, readable value, accessible meter   |
| `Timeline`                  | Named intervals, timing scale, selected item, keyboard access |
| `PayloadViewer`             | Read-only JSON/text, unavailable state, wrap and copy         |
| `EmptyState`                | Empty, filtered-empty, unavailable, and error explanations    |
| `AppShell`, `PageHeader`    | Navigation, page hierarchy, and common actions                |

Add primitives as their first consumer arrives. Use shadcn's native table until
sorting/virtualization requires more; a chart package is justified when the live
load screen needs coordinated axes and tooltips. Avoid a second component system.

## State, accessibility, and motion

- Preserve the previous response during refresh. A refresh error adds an inline
  notice with Retry and the last successful update time; it never becomes an empty
  workflow list. Distinguish no workflows from no observations in the chosen window.
- Pause polling in hidden tabs. Cancel superseded requests. Preserve selected
  rows and focus when data changes; don't reorder under the user's pointer.
- Use links for navigation, buttons for actions, proper table headers, and explicit
  labels for icon-only controls. Keyboard users must reach row details and payloads.
- Dialogs/sheets trap focus, close with Escape, and return focus to their trigger.
  Essential values and status labels remain visible without a tooltip.
- Minimum contrast: 4.5:1 for ordinary text; 3:1 for large text and important UI
  boundaries. Use a visible blue focus ring in both themes.
- Transitions: 120–160 ms for hover/selection, up to 180 ms for panels. No continuous
  decorative motion; disable movement under `prefers-reduced-motion`.
- Reference examples are explicitly marked as sample data and never mixed into live
  responses. An unavailable API must never silently fall back to demo numbers.

## Current API contract and implementation limits

All paths below are relative to `/api`. Confirmed against the current Rust routes.

| Need                 | Endpoint                                                                     | Available today                                                                            |
| -------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Workflow list        | `GET /observability-state/instances?from=…&to=…&limit=…&after=…`             | Instance ID, latest run, terminal outcome, last event; cursor-paginated by last activity   |
| Workflow detail      | `GET /observability-state/instances/{vm_id}`                                 | The same observed state for one instance                                                   |
| Action/event history | `GET /observability-events/vms/{vm_id}/timeline?limit=…&after=…`             | Oldest-first events, action names/modules, promise IDs, settlement kind and exception type |
| Current load         | `GET /essential-metrics/nodes/latest`                                        | Latest per-node concurrency, queue, pool size, counters, and latency histograms            |
| Historical load      | `GET /essential-metrics/nodes/{node_id}/series?from=…&to=…&bucket_seconds=…` | Bucketed per-node samples                                                                  |

### Status rules

Terminal workflow outcome takes precedence: `complete` → success;
`unhandled_exception` → failed. An un-stopped latest run is observed running,
subject to freshness. A driver error is a run failure, not proof that the workflow
has permanently failed. A cancelled driver run is not necessarily a cancelled
workflow. A rejected action can be caught or retried; it doesn't make its parent
workflow failed. Waiting/unknown needs an explicit label when the observations
cannot establish a terminal or active state.

### Gaps to resolve when wiring the full UI

- Workflow display names, application/environment grouping, and full start times
  are not in the instance response. Use IDs until metadata is exposed.
- The list is a page of activity within a time window, not a complete inventory of
  in-flight workflows. Global state counts and server-side state/search filters need
  a backend query; the UI must label page-local counts and filtering accordingly.
- Action arguments, returned values, and full exceptions are not recorded in the
  event summaries. Input/result inspection needs an API and persistence extension
  with explicit retention and redaction rules. Don't reconstruct payloads from names.
- Event capture can drop observations and retention can remove earlier history.
  Correlate actions within their driver run and promise identity, expose incomplete
  timelines, and don't assign invented durations or outcomes to unmatched events.
- Per-process CPU/memory, per-application grouping, and configured freshness intervals
  are not returned by these endpoints. Initial load views use the reported node
  metrics, and any freshness threshold must be documented.

## Sources

- User-provided monitoring screenshot: dense work area, in-row visualization,
  restrained surfaces, and a persistent detail pane.
- [shadcn/ui with Vite](https://ui.shadcn.com/docs/installation/vite)
- [shadcn/ui theme tokens](https://ui.shadcn.com/docs/theming)
