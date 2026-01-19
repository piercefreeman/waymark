# Unified Readiness Model

This document explains how Rappel decides when a node is runnable. The same model also defines our push-based scheduling behavior: completions push readiness forward; we never rescan the DAG to find work.

## Core principle

**Every node gets readiness tracking. A node is only enqueued when `completed_count == required_count`.**

That single rule applies to actions, barriers, joins, and workflow outputs.

---

## Vocabulary

- **State machine edges**: execution order (who can run after whom).
- **Data-flow edges**: variable writes into a node's inbox.
- **Inline nodes**: assignments, expressions, branches, returns.
- **Frontier nodes**: actions, barriers, outputs (places where inline traversal stops).
- **Readiness**: a per-node counter of completed predecessors.

---

## Push-based scheduling

When a node completes, we immediately propagate its effects:

- write its outputs into downstream inboxes
- increment readiness for downstream nodes
- enqueue any node that just became ready

We never do a global scan to ask "what is runnable now?". Each completion carries its own impact forward.

---

## Completion flow (high level)

1. Analyze the reachable subgraph via state machine edges.
2. Execute inline nodes in memory to update scope.
3. Collect inbox writes for frontier nodes.
4. Increment readiness for frontier nodes touched by this completion.
5. Enqueue any node whose readiness just reached the required count.

A sketch of the shape:

```rust
// Not real code, just the shape.
let subgraph = analyze_subgraph(completed_node_id, dag);
let scope = execute_inline(subgraph.inline_nodes, completed_result, inbox);
let writes = collect_inbox_writes(scope, subgraph.frontier_nodes);
let increments = compute_readiness_increments(subgraph.frontier_nodes);
commit_plan(writes, increments, maybe_complete_instance);
```

The plan is committed atomically so the system never observes a half-updated state.

---

## Frontier categories

Traversal stops at frontier nodes because they require coordination:

- **Action**: dispatched to Python workers.
- **Barrier**: waits for multiple predecessors (spread/parallel aggregators, joins).
- **Output**: workflow completion.

Join nodes with `required_count = 1` are treated as inline; joins with multiple predecessors become barriers.

---

## Example: fan-out with spread

```
items = @fetch_items()
results = spread items:item -> @process_item(item=item)
summary = @summarize(items=results)
```

What happens:

- `@fetch_items()` completes and pushes `items` to the spread node.
- The spread node creates N action instances (one per item).
- Each `@process_item` completion writes a result with a `spread_index`.
- The aggregator becomes ready once all N results arrive.
- The aggregator runs inline and pushes the ordered list to `@summarize`.

No scanning is needed; each completion only touches its downstream nodes.

---

## Loops and branches

Loops and branches are still just nodes and edges.

Brief IR sketch for a loop:

```
fn main(input: [items], output: [results]):
    results = []
    for item in items:
        processed = @process_item(item=item)
        results = results + [processed]
    return results
```

That loop expands into a small state machine. One iteration looks like this:

1. `loop_init` sets the internal index (inline).
2. `loop_cond` evaluates the guard and chooses continue vs break.
3. `loop_extract` assigns `item = items[__loop_i]` (inline).
4. `@process_item` is a frontier action and is dispatched to a worker.
5. On completion, we write `processed` to inboxes and increment readiness for the inline `results = results + [processed]` assignment.
6. The assignment runs inline, updates `results` in scope, and pushes the new value to downstream inboxes (including `loop_exit`).
7. `loop_incr` updates `__loop_i` and the back-edge routes back to `loop_cond`.
8. If the guard fails, the break edge routes to `loop_exit` (a join, often `required_count = 1`).

- A loop head is modeled as a `branch` node with guarded edges.
- Loop back-edges are marked and do not count toward readiness.
- Each iteration increments readiness for the next step and resets readiness where needed.
- Branch joins become barriers only when multiple paths can converge.

This keeps loops in the same push-based model without a separate scheduler mode.

---

## Guarantees (conceptual)

- **No duplicate enqueues**: nodes are enqueued only when readiness hits the required count.
- **No stale inbox**: writes happen alongside readiness increments.
- **Crash safety**: the completion plan is committed atomically.
- **Idempotent completion**: each completion is tied to a delivery token.

---

## Why this scales

Push-based scheduling costs **O(d)** per completion, where `d` is the number of downstream nodes touched by that completion. There is no quadratic blow-up as workflows grow.
