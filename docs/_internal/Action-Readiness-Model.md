# Unified Readiness Model

This document explains how Waymark decides when a node is runnable. The same model also defines our push-based scheduling behavior: completions push readiness forward; we never rescan the DAG to find work.

## Core principle

**Every node gets readiness tracking. A node is only enqueued when all required predecessors have completed.**

That single rule applies to actions, barriers, joins, and workflow outputs.

---

## Vocabulary

- **State machine edges**: execution order (who can run after whom).
- **Data-flow edges**: variable writes from one node to another.
- **Inline nodes**: assignments, expressions, branches, returns.
- **Frontier nodes**: actions, barriers, outputs (places where inline traversal stops).
- **Readiness**: determined by predecessor completion status.

---

## Push-based scheduling

When a node completes, we immediately propagate its effects:

- store its result in the execution graph
- update variables in the workflow scope
- check if downstream nodes are now ready
- add newly ready nodes to the ready queue

We never do a global scan to ask "what is runnable now?". Each completion carries its own impact forward.

---

## Completion flow (high level)

1. Mark the completed node in the execution graph.
2. Store the result and update workflow variables.
3. Evaluate guards on outgoing edges.
4. For each successor, check if all predecessors are complete.
5. Add ready successors to the ready queue.

All state lives in the execution graph. Changes are applied in memory and batched to the database periodically.

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

- `@fetch_items()` completes and stores `items` in the workflow scope.
- The spread node creates N action instances (one per item).
- Each `@process_item` completion stores a result with a `spread_index`.
- The barrier becomes ready once all N results arrive.
- The barrier aggregates results and stores the ordered list.
- `@summarize` becomes ready and receives the aggregated results.

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
5. On completion, we store `processed` and update `results`.
6. The assignment runs inline, updates `results` in the workflow scope.
7. `loop_incr` updates `__loop_i` and the back-edge routes back to `loop_cond`.
8. If the guard fails, the break edge routes to `loop_exit`.

- A loop head is modeled as a `branch` node with guarded edges.
- Loop back-edges are marked and do not count toward readiness.
- Each iteration updates state and resets node status where needed.
- Branch joins become barriers only when multiple paths can converge.

This keeps loops in the same push-based model without a separate scheduler mode.

---

## Guarantees (conceptual)

- **No duplicate enqueues**: nodes are enqueued only when all predecessors complete.
- **No stale data**: results are stored alongside status updates.
- **Crash safety**: execution graphs are persisted periodically; reclaimed instances resume from last checkpoint.
- **Durable action history**: successful action outputs are appended by `execution_id` and `attempt`, then rehydrated on reclaim.

---

## Why this scales

Push-based scheduling costs **O(d)** per completion, where `d` is the number of downstream nodes touched by that completion. There is no quadratic blow-up as workflows grow.
