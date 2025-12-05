# Unified Readiness Model

## Overview

This document describes the architecture for workflow node execution with transactional consistency and crash recovery guarantees.

### Core Principle

**Every node gets readiness tracking. A node is only enqueued when `completed_count == required_count`.**

No special cases. No branching logic for single vs multiple predecessors. One unified path.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     COMPLETION HANDLER                          │
│                                                                 │
│  1. Traverse DAG to find inline subgraph + frontier nodes       │
│  2. Batch fetch inbox: SELECT WHERE node_id IN (all_nodes)      │
│  3. Execute inline nodes in memory, accumulate scope            │
│  4. Single transaction:                                         │
│       - Mark current node complete (fail if stale)              │
│       - Write inbox entries for frontier nodes                  │
│       - Increment readiness for frontier nodes                  │
│       - Enqueue any node where completed == required            │
│       - Mark instance complete (if output, only if running)     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                       POLLING LOOP                              │
│                                                                 │
│  SELECT FROM action_queue WHERE status='queued'                 │
│    - If action → dispatch to worker                             │
│    - If barrier → process aggregation inline, re-enter handler  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Node Lifecycle

```
                    ┌──────────┐
                    │  (none)  │  Node not yet reachable
                    └────┬─────┘
                         │ First predecessor completes
                         ▼
                    ┌──────────┐
                    │ Tracking │  node_readiness row exists
                    │ (1 of N) │  completed_count < required_count
                    └────┬─────┘
                         │ More predecessors complete
                         ▼
                    ┌──────────┐
                    │  Ready   │  completed_count == required_count
                    │ (N of N) │  → INSERT into action_queue
                    └────┬─────┘
                         │ Poller picks up
                         ▼
                    ┌──────────┐
                    │ Running  │  status = 'dispatched'
                    └────┬─────┘
                         │ Completion handler
                         ▼
                    ┌──────────┐
                    │ Complete │  status = 'completed'
                    └──────────┘
```

---

## Guarantees

| Property | How It's Guaranteed |
|----------|---------------------|
| No duplicate enqueues | Node only enqueued when `completed == required` (exactly once) |
| No stale inbox | Inbox writes in same transaction as readiness increment |
| No lost data | Single transaction = all or nothing |
| Crash recovery | Incomplete transaction rolls back; node stays queued |
| Idempotent completion | `WHERE status='dispatched' AND delivery_token=?` |

---

## Completion Handler Steps

### Step 1: Analyze Subgraph (Pure DAG Traversal)

Starting from the completed node, traverse the DAG via StateMachine edges to find:

- **Inline nodes**: Assignments, expressions, control flow (can execute immediately)
- **Frontier nodes**: Actions, barriers, output (where traversal stops)

```rust
struct SubgraphAnalysis {
    inline_nodes: Vec<String>,      // Nodes to execute in memory
    frontier_nodes: Vec<FrontierNode>, // Nodes needing inbox + readiness
    all_node_ids: HashSet<String>,  // Union for batch inbox fetch
}

struct FrontierNode {
    node_id: String,
    category: FrontierCategory,     // Action, Barrier, or Output
    required_count: i32,            // Number of StateMachine predecessors
}
```

Traversal stops when encountering:
- `action_call` nodes (need worker execution)
- `aggregator` nodes (barriers waiting for spread results)
- `output`/`return` nodes (workflow completion)

### Step 2: Batch Fetch Inbox (Single Query)

Fetch all existing inbox data for nodes in the subgraph:

```sql
SELECT target_node_id, variable_name, value, spread_index
FROM node_inputs
WHERE instance_id = $1 AND target_node_id = ANY($2)
ORDER BY target_node_id, spread_index NULLS FIRST, created_at
```

Returns `HashMap<node_id, HashMap<variable_name, value>>`.

### Step 3: Execute Inline Subgraph (Pure Computation)

Using the completed node's result and fetched inbox data:

1. Initialize scope with completed node's result
2. Traverse inline nodes in topological order
3. Execute each inline node, accumulating results in scope
4. Collect inbox writes for frontier nodes via DataFlow edges
5. Build the completion plan

```rust
struct CompletionPlan {
    // Completion details
    completed_node_id: String,
    completed_action_id: ActionId,
    delivery_token: Uuid,
    success: bool,
    result_payload: Vec<u8>,

    // Writes for frontier nodes
    inbox_writes: Vec<InboxWrite>,

    // Readiness increments (one per frontier node we're a predecessor of)
    readiness_increments: Vec<ReadinessIncrement>,

    // Workflow completion (if output reached)
    instance_completion: Option<InstanceCompletion>,
}
```

### Step 4: Execute Transaction (Single Atomic Operation)

All database writes happen in one transaction:

```sql
BEGIN;

-- 1. Mark current node complete (idempotent guard)
UPDATE action_queue
SET status = 'completed', success = $success, result_payload = $result, completed_at = NOW()
WHERE id = $id AND delivery_token = $token AND status = 'dispatched';
-- If rows_affected = 0 → ROLLBACK (stale/duplicate completion)

-- 2. Write inbox entries for all frontier nodes
INSERT INTO node_inputs (instance_id, target_node_id, variable_name, value, source_node_id, spread_index)
VALUES (...), (...), (...);

-- 3. For each frontier node: increment readiness, enqueue if ready
FOR EACH frontier_node:
    INSERT INTO node_readiness (instance_id, node_id, required_count, completed_count)
    VALUES ($instance, $node, $required, 1)
    ON CONFLICT (instance_id, node_id)
    DO UPDATE SET completed_count = node_readiness.completed_count + 1, updated_at = NOW()
    RETURNING completed_count, required_count;

    IF completed_count == required_count THEN
        INSERT INTO action_queue (instance_id, node_id, node_type, module_name, action_name, dispatch_payload, ...)
        VALUES (...);
    END IF;

-- 4. Complete workflow instance (if output node reached, only if still running)
UPDATE workflow_instances
SET status = 'completed', result_payload = $result, completed_at = NOW()
WHERE id = $instance AND status = 'running';

COMMIT;
```

---

## Polling Loop

The polling loop dispatches queued nodes:

```rust
loop {
    // Fetch queued nodes (both actions and barriers)
    let nodes = db.dispatch_runnable_nodes(limit).await?;

    for node in nodes {
        match node.node_type {
            "action" => {
                // Dispatch to Python worker
                // On completion → re-enter completion handler
                worker_pool.dispatch(node).await;
            }
            "barrier" => {
                // Process aggregation: read spread results from inbox
                // Then re-enter completion handler with aggregated result
                let spread_results = db.read_inbox_for_aggregator(node.node_id).await?;
                let aggregated = aggregate_results(spread_results);
                completion_handler.handle(node, aggregated).await?;
            }
        }
    }

    sleep(poll_interval).await;
}
```

---

## Database Schema

### action_queue (extended)

```sql
ALTER TABLE action_queue ADD COLUMN node_type TEXT DEFAULT 'action';
-- node_type IN ('action', 'barrier')
```

### node_readiness (existing, unchanged)

```sql
CREATE TABLE node_readiness (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id UUID NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    node_id TEXT NOT NULL,
    required_count INTEGER NOT NULL,
    completed_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (instance_id, node_id)
);
```

### node_inputs (existing, unchanged)

```sql
CREATE TABLE node_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    instance_id UUID NOT NULL REFERENCES workflow_instances(id) ON DELETE CASCADE,
    target_node_id TEXT NOT NULL,
    variable_name TEXT NOT NULL,
    value JSONB NOT NULL,
    source_node_id TEXT NOT NULL,
    spread_index INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## Node Categories

### Inline Nodes (execute in memory)

- `assignment`: Variable assignment (`x = 5`)
- `expression`: Computed value (`x + y`)
- `conditional`: Control flow branch evaluation (`if x > 0`)
- `input`: Function entry boundary
- `fn_call`: Internal function expansion

### Frontier Nodes (stop traversal, need queueing)

- `action_call`: External worker execution
- `aggregator`: Barrier waiting for spread results
- `output`/`return`: Workflow completion

---

## Spread/Parallel Pattern

For spread operations (`spread items:item -> @action()`):

1. **Spread node creates N actions**: Each with `node_id = "spread_action_1[0]"`, `"spread_action_1[1]"`, etc.
2. **Aggregator initialized**: `node_readiness` row with `required_count = N`
3. **Each spread action completes**: Writes result to aggregator's inbox with `spread_index`
4. **Last completion**: `completed_count == required_count` → enqueue aggregator as barrier
5. **Barrier processing**: Read inbox, aggregate by `spread_index` order, continue DAG traversal

---

## Error Handling

### Stale Completion (duplicate delivery)

```rust
let rows = UPDATE action_queue SET status='completed'
           WHERE id=$id AND delivery_token=$token AND status='dispatched';

if rows.rows_affected() == 0 {
    tx.rollback();
    return Ok(CompletionResult::Stale);
}
```

### Readiness Overflow (bug detection)

```rust
let (completed, required) = /* increment result */;

if completed > required {
    tx.rollback();
    return Err(DbError::ReadinessOverflow { node_id });
}
```

### Instance Already Complete (parallel paths to output)

```rust
UPDATE workflow_instances SET status='completed'
WHERE id=$id AND status='running';
-- If rows_affected = 0, another path already completed it (ok)
```

---

## Migration Path

### Phase 1: Database Layer
- Add `node_type` column to `action_queue`
- Implement `batch_read_inbox()`
- Implement `execute_completion_plan()` with single transaction

### Phase 2: Subgraph Analysis
- Implement `analyze_subgraph()` - pure DAG traversal
- Compute `required_count` from StateMachine predecessor edges

### Phase 3: Completion Handler
- Replace current `process_completion_task` with 4-step flow
- Remove all inline aggregator processing
- Remove non-transactional `write_batch()`

### Phase 4: Polling Loop
- Update `dispatch_actions` → `dispatch_runnable_nodes`
- Add barrier processing path

### Phase 5: Testing
- Unit tests for subgraph analysis
- Integration tests for completion plans
- Crash recovery tests (kill mid-transaction)
- Parallel execution stress tests
