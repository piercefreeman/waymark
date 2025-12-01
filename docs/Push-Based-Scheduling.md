# Push-Based Scheduling Architecture

This document explains Rappel's push-based scheduling system, which enables O(1) node queuing.

## Overview

Rappel uses a **push-based event-driven scheduler** rather than a pull-based polling approach. When a node completes, it immediately pushes its results to downstream nodes and increments their dependency counters. This eliminates the need to re-scan completed actions on every scheduling cycle to build up variable dependencies within the workflow `run()` implementation.

## Complex Workflow Example

Here's a realistic Python workflow that exercises fan-out parallelism, multi-action loops, and final aggregation:

```python
from rappel import Workflow, action, workflow
from pydantic import BaseModel

class Item(BaseModel):
    id: int
    value: str

class ProcessedItem(BaseModel):
    id: int
    hash: str
    score: int

class ChunkResult(BaseModel):
    chunk_id: int
    total: int
    digest: str

@action(name="fetch_items")
async def fetch_items(count: int) -> list[Item]:
    return [Item(id=i, value=f"item_{i}") for i in range(count)]

@action(name="process_item")
async def process_item(item: Item) -> ProcessedItem:
    # CPU work happens here
    return ProcessedItem(id=item.id, hash=f"hash_{item.id}", score=item.id * 10)

@action(name="validate_chunk")
async def validate_chunk(chunk_id: int, items: list[ProcessedItem]) -> bool:
    return all(item.score > 0 for item in items)

@action(name="aggregate_chunk")
async def aggregate_chunk(chunk_id: int, items: list[ProcessedItem], is_valid: bool) -> ChunkResult:
    total = sum(item.score for item in items) if is_valid else 0
    return ChunkResult(chunk_id=chunk_id, total=total, digest=f"chunk_{chunk_id}")

@action(name="finalize")
async def finalize(results: list[ChunkResult]) -> int:
    return sum(r.total for r in results)

@workflow
class DataPipelineWorkflow(Workflow):
    name = "data_pipeline"
    concurrent = True

    async def run(self, fan_out: int = 8, loop_iters: int = 4) -> int:
        # Phase 1: Setup
        items = await fetch_items(count=fan_out)

        # Phase 2: Fan-out (parallel processing)
        processed: list[ProcessedItem] = []
        for item in items:
            result = await process_item(item)  # Each runs in parallel
            processed.append(result)

        # Phase 3: Loop with multi-action body
        chunk_size = len(processed) // loop_iters
        results: list[ChunkResult] = []

        for chunk_id in range(loop_iters):
            chunk = processed[chunk_id * chunk_size:(chunk_id + 1) * chunk_size]

            # Multi-action loop body: validate -> aggregate
            is_valid = await validate_chunk(chunk_id=chunk_id, items=chunk)
            chunk_result = await aggregate_chunk(
                chunk_id=chunk_id,
                items=chunk,
                is_valid=is_valid
            )
            results.append(chunk_result)

        # Phase 4: Final aggregation
        return await finalize(results)
```

## DAG Structure

The workflow compiler parses the AST and generates this DAG structure:

```
                          ┌─────────────────┐
                          │   fetch_items   │
                          │   (node_0)      │
                          └────────┬────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
              ▼                    ▼                    ▼
       ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
       │process_item │      │process_item │  ... │process_item │
       │  (node_1)   │      │  (node_2)   │      │  (node_8)   │
       └──────┬──────┘      └──────┬──────┘      └──────┬──────┘
              │                    │                    │
              └────────────────────┼────────────────────┘
                                   │ (all 8 converge)
                                   ▼
                          ┌─────────────────┐
                          │  iter_source    │  ← Generates __iter_loop0 = range(4)
                          │   (node_9)      │    Initializes results = []
                          └────────┬────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
              ┌─────│        loop_head             │◄────────────┐
              │     │   (synthetic LOOP_HEAD)      │             │
              │     │   node_type = LOOP_HEAD      │             │
              │     └──────────────┬───────────────┘             │
              │                    │                             │
         BREAK edge          CONTINUE edge                   BACK edge
              │                    │                             │
              │                    ▼                             │
              │           ┌─────────────────┐                    │
              │           │ validate_chunk  │                    │
              │           │   (node_10)     │                    │
              │           └────────┬────────┘                    │
              │                    │                             │
              │                    ▼                             │
              │           ┌─────────────────┐                    │
              │           │aggregate_chunk  │                    │
              │           │   (node_11)     │────────────────────┘
              │           └─────────────────┘
              │
              ▼
     ┌─────────────────┐
     │   finalize      │
     │   (node_12)     │
     └─────────────────┘
```

### Edge Types

The DAG uses explicit edge types for control flow:

| Edge Type | Description |
|-----------|-------------|
| `FORWARD` | Normal dependency flow |
| `CONTINUE` | loop_head → body (iterator not exhausted) |
| `BREAK` | loop_head → exit (iterator exhausted) |
| `BACK` | body_tail → loop_head (next iteration) |

### Synthetic LOOP_HEAD Node

Loop heads are synthetic nodes evaluated by the scheduler without dispatching to workers:

- `node_type = NODE_TYPE_LOOP_HEAD`
- Contains `LoopHeadMeta` with iterator source, preamble ops, body nodes
- Executes preamble operations (set index, set iterator value) in O(1)

## Data Flow: Step by Step

### 1. Instance Initialization

When a workflow starts, the scheduler initializes ready state for all nodes:

```sql
-- Create ready state for all nodes
INSERT INTO node_ready_state (instance_id, node_id, deps_required, deps_satisfied)
VALUES
  (instance_id, 'node_0', 0, 0),  -- fetch_items: no deps, immediately ready
  (instance_id, 'node_1', 1, 0),  -- process_item[0]: waits for node_0
  (instance_id, 'node_2', 1, 0),  -- process_item[1]: waits for node_0
  ...
  (instance_id, 'loop_head', 1, 0),  -- waits for iter_source
  ...

-- Initialize eval context with workflow input
INSERT INTO instance_eval_context (instance_id, context_json, exceptions_json)
VALUES (instance_id, '{"fan_out": 8, "loop_iters": 4}', '{}');
```

### 2. Node Completion (Push Logic)

When `fetch_items` completes with `items = [...]`, the `complete_node_push_tx` function:

```rust
// 1. Mark node completed
UPDATE node_ready_state SET is_completed = TRUE
WHERE instance_id = $1 AND node_id = 'node_0';

// 2. Extract variables and update eval context
let vars = extract_variables_from_result(node, &payload);  // {"items": [...]}
UPDATE instance_eval_context
SET context_json = context_json || '{"items": [...]}'
WHERE instance_id = $1;

// 3. Push payload to downstream nodes
let downstream = get_downstream_nodes("node_0", dag);  // [node_1..node_8, node_9]
for downstream_id in downstream {
    INSERT INTO node_pending_context (instance_id, node_id, source_node_id, variable, payload)
    VALUES ($1, downstream_id, 'node_0', 'items', payload_bytes);
}

// 4. Increment deps_satisfied for all downstream (SINGLE QUERY)
UPDATE node_ready_state
SET deps_satisfied = deps_satisfied + 1
WHERE instance_id = $1 AND node_id = ANY(['node_1', 'node_2', ..., 'node_9']);
```

### 3. Finding Ready Nodes

Ready nodes are found with an O(1) indexed query:

```sql
SELECT node_id FROM node_ready_state
WHERE instance_id = $1
  AND deps_satisfied >= deps_required
  AND is_queued = FALSE
  AND is_completed = FALSE;
```

### 4. Loop Head Evaluation

When `loop_head` becomes ready, the scheduler evaluates it locally:

```rust
// Check if more iterations
let iterator: Vec<Value> = eval_ctx.get("__iter_loop0");
let current_index: i32 = loop_state.current_index;

if current_index < iterator.len() {
    // Execute preamble ops (scheduler-side, O(1)):
    eval_ctx.insert("chunk_id", current_index);
    eval_ctx.insert("chunk", iterator[current_index]);

    // Trigger CONTINUE edge → body_entry nodes
    increment_deps_satisfied(["node_10"]);  // validate_chunk
} else {
    // Iterator exhausted → BREAK edge
    increment_deps_satisfied(["node_12"]);  // finalize

    // Export accumulated results to eval context
    eval_ctx.insert("results", accumulators["results"]);
}
```

### 5. Back Edge Handling

When the loop body tail completes, back edge handling:

```rust
// 1. Update accumulator
accumulators["results"].push(chunk_result);

UPDATE loop_iteration_state
SET current_index = current_index + 1, accumulators = $3
WHERE instance_id = $1 AND node_id = 'loop_head';

// 2. Re-enable loop_head for next iteration
UPDATE node_ready_state
SET is_queued = FALSE, is_completed = FALSE, deps_satisfied = deps_required
WHERE instance_id = $1 AND node_id = 'loop_head';

// 3. Reset body nodes for next iteration
UPDATE node_ready_state
SET is_queued = FALSE, is_completed = FALSE, deps_satisfied = 0
WHERE instance_id = $1 AND node_id = ANY(['node_10', 'node_11']);
```

## Complexity Analysis

### Push-Based (Current)

Per node completion:

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Mark completed | O(1) | Primary key update |
| Update eval context | O(k) | k = variables produced (typically 1-2) |
| Push to downstream | O(d) | d = downstream count (typically 1-3) |
| Increment deps | O(1) | Single UPDATE with ANY() |
| Find ready nodes | O(1) | Uses partial index |

**Total: O(1) amortized per completion**

### Scaling Comparison

| Workflow Nodes | Pull-Based | Push-Based | Speedup |
|----------------|------------|------------|---------|
| 10 nodes       | O(100)     | O(10)      | 10x     |
| 50 nodes       | O(2,500)   | O(50)      | 50x     |
| 100 nodes      | O(10,000)  | O(100)     | 100x    |
| 500 nodes      | O(250,000) | O(500)     | 500x    |

### Loop Iteration Complexity

For a loop with `i` iterations and `a` actions per iteration:

| Approach | Per Iteration | Total |
|----------|---------------|-------|
| Pull     | O(completed)  | O(i² × a) |
| Push     | O(1) + O(a)   | O(i × a) |

**Example**: 32 iterations × 3 actions = 96 actions
- Pull: ~4,600 operations (quadratic)
- Push: ~96 operations (linear)
