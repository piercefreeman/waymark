# DAG Conversion

We start with the parsed IR (Waymark AST) and produce a graph the runner can execute. It is a DAG for most workflows, with a single back-edge for loops. The conversion step is where control flow and data flow are made explicit.

## Mental model

- Nodes are steps: action calls, assignments, branches, joins, aggregators, and function boundaries.
- State machine edges encode "what can run after this completes".
- Data-flow edges encode "which variable values should be written into a node's inbox".

A node is runnable when its state machine predecessors have completed and its required inbox values are present. See `docs/Action-Readiness-Model.md` for readiness rules.

## Two-phase conversion

1. **Per-function subgraphs.** Each function becomes an isolated subgraph with `input` and `output` boundary nodes. Calls inside the function become `fn_call` nodes that capture kwargs.
2. **Expansion + global wiring.** Starting from the entry function (prefer `main`, then the first non-internal function), we inline helper functions, remap exception edges, and recompute global data-flow edges so values defined inside helpers can reach downstream nodes.

We validate the result (no dangling edges, no invalid loop wiring, no stray output edges) before returning the DAG.

## Node types you will see

- `input` / `output`: function boundaries.
- `action_call`: delegated work sent to Python workers.
- `assignment` / `expression` / `return`: inline work executed in the runner.
- `branch`: decision points for `if` / `elif` / `else` and loop conditions.
- `join`: merge points; often `required_count = 1` to avoid unnecessary barriers.
- `parallel`: entry node for parallel blocks.
- `aggregator`: barrier node that waits for spread/parallel results.
- `fn_call`: function call placeholders before expansion (external calls may remain).

## Edge types

### State machine edges

Execution order edges. They can carry:

- `guard_expr` for branch and loop conditions.
- `condition` labels like `success` or `else`.
- `exception_types` for try/except routing.
- `is_loop_back` for loop back-edges.

### Data-flow edges

Per-variable edges. `(src, dst, var)` means "write `var` from src into dst's inbox". We avoid pushing stale values by only wiring from the most recent definition along the execution order. Join nodes do not define values; they only mark where paths converge.

## Conversions that matter

### Straight-line code

Assignments and expressions become inline nodes. Action calls become `action_call` nodes. State machine edges preserve order; data-flow edges carry the variables used later.

### Function boundaries and returns

Each function has explicit input and output nodes. All return nodes connect to the output boundary so early returns still terminate the function. During expansion, helper functions are inlined and their nodes are prefixed; input nodes are stripped for inlined functions so the caller supplies inputs via data-flow.

### Conditionals

A `branch` node fans out to guarded edges for `if` / `elif` and an `else` edge for the default. If at least one branch can continue, we add a `join` node with `required_count = 1` so the next step sees a single merge point.

### Try/except

Try bodies are flattened. Every node inside the try body can emit exception edges to handlers. Success edges flow to a join node. If the handler binds an exception variable, we insert an assignment from `__waymark_exception__` before the handler body.

### For/while loops

Loops expand into a small state machine:

- `for` loops create `loop_init`, `loop_cond` (branch), `loop_extract`, body, `loop_incr`, and a `loop_exit` join.
- `while` loops create `loop_cond`, body, `loop_continue` (for continue wiring), and `loop_exit`.

Back-edges are marked with `is_loop_back` so readiness ignores them.

### Spread + parallel

- **spread** turns into a spread action node plus an `aggregator`. Each action result is written with a `spread_index`; the aggregator reads and emits an ordered list.
- **parallel** blocks create a `parallel` entry node, one node per call, and an `aggregator` that waits for all results.

## Visualizing the graph

Use the `dag-visualize` binary to render HTML:

```bash
cargo run --bin dag-visualize -- path/to/workflow.py -o dag.html
```

Solid lines are control flow; dotted lines are data flow.
