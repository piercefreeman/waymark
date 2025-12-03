# Rappel Language Specification

## Motivation

Going from Python directly to a DAG results in a heavy logical burden on the Python AST parser, plus is hard for us to validate at build time whether our DAG will actually execute. We'd like to catch errors early.

Rappel is an intermediate representation (IR) where by virtue of being built we can assert that our DAG will be able to process it. This also opens up the extension for the `run()` workflow to be written in other languages in the future so long as they can compile down to our IR.

We imagine we won't ever touch this grammar directly (preferring `code -> AST -> IR -> DAG`), but it's useful to sketch out the full range of primitives that we expect to have to support.

## Design Principles

1. **Immutable Variables**: All variables are assigned once and never mutated
2. **Explicit I/O**: Functions declare their inputs and outputs upfront
3. **First-Class Actions**: External calls (`@actions`) are the unit of durable execution
4. **No Closures**: No nested functions or captured state
5. **Serializable State**: All values must be JSON-serializable for distributed execution

---

## Lexical Elements

```
IDENT       = [a-zA-Z_][a-zA-Z0-9_]*
INT         = [0-9]+
FLOAT       = [0-9]+ "." [0-9]+
STRING      = '"' [^"]* '"'
BOOL        = "True" | "False"
COMMENT     = "#" [^\n]*
DURATION    = [0-9]+ ("s" | "m" | "h")   # e.g., 30s, 2m, 1h
```

---

## Grammar (EBNF)

### Top-Level

```ebnf
(* Program is a collection of function definitions *)
program         = function_def+ ;

(* Function definition with explicit input/output declarations *)
function_def    = "fn" IDENT "(" io_decl ")" ":" body ;
io_decl         = "input:" "[" ident_list? "]" "," "output:" "[" ident_list? "]" ;
ident_list      = IDENT ("," IDENT)* ;
body            = INDENT statement+ DEDENT ;
```

### Statements

```ebnf
statement       = assignment
                | action_call
                | spread_action
                | parallel_block
                | for_loop
                | conditional
                | try_except
                | return_stmt
                | expr_stmt ;

(* Assignment - binds a value to a variable *)
assignment      = IDENT "=" expr ;

(* Multi-assignment for tuple unpacking *)
multi_assign    = IDENT ("," IDENT)+ "=" expr ;

(* Expression as statement *)
expr_stmt       = expr ;

(* Return statement *)
return_stmt     = "return" expr? ;
```

### Actions

```ebnf
(* Action call - the fundamental unit of durable execution *)
action_call     = "@" IDENT "(" kwargs? ")" policy_bracket* ;
kwargs          = kwarg ("," kwarg)* ;
kwarg           = IDENT "=" expr ;

(* Policy brackets - retry policies and/or timeout *)
policy_bracket  = retry_policy | timeout_policy ;

(* Retry policy - error handling for actions *)
retry_policy    = "[" (exception_spec "->")? retry_params "]" ;
exception_spec  = IDENT | "(" IDENT ("," IDENT)* ")" ;
retry_params    = retry_param ("," retry_param)* ;
retry_param     = "retry" ":" INT
                | "backoff" ":" (INT | DURATION) ;

(* Timeout policy - separate from retry *)
timeout_policy  = "[" "timeout" ":" (INT | DURATION) "]" ;

(* Spread action - parallel execution over a collection *)
spread_action   = "spread" expr ":" IDENT "->" action_call ;

(* Parallel block - concurrent execution of multiple calls *)
parallel_block  = IDENT "=" "parallel" ":" INDENT call_list DEDENT
                | "parallel" ":" INDENT call_list DEDENT ;
call_list       = (action_call | function_call)+ ;
```

### Control Flow

```ebnf
(* For loop - iteration over collection with optional unpacking *)
for_loop        = "for" loop_vars "in" expr ":" body ;
loop_vars       = IDENT ("," IDENT)* ;

(* Conditional branching *)
conditional     = if_branch elif_branch* else_branch? ;
if_branch       = "if" expr ":" body ;
elif_branch     = "elif" expr ":" body ;
else_branch     = "else:" body ;

(* Exception handling for actions *)
try_except      = "try" ":" body except_handler+ ;
except_handler  = "except" exception_types? ":" body ;
```

### Expressions

```ebnf
expr            = literal
                | variable
                | binary_op
                | unary_op
                | list_expr
                | dict_expr
                | index_access
                | dot_access
                | function_call
                | action_call ;

literal         = INT | FLOAT | STRING | BOOL ;
variable        = IDENT ;

(* Binary operations *)
binary_op       = expr operator expr ;
operator        = "+" | "-" | "*" | "/" | "//" | "%"
                | "==" | "!=" | "<" | ">" | "<=" | ">="
                | "and" | "or" ;

(* Unary operations *)
unary_op        = ("not" | "-") expr ;

(* Collection literals *)
list_expr       = "[" (expr ("," expr)*)? "]" ;
dict_expr       = "{" (dict_entry ("," dict_entry)*)? "}" ;
dict_entry      = (STRING | IDENT) ":" expr ;

(* Access operations *)
index_access    = expr "[" expr "]" ;
dot_access      = expr "." IDENT ;

(* Function call (non-action) *)
function_call   = IDENT "(" kwargs? ")" ;
```

---

## Built-in Functions

Rappel provides several built-in functions for common operations:

| Function | Description | Example |
|----------|-------------|---------|
| `range(n)` | Generate list from 0 to n-1 | `range(5)` → `[0, 1, 2, 3, 4]` |
| `range(start, stop)` | Generate list from start to stop-1 | `range(2, 5)` → `[2, 3, 4]` |
| `range(start, stop, step)` | Generate list with step | `range(0, 10, 2)` → `[0, 2, 4, 6, 8]` |
| `enumerate(list)` | List of `[index, item]` pairs | `enumerate(["a", "b"])` → `[[0, "a"], [1, "b"]]` |
| `len(collection)` | Length of list, dict, or string | `len([1, 2, 3])` → `3` |

---

## Semantic Constraints

### Functions

- Functions must declare all inputs and outputs explicitly
- All function calls must use keyword arguments (no positional args)
- Functions are not first-class values (no passing functions as arguments)
- No recursion allowed (DAG must be acyclic)

### Variables

- Variables are immutable - each name can only be assigned once per scope
- All variables must be defined before use
- Variable names must not shadow input parameters

### Actions

- Actions are marked with `@` prefix to distinguish from regular function calls
- Actions are the unit of durable execution - they run on workers and results are persisted
- Action arguments must be serializable expressions
- Actions may have a target to capture the return value: `result = @action(...)`

### Built-in Actions

| Action | Description | Example |
|--------|-------------|---------|
| `@sleep(duration=n)` | Durable sleep for n seconds | `@sleep(duration=60)` |

### Retry Policies

Actions can have retry policies for automatic error recovery. Timeout is specified in a separate bracket since it's independent of exception types:

```rappel
# Catch all exceptions, retry up to 3 times with 60s backoff
result = @risky_action() [retry: 3, backoff: 60]

# Catch specific exception using -> syntax
result = @network_call() [NetworkError -> retry: 5, backoff: 2m]

# Multiple exception types in a tuple
result = @api_call() [(ValueError, KeyError) -> retry: 3, backoff: 30s]

# Multiple policies for different exception types
result = @api_call() [RateLimitError -> retry: 10, backoff: 1m] [NetworkError -> retry: 3, backoff: 30s]

# Timeout is a separate bracket (not tied to exception type)
result = @slow_action() [retry: 3, backoff: 60] [timeout: 2m]
```

Retry parameters (in retry policy brackets):
- `retry: N` - Maximum number of retry attempts
- `backoff: DURATION` - Base backoff duration (exponential: backoff * 2^attempt)

Timeout (in separate bracket):
- `[timeout: DURATION]` - Action timeout before considering it failed

Duration formats: bare numbers are seconds, or use `30s`, `2m`, `1h`.

### Spread Actions

- `spread collection:item -> @action(...)` executes the action for each item in parallel
- The loop variable (`item`) is scoped to the action call
- Results are collected into a list in original order
- All spread iterations are independent (no dependencies between them)

### Parallel Blocks

- `parallel:` executes multiple action/function calls concurrently
- Results are collected into a list in declaration order
- All calls in a parallel block must be independent

```rappel
# Concurrent execution with result collection
results = parallel:
    @fetch_user(id=1)
    @fetch_user(id=2)
    @fetch_user(id=3)

# Without result assignment
parallel:
    @send_notification(user=1)
    @send_notification(user=2)
```

### Conditionals

- All branches that assign to a variable must assign to the same variable name
- Guards are expressions that evaluate to boolean
- Guards must not contain action calls

### For Loops

- Loop variable is scoped to the loop body
- Multiple loop variables supported for unpacking: `for i, item in enumerate(items)`
- Loop body can contain any statements including nested loops
- Cannot break or continue (full iteration required for DAG)

### Try/Except Blocks

- Try blocks wrap action calls for error handling
- Except handlers can catch specific exception types or all exceptions
- Each branch (try and except) contains action calls that are durably executed

```rappel
try:
    result = @risky_action()
except NetworkError:
    result = @fallback_action()
except:
    result = @default_handler()
```

---

## DAG Translation

The IR maps to DAG nodes as follows:

| IR Construct | DAG Representation |
|--------------|-------------------|
| `fn` definition | Subgraph with input/output boundary nodes |
| `x = expr` | Assignment node (inline computation) |
| `@action(...)` | Action node (delegated to worker) |
| `spread c:i -> @a(...)` | Spread node → N action nodes → Aggregator node |
| `parallel: ...` | Parallel node → N call nodes → Aggregator node |
| `if/elif/else` | Condition node → Branch nodes → Merge node |
| `for x in c:` | Iterator → Loop body subgraph → Collector |
| `try/except` | Try node → [Success: next] or [Exception: handler] |
| `return expr` | Output node (function boundary) |

### Edge Types

| Edge Type | Description |
|-----------|-------------|
| `STATE_MACHINE` | Execution order / control flow |
| `DATA_FLOW` | Value flows from source to target |
| `SPREAD` | Fan-out from spread source to iterations |
| `AGGREGATE` | Fan-in from iterations to result collector |

---

## Comprehensive Example

```rappel
# Order Processing Workflow
# Demonstrates all language constructs in a realistic scenario

fn process_orders(input: [orders, config], output: [summary]):
    # Step 1: Fetch additional data via action
    inventory = @fetch_inventory(warehouse=config["warehouse"])

    # Step 2: Filter orders using for loop
    valid_orders = []
    rejected = []
    for order in orders:
        if order["sku"] in inventory and inventory[order["sku"]] >= order["qty"]:
            valid_orders = valid_orders + [order]
        else:
            rejected = rejected + [{"order": order, "reason": "out_of_stock"}]

    # Step 3: Conditional handling based on validation results
    if len(valid_orders) > 0:
        # Step 4: Parallel processing with spread
        # Each order gets payment processed independently
        payments = spread valid_orders:order -> @process_payment(
            order_id=order["id"],
            amount=order["total"],
            customer=order["customer_id"]
        )

        # Step 5: Parallel shipping quotes for all orders
        shipping = spread valid_orders:order -> @get_shipping_quote(
            destination=order["address"],
            weight=order["weight"]
        )

        # Step 6: Combine results using for loop with unpacking
        confirmations = []
        for i, order in enumerate(valid_orders):
            confirmation = {
                "order_id": order["id"],
                "payment": payments[i],
                "shipping": shipping[i],
                "status": "confirmed"
            }
            confirmations = confirmations + [confirmation]
    else:
        confirmations = []

    # Step 7: Send notifications via action with retry policy
    notification_result = @send_notifications(
        confirmations=confirmations,
        rejected=rejected
    ) [NetworkError -> retry: 3, backoff: 30s] [timeout: 60s]

    # Step 8: Build final summary
    summary = {
        "processed": len(confirmations),
        "rejected": len(rejected),
        "notification_id": notification_result["id"]
    }

    return summary
```

### DAG Visualization

The above program translates to the following DAG structure:

```
                    ┌─────────────────┐
                    │  INPUT (orders, │
                    │     config)     │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ @fetch_inventory│
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   FOR LOOP      │
                    │ (filter orders) │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │   CONDITION     │
                    │ len(valid) > 0  │
                    └───┬─────────┬───┘
                        │         │
              ┌─────────┘         └─────────┐
              │ TRUE                  FALSE │
              ▼                             ▼
     ┌────────────────┐           ┌─────────────────┐
     │  SPREAD NODE   │           │ confirmations=[]│
     │ (payments)     │           └────────┬────────┘
     └───────┬────────┘                    │
             │                             │
    ┌────────┼────────┐                    │
    ▼        ▼        ▼                    │
┌───────┐┌───────┐┌───────┐                │
│@pay(1)││@pay(2)││@pay(3)│                │
└───┬───┘└───┬───┘└───┬───┘                │
    │        │        │                    │
    └────────┼────────┘                    │
             ▼                             │
     ┌───────────────┐                     │
     │  AGGREGATOR   │                     │
     │  (payments)   │                     │
     └───────┬───────┘                     │
             │                             │
             ▼                             │
     ┌────────────────┐                    │
     │  SPREAD NODE   │                    │
     │  (shipping)    │                    │
     └───────┬────────┘                    │
             │                             │
    ┌────────┼────────┐                    │
    ▼        ▼        ▼                    │
┌───────┐┌───────┐┌───────┐                │
│@ship1 ││@ship2 ││@ship3 │                │
└───┬───┘└───┬───┘└───┬───┘                │
    │        │        │                    │
    └────────┼────────┘                    │
             ▼                             │
     ┌───────────────┐                     │
     │  AGGREGATOR   │                     │
     │  (shipping)   │                     │
     └───────┬───────┘                     │
             │                             │
             ▼                             │
     ┌───────────────┐                     │
     │   FOR LOOP    │                     │
     │ (combine)     │                     │
     └───────┬───────┘                     │
             │                             │
             └──────────┬──────────────────┘
                        │
                        ▼
               ┌─────────────────┐
               │     MERGE       │
               │ (confirmations) │
               └────────┬────────┘
                        │
                        ▼
               ┌─────────────────┐
               │ @send_notifs    │
               │ [retry policy]  │
               └────────┬────────┘
                        │
                        ▼
               ┌─────────────────┐
               │  BUILD SUMMARY  │
               └────────┬────────┘
                        │
                        ▼
               ┌─────────────────┐
               │ OUTPUT (summary)│
               └─────────────────┘
```

---

## Execution Model

### Single-Threaded Execution

1. Actions are queued and executed one at a time
2. Results are stored in the database after each action
3. Provides deterministic, debuggable execution

### Multi-Threaded / Distributed Execution

1. Workers poll an action queue (simulates `SELECT ... FOR UPDATE SKIP LOCKED`)
2. Independent actions (e.g., spread iterations) execute in parallel
3. Database serves as the central coordinator
4. Results are persisted for fault tolerance

### Retry and Timeout Handling

1. Actions with retry policies store retry metadata in the queue
2. On failure, matching retry policy decrements retry count and schedules with exponential backoff
3. Timed-out actions are detected via `timeout_at < NOW()` and re-queued
4. Workers claim actions with a `lock_uuid` for ownership tracking

### Inbox Pattern

For efficient distributed execution, we use an append-only inbox pattern:

1. When Node A completes, it `INSERT`s results into an inbox table for each downstream node
2. When Node B is ready to run, it `SELECT`s all rows where `target_node_id = B`
3. This provides O(1) writes (no locks) and efficient batched reads

---

## Type System

Rappel uses dynamic typing with JSON-serializable values:

| Type | Description | Example |
|------|-------------|---------|
| `int` | Integer numbers | `42` |
| `float` | Floating point | `3.14` |
| `str` | Strings | `"hello"` |
| `bool` | Boolean | `True`, `False` |
| `list` | Ordered collection | `[1, 2, 3]` |
| `dict` | Key-value mapping | `{"a": 1, "b": 2}` |
| `None` | Null value | `None` |

All action inputs and outputs must be one of these types (or nested compositions thereof).

---

## Error Handling

### Compile-Time Errors

- Undefined variable reference
- Duplicate variable assignment (immutability violation)
- Missing required function inputs/outputs
- Positional arguments in function/action calls

### Runtime Errors

- Action handler not registered
- Action execution failure (may trigger retry if policy defined)
- Type mismatch in operations
- Index out of bounds
- Key not found in dict

---

## Future Extensions

Potential additions to the language:

1. **Type Annotations**: Optional type hints for better validation
2. **Map/Filter/Reduce**: Functional operations on collections
3. **Async Actions**: Actions that return futures for deferred results
4. **Checkpointing**: Explicit state snapshots for long-running workflows
