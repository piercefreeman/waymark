# Rappel IR Grammar Specification

## Overview

Rappel IR is an intermediate representation for workflow definitions. It captures the subset of Python that can be translated into a durable execution DAG. The IR is produced by parsing Python AST and can be serialized to a human-readable text format for debugging.

## Lexical Elements

```
IDENT       = [a-zA-Z_][a-zA-Z0-9_]*
MODULE_PATH = IDENT ("." IDENT)*
INT         = [0-9]+
FLOAT       = [0-9]+ "." [0-9]+
STRING      = '"' [^"]* '"' | "'" [^']* "'"
EXPR        = <any valid Python expression as string>
CODE        = <any valid Python code block as string>
```

## Grammar (EBNF)

```ebnf
(* Top-level *)
workflow        = "workflow" IDENT "(" params? ")" return_type? ":" body ;
params          = param ("," param)* ;
param           = IDENT (":" type_ann)? ;
return_type     = "->" type_ann ;
type_ann        = EXPR ;
body            = statement+ ;

(* Statements *)
statement       = action_call
                | gather
                | loop
                | conditional
                | try_except
                | sleep
                | python_block
                | return_stmt ;

(* Action call - the fundamental unit of durable execution *)
action_call     = target? "@" action_ref "(" kwargs? ")" policy? ;
target          = IDENT "=" ;
action_ref      = MODULE_PATH? IDENT ;
kwargs          = kwarg ("," kwarg)* ;
kwarg           = IDENT "=" EXPR ;
policy          = "[" "policy:" policy_opts "]" ;
policy_opts     = policy_opt ("," policy_opt)* ;
policy_opt      = "timeout=" INT "s"
                | "retry=" INT
                | "backoff=" backoff_spec ;
backoff_spec    = "linear(" INT "ms)"
                | "exp(" INT "ms," FLOAT "x)" ;

(* Parallel execution *)
gather          = target? "parallel(" action_call ("," action_call)* ")" ;

(* Loop with accumulator(s) *)
loop            = "loop" IDENT "in" EXPR "->" "[" accumulators "]" ":" loop_body ;
accumulators    = IDENT ("," IDENT)* ;
loop_body       = preamble? action_chain yields ;
preamble        = python_block+ ;
action_chain    = action_call+ ;
yields          = yield_stmt+ ;
yield_stmt      = "yield" EXPR "->" IDENT ;

(* Conditional branching *)
conditional     = branch+ ;
branch          = "branch" branch_type guard? ":" branch_body ;
branch_type     = "if" | "elif" | "else" ;
guard           = EXPR ;
branch_body     = preamble? action_chain postamble? ;
postamble       = python_block+ ;

(* Exception handling *)
try_except      = "try:" action_chain except_handler+ ;
except_handler  = "except" exception_types? ":" action_chain ;
exception_types = exception_type
                | "(" exception_type ("," exception_type)* ")" ;
exception_type  = MODULE_PATH? IDENT ;

(* Durable sleep *)
sleep           = "@sleep(" EXPR ")" ;

(* Escape hatch for arbitrary Python *)
python_block    = "python" io_spec? "{" CODE "}" ;
io_spec         = "(" io_parts ")" ;
io_parts        = io_part (";" io_part)* ;
io_part         = "reads:" IDENT ("," IDENT)*
                | "writes:" IDENT ("," IDENT)* ;

(* Return *)
return_stmt     = "return" return_value? ;
return_value    = EXPR | action_call | gather ;

(* Spread - compile-time expansion of action over collection *)
spread          = target "spread" action_call "over" IDENT "as" IDENT ;
```

## Semantic Constraints

### Actions

1. Actions are marked with `@` prefix to distinguish from regular Python calls
2. Actions are the unit of durable execution - they run on workers and their results are persisted
3. Action arguments must be serializable expressions
4. Actions may have an optional `target` to capture the return value

### Parallel Execution

1. `parallel(...)` executes all contained actions concurrently
2. All actions in a parallel block must be independent (no dependencies on each other's results)
3. Results are returned as a tuple in the order specified

### Loops

1. Loops must have at least one accumulator initialized before the loop
2. Loop body must contain at least one action
3. Loop body must end with `yield` statements that populate accumulators
4. Preamble (Python before first action) is executed per-iteration
5. No Python code allowed between actions (only before first action)

### Conditionals

1. Conditionals with actions must have an `else` branch (all paths must be covered)
2. Each branch must contain at least one action
3. Guards are Python expressions that evaluate to boolean
4. Guards must not contain:
   - Await expressions
   - Lambda expressions
   - Yield expressions
   - Function calls except: `len`, `str`, `int`, `float`, `bool`, `abs`, `min`, `max`, `sum`, `any`, `all`, `isinstance`, `hasattr`, `getattr`
5. Multiple actions per branch are chained sequentially (only first action needs guard in DAG)

### Try/Except

1. Try block must contain only action calls
2. Except handlers must contain only action calls
3. No `finally` blocks (not representable in DAG)
4. No `else` blocks (try/else)
5. Exception variable binding not allowed (`except E as e:`)

### Python Blocks

1. Python blocks are the escape hatch for arbitrary computation
2. Must declare inputs (variables read) and outputs (variables written)
3. Cannot contain action calls
4. Imports and definitions are captured for serialization
5. Executed inline (not durably persisted)

### Sleep

1. `@sleep(duration)` is a durable sleep (survives restarts)
2. Duration is in seconds (can be expression)

### Spread

1. Compile-time expansion of an action over a known collection
2. Only valid when the collection size is known at compile time (e.g., from a `parallel()` result)
3. Expands to N parallel action calls where N = len(collection)

## Data Flow

Variables flow through the workflow as follows:

1. **Workflow parameters** → available to all statements
2. **Action results** → available to subsequent statements via `target`
3. **Parallel results** → available as tuple via `target`
4. **Loop accumulators** → available after loop completes
5. **Python block outputs** → available to subsequent statements
6. **Branch results** → available after conditional (if all branches assign same target)

## DAG Translation

The IR maps to DAG nodes as follows:

| IR Construct | DAG Representation |
|--------------|-------------------|
| `action_call` | Single node |
| `parallel(a, b, c)` | 3 nodes with no dependencies between them, join node |
| `loop` | Iterator source → loop head → body nodes → back edge → exit |
| `conditional` | Guarded nodes for each branch → merge node |
| `try_except` | Try nodes with exception edges to handler nodes |
| `sleep` | Sleep node (special handling by runtime) |
| `python_block` | Preamble/computed node (inline execution) |
| `spread` | Expands to N parallel nodes at compile time |

## Example

```
workflow OrderProcessor(orders: list[Order]) -> list[Confirmation]:
    loop order in orders -> [confirmations]:
        # preamble (reads: order; writes: validated_data)
        validated_data = validate_locally(order)

        validated = @validate_order(data=validated_data)
        payment = @process_payment(order_id=validated.id)
        confirmation = @send_confirmation(payment_id=payment.id)

        yield confirmation -> confirmations

    return confirmations
```

This translates to a DAG with:
- Iterator source node (creates loop iterator)
- Loop head node (manages iteration state)
- Preamble node (inline Python execution)
- `validate_order` node (depends on preamble)
- `process_payment` node (depends on validate_order)
- `send_confirmation` node (depends on process_payment)
- Back edge from send_confirmation to loop head
- Exit node (finalizes accumulators)
