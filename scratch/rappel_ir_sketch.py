"""
Sketch: Rappel IR Parser

This file explores parsing Python AST into a validated Rappel IR,
then serializing that IR to a text representation for debugging/validation.

Production features implemented:
1. Input/output analysis for RappelPythonBlock
2. Module resolution for actions
3. Positional arg mapping using action signatures
4. Import/definition capture for python blocks
5. Source location tracking for error messages
6. Validation of guard expressions
"""

from __future__ import annotations

import ast
import inspect
import textwrap
from dataclasses import dataclass, field
from typing import Any


# =============================================================================
# Source Location Tracking
# =============================================================================


@dataclass
class SourceLocation:
    """Tracks source location for error messages."""

    lineno: int
    col_offset: int
    end_lineno: int | None = None
    end_col_offset: int | None = None

    @classmethod
    def from_node(cls, node: ast.AST) -> SourceLocation:
        return cls(
            lineno=getattr(node, "lineno", 0),
            col_offset=getattr(node, "col_offset", 0),
            end_lineno=getattr(node, "end_lineno", None),
            end_col_offset=getattr(node, "end_col_offset", None),
        )

    def __str__(self) -> str:
        if self.end_lineno and self.end_lineno != self.lineno:
            return f"lines {self.lineno}-{self.end_lineno}"
        return f"line {self.lineno}, col {self.col_offset}"


# =============================================================================
# Rappel IR Node Types
# =============================================================================


@dataclass
class RunActionConfig:
    timeout_seconds: int | None = None
    max_retries: int | None = None
    backoff: BackoffConfig | None = None


@dataclass
class BackoffConfig:
    kind: str  # "linear" or "exponential"
    base_delay_ms: int
    multiplier: float | None = None  # Only for exponential


@dataclass
class RappelActionCall:
    """A single action invocation."""

    action: str
    module: str | None
    kwargs: dict[str, str]  # All argument expressions as strings
    target: str | None  # Variable to assign result to
    config: RunActionConfig | None = None
    location: SourceLocation | None = None


@dataclass
class RappelGather:
    """Parallel execution of multiple actions."""

    calls: list[RappelActionCall]
    target: str | None  # Variable to assign tuple result to
    location: SourceLocation | None = None


@dataclass
class RappelPythonBlock:
    """Arbitrary Python code that doesn't contain actions."""

    code: str
    imports: list[str] = field(default_factory=list)
    definitions: list[str] = field(default_factory=list)
    inputs: list[str] = field(default_factory=list)  # Variables read
    outputs: list[str] = field(default_factory=list)  # Variables produced
    location: SourceLocation | None = None


@dataclass
class RappelLoop:
    """A for loop with action(s) in the body."""

    iterator_expr: str  # Expression producing the iterable
    loop_var: str  # Loop variable name
    accumulators: list[str]  # Variables initialized as [] before loop
    preamble: list[RappelPythonBlock]  # Pure Python before first action
    body: list[RappelActionCall]  # Action calls in sequence
    append_exprs: list[tuple[str, str]]  # (accumulator_name, source_expr)
    location: SourceLocation | None = None


@dataclass
class RappelBranch:
    """One branch of a conditional."""

    guard: str  # Guard expression
    preamble: list[RappelPythonBlock]  # Statements before first action
    actions: list[RappelActionCall]  # One or more actions (chained in DAG)
    postamble: list[RappelPythonBlock]  # Statements after last action
    location: SourceLocation | None = None


@dataclass
class RappelConditional:
    """An if/elif/else with actions in each branch."""

    branches: list[RappelBranch]  # Must cover all cases (else required)
    target: str | None  # Common target variable if branches assign
    location: SourceLocation | None = None


@dataclass
class RappelExceptHandler:
    """An except clause."""

    exception_types: list[tuple[str | None, str | None]]  # (module, type) pairs
    body: list[RappelActionCall]
    location: SourceLocation | None = None


@dataclass
class RappelTryExcept:
    """A try/except block with actions."""

    try_body: list[RappelActionCall]
    handlers: list[RappelExceptHandler]
    location: SourceLocation | None = None


@dataclass
class RappelSleep:
    """A durable sleep."""

    duration_expr: str
    location: SourceLocation | None = None


@dataclass
class RappelReturn:
    """Return statement."""

    value: str | RappelActionCall | RappelGather | None
    location: SourceLocation | None = None


@dataclass
class RappelComprehensionMap:
    """List comprehension mapping actions over a collection.

    Pattern: [await action(x=item) for item in collection]
    This expands at compile time if collection is known (from gather).
    """

    action: RappelActionCall  # Template action (kwargs reference loop_var)
    loop_var: str
    iterable: str  # Variable name of collection
    target: str | None
    location: SourceLocation | None = None


# Union type for all statements
RappelStatement = (
    RappelActionCall
    | RappelGather
    | RappelPythonBlock
    | RappelLoop
    | RappelConditional
    | RappelTryExcept
    | RappelSleep
    | RappelReturn
    | RappelComprehensionMap
)


@dataclass
class RappelWorkflow:
    """A complete workflow definition."""

    name: str
    params: list[tuple[str, str | None]]  # (name, type_annotation)
    body: list[RappelStatement]
    return_type: str | None = None


# =============================================================================
# Action Definition (for module resolution and signature mapping)
# =============================================================================


@dataclass
class ActionDefinition:
    """Metadata about an action for module resolution and arg mapping."""

    name: str  # The action name as registered
    module: str | None  # Module where the action is defined
    signature: inspect.Signature | None  # For positional arg mapping
    param_names: list[str] = field(default_factory=list)  # Ordered parameter names


# =============================================================================
# Module Index (for import/definition capture)
# =============================================================================


class ModuleIndex:
    """Indexes a module's imports and definitions for capture in python blocks."""

    def __init__(self, module_source: str):
        self._source = module_source
        self._imports: dict[str, str] = {}  # name -> import statement
        self._definitions: dict[str, str] = {}  # name -> definition source
        self._definition_deps: dict[str, set[str]] = {}  # name -> names referenced

        tree = ast.parse(module_source)
        for node in tree.body:
            snippet = ast.get_source_segment(module_source, node)
            if snippet is None:
                continue
            text = textwrap.dedent(snippet)

            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                self._definitions[node.name] = text
                self._definition_deps[node.name] = self._extract_deps(node)
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                for alias in node.names:
                    exposed = alias.asname or alias.name.split(".")[0]
                    self._imports[exposed] = text

    def _extract_deps(self, node: ast.AST) -> set[str]:
        """Extract names referenced in a definition."""
        deps: set[str] = set()
        for sub in ast.walk(node):
            if isinstance(sub, ast.Name) and isinstance(sub.ctx, ast.Load):
                deps.add(sub.id)
        return deps

    @property
    def symbols(self) -> set[str]:
        return set(self._imports) | set(self._definitions)

    def resolve(self, names: set[str]) -> tuple[list[str], list[str]]:
        """Resolve names to import and definition blocks, with transitive deps."""
        import_blocks: list[str] = []
        definition_blocks: list[str] = []
        resolved: set[str] = set()
        to_resolve = list(names)

        while to_resolve:
            name = to_resolve.pop()
            if name in resolved:
                continue
            resolved.add(name)

            if name in self._imports:
                import_blocks.append(self._imports[name])
            elif name in self._definitions:
                definition_blocks.append(self._definitions[name])
                # Add transitive dependencies
                for dep in self._definition_deps.get(name, set()):
                    if dep not in resolved and dep in self.symbols:
                        to_resolve.append(dep)

        return sorted(set(import_blocks)), sorted(set(definition_blocks))


# =============================================================================
# Variable Analysis (for input/output tracking)
# =============================================================================


class VariableAnalyzer(ast.NodeVisitor):
    """Analyzes Python code to determine inputs (reads) and outputs (writes)."""

    def __init__(self, known_vars: set[str] | None = None):
        self.reads: set[str] = set()
        self.writes: set[str] = set()
        self._known_vars = known_vars or set()
        self._local_scope: set[str] = set()  # Variables defined in this block

    def analyze(self, nodes: list[ast.stmt]) -> tuple[list[str], list[str]]:
        """Analyze statements and return (inputs, outputs)."""
        for node in nodes:
            self.visit(node)

        # Inputs are reads that aren't local writes and are known vars
        inputs = sorted(self.reads - self._local_scope)
        # Outputs are writes
        outputs = sorted(self.writes)
        return inputs, outputs

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            self.reads.add(node.id)
        elif isinstance(node.ctx, (ast.Store, ast.Del)):
            self.writes.add(node.id)
            self._local_scope.add(node.id)
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        # Visit value first (reads before writes)
        self.visit(node.value)
        for target in node.targets:
            self.visit(target)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        # Augmented assignment reads and writes
        if isinstance(node.target, ast.Name):
            self.reads.add(node.target.id)
            self.writes.add(node.target.id)
        self.visit(node.value)

    def visit_For(self, node: ast.For) -> None:
        self.visit(node.iter)
        self.visit(node.target)
        for stmt in node.body:
            self.visit(stmt)
        for stmt in node.orelse:
            self.visit(stmt)

    def visit_comprehension(self, node: ast.comprehension) -> None:
        self.visit(node.iter)
        # Target is local to comprehension
        if isinstance(node.target, ast.Name):
            self._local_scope.add(node.target.id)
        for if_ in node.ifs:
            self.visit(if_)


# =============================================================================
# Guard Expression Validator
# =============================================================================


class GuardValidator(ast.NodeVisitor):
    """Validates that guard expressions are safe and well-formed."""

    ALLOWED_OPS = {
        ast.And, ast.Or, ast.Not,
        ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
        ast.Is, ast.IsNot, ast.In, ast.NotIn,
        ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod, ast.FloorDiv,
    }

    ALLOWED_FUNCS = {
        "len", "str", "int", "float", "bool", "abs", "min", "max",
        "sum", "any", "all", "isinstance", "hasattr", "getattr",
    }

    def __init__(self, known_vars: set[str]):
        self.known_vars = known_vars
        self.errors: list[str] = []
        self.referenced_vars: set[str] = set()

    def validate(self, guard_expr: str, location: SourceLocation | None = None) -> list[str]:
        """Validate a guard expression. Returns list of errors (empty if valid)."""
        self.errors = []
        self.referenced_vars = set()

        try:
            tree = ast.parse(guard_expr, mode="eval")
            self.visit(tree.body)
        except SyntaxError as e:
            loc_str = f" at {location}" if location else ""
            self.errors.append(f"Invalid guard syntax{loc_str}: {e}")

        return self.errors

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            self.referenced_vars.add(node.id)
            # Don't error on unknown vars - they might be defined upstream
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        # Check if it's a simple function call
        if isinstance(node.func, ast.Name):
            if node.func.id not in self.ALLOWED_FUNCS:
                self.errors.append(
                    f"Function '{node.func.id}' not allowed in guard expressions. "
                    f"Allowed: {', '.join(sorted(self.ALLOWED_FUNCS))}"
                )
        elif isinstance(node.func, ast.Attribute):
            # Allow method calls like str.startswith
            pass
        else:
            self.errors.append(f"Complex function call not allowed in guard: {ast.unparse(node)}")
        self.generic_visit(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        self.errors.append("Lambda expressions not allowed in guards")

    def visit_Await(self, node: ast.Await) -> None:
        self.errors.append("Await expressions not allowed in guards")

    def visit_Yield(self, node: ast.Yield) -> None:
        self.errors.append("Yield expressions not allowed in guards")

    def visit_YieldFrom(self, node: ast.YieldFrom) -> None:
        self.errors.append("Yield expressions not allowed in guards")


# =============================================================================
# Validation Errors
# =============================================================================


class RappelParseError(Exception):
    """Raised when Python code cannot be parsed into valid Rappel IR."""

    def __init__(
        self,
        message: str,
        node: ast.AST | None = None,
        location: SourceLocation | None = None,
    ):
        self.node = node
        self.location = location or (SourceLocation.from_node(node) if node else None)
        loc_str = f" ({self.location})" if self.location else ""
        super().__init__(f"{message}{loc_str}")


# =============================================================================
# Parser: Python AST -> Rappel IR
# =============================================================================


class RappelParser:
    """Parses a Python async def into Rappel IR."""

    def __init__(
        self,
        action_defs: dict[str, ActionDefinition],
        module_index: ModuleIndex | None = None,
        source: str | None = None,
    ):
        self.action_defs = action_defs
        self.action_names = set(action_defs.keys())
        self.module_index = module_index
        self.source = source

        self._known_collections: dict[str, list[str]] = {}
        self._known_vars: set[str] = set()  # Variables defined so far
        self._guard_validator = GuardValidator(self._known_vars)

    def parse_workflow(self, func: ast.AsyncFunctionDef) -> RappelWorkflow:
        """Parse an async function definition into a RappelWorkflow."""
        params = []
        for arg in func.args.args:
            if arg.arg == "self":
                continue
            type_ann = ast.unparse(arg.annotation) if arg.annotation else None
            params.append((arg.arg, type_ann))
            self._known_vars.add(arg.arg)

        return_type = ast.unparse(func.returns) if func.returns else None

        body = self._parse_body(func.body)

        return RappelWorkflow(
            name=func.name,
            params=params,
            body=body,
            return_type=return_type,
        )

    def _parse_body(self, stmts: list[ast.stmt]) -> list[RappelStatement]:
        """Parse a list of statements into Rappel IR."""
        result: list[RappelStatement] = []

        i = 0
        while i < len(stmts):
            stmt = stmts[i]

            # Check for accumulator initialization pattern: `results = []`
            if self._is_empty_list_init(stmt):
                # Track it but don't emit - accumulators are handled by loops
                if isinstance(stmt, ast.Assign):
                    for t in stmt.targets:
                        if isinstance(t, ast.Name):
                            self._known_vars.add(t.id)
                i += 1
                continue

            parsed = self._parse_statement(stmt)
            if parsed is not None:
                result.append(parsed)
                # Track outputs as known vars
                self._track_statement_outputs(parsed)
            i += 1

        return result

    def _track_statement_outputs(self, stmt: RappelStatement) -> None:
        """Track variables produced by a statement."""
        if isinstance(stmt, RappelActionCall) and stmt.target:
            self._known_vars.add(stmt.target)
        elif isinstance(stmt, RappelGather) and stmt.target:
            self._known_vars.add(stmt.target)
        elif isinstance(stmt, RappelPythonBlock):
            self._known_vars.update(stmt.outputs)
        elif isinstance(stmt, RappelLoop):
            self._known_vars.update(stmt.accumulators)
        elif isinstance(stmt, RappelConditional) and stmt.target:
            self._known_vars.add(stmt.target)
        elif isinstance(stmt, RappelComprehensionMap) and stmt.target:
            self._known_vars.add(stmt.target)

    def _parse_statement(self, stmt: ast.stmt) -> RappelStatement | None:
        """Parse a single statement into Rappel IR."""
        location = SourceLocation.from_node(stmt)

        # Return statement
        if isinstance(stmt, ast.Return):
            return self._parse_return(stmt, location)

        # Expression statement (action call, gather, sleep)
        if isinstance(stmt, ast.Expr):
            return self._parse_expr_statement(stmt, location)

        # Assignment
        if isinstance(stmt, ast.Assign):
            return self._parse_assignment(stmt, location)

        # For loop
        if isinstance(stmt, ast.For):
            return self._parse_for_loop(stmt, location)

        # If statement
        if isinstance(stmt, ast.If):
            return self._parse_conditional(stmt, location)

        # Try/except
        if isinstance(stmt, ast.Try):
            return self._parse_try_except(stmt, location)

        # Anything else becomes a python block
        return self._make_python_block([stmt], location)

    def _parse_return(self, stmt: ast.Return, location: SourceLocation) -> RappelReturn:
        """Parse a return statement."""
        if stmt.value is None:
            return RappelReturn(value=None, location=location)

        # return await action()
        action_call = self._extract_action_call(stmt.value)
        if action_call is not None:
            action_call.target = "__workflow_return"
            return RappelReturn(value=action_call, location=location)

        # return await asyncio.gather(...)
        gather = self._extract_gather(stmt.value)
        if gather is not None:
            gather.target = "__workflow_return"
            return RappelReturn(value=gather, location=location)

        # return expr (including variable names)
        return RappelReturn(value=ast.unparse(stmt.value), location=location)

    def _parse_expr_statement(
        self, stmt: ast.Expr, location: SourceLocation
    ) -> RappelStatement | None:
        """Parse an expression statement."""
        expr = stmt.value

        # await action()
        action_call = self._extract_action_call(expr)
        if action_call is not None:
            action_call.location = location
            return action_call

        # await asyncio.gather(...)
        gather = self._extract_gather(expr)
        if gather is not None:
            gather.location = location
            return gather

        # await asyncio.sleep(...)
        sleep = self._extract_sleep(expr)
        if sleep is not None:
            sleep.location = location
            return sleep

        # Side effect expression (like list.append)
        return self._make_python_block([stmt], location)

    def _parse_assignment(
        self, stmt: ast.Assign, location: SourceLocation
    ) -> RappelStatement | None:
        """Parse an assignment statement."""
        if len(stmt.targets) != 1:
            return self._make_python_block([stmt], location)

        target = stmt.targets[0]
        if not isinstance(target, ast.Name):
            return self._make_python_block([stmt], location)

        target_name = target.id

        # Empty list init - skip (handled by loop detection)
        if self._is_empty_list_init(stmt):
            return None

        # x = await action()
        action_call = self._extract_action_call(stmt.value)
        if action_call is not None:
            action_call.target = target_name
            action_call.location = location
            return action_call

        # x = await asyncio.gather(...)
        gather = self._extract_gather(stmt.value)
        if gather is not None:
            gather.target = target_name
            gather.location = location
            # Track for later comprehension expansion
            self._known_collections[target_name] = [
                f"{target_name}__item{i}" for i in range(len(gather.calls))
            ]
            return gather

        # x = [await action(y=v) for v in collection]
        comp_map = self._extract_comprehension_map(stmt.value, target_name)
        if comp_map is not None:
            comp_map.location = location
            return comp_map

        # Regular Python assignment
        return self._make_python_block([stmt], location)

    def _parse_for_loop(self, stmt: ast.For, location: SourceLocation) -> RappelStatement:
        """Parse a for loop."""
        if stmt.orelse:
            raise RappelParseError("for/else is not supported", stmt, location)

        if not isinstance(stmt.target, ast.Name):
            raise RappelParseError(
                "for loop target must be a simple variable", stmt, location
            )

        loop_var = stmt.target.id
        iterator_expr = ast.unparse(stmt.iter)

        # Parse loop body to find: preamble, actions, appends
        preamble: list[RappelPythonBlock] = []
        actions: list[RappelActionCall] = []
        appends: list[tuple[str, str]] = []
        accumulators: list[str] = []

        # Track loop var as known within loop
        loop_known_vars = self._known_vars | {loop_var}

        found_first_action = False

        for body_stmt in stmt.body:
            body_location = SourceLocation.from_node(body_stmt)

            # Check for append
            append_info = self._extract_append(body_stmt)
            if append_info is not None:
                acc_name, source_expr = append_info
                appends.append((acc_name, source_expr))
                if acc_name not in accumulators:
                    accumulators.append(acc_name)
                continue

            # Check for action call
            if isinstance(body_stmt, ast.Assign) and len(body_stmt.targets) == 1:
                target = body_stmt.targets[0]
                if isinstance(target, ast.Name):
                    action_call = self._extract_action_call(body_stmt.value)
                    if action_call is not None:
                        action_call.target = target.id
                        action_call.location = body_location
                        actions.append(action_call)
                        loop_known_vars.add(target.id)
                        found_first_action = True
                        continue

            if isinstance(body_stmt, ast.Expr):
                action_call = self._extract_action_call(body_stmt.value)
                if action_call is not None:
                    action_call.location = body_location
                    actions.append(action_call)
                    found_first_action = True
                    continue

            # Not an action or append
            if found_first_action:
                raise RappelParseError(
                    "Non-action statements after first action in loop body not supported",
                    body_stmt,
                    body_location,
                )
            else:
                # Before first action - this is preamble
                preamble.append(
                    self._make_python_block([body_stmt], body_location, loop_known_vars)
                )
                # Track preamble outputs
                for p in preamble:
                    loop_known_vars.update(p.outputs)

        if not actions:
            raise RappelParseError(
                "for loop must contain at least one action call", stmt, location
            )

        if not appends:
            raise RappelParseError(
                "for loop must append to an accumulator", stmt, location
            )

        return RappelLoop(
            iterator_expr=iterator_expr,
            loop_var=loop_var,
            accumulators=accumulators,
            preamble=preamble,
            body=actions,
            append_exprs=appends,
            location=location,
        )

    def _parse_conditional(
        self, stmt: ast.If, location: SourceLocation
    ) -> RappelStatement:
        """Parse an if/elif/else statement."""
        # Check if any branch contains an action
        if not self._contains_action(stmt):
            return self._make_python_block([stmt], location)

        # Must have else branch if any branch has action
        if not stmt.orelse:
            raise RappelParseError(
                "Conditional with actions requires an else branch", stmt, location
            )

        branches = self._extract_conditional_branches(stmt, parent_guard=None)

        # Validate all guards
        for branch in branches:
            errors = self._guard_validator.validate(branch.guard, branch.location)
            if errors:
                raise RappelParseError(
                    f"Invalid guard expression: {'; '.join(errors)}",
                    location=branch.location,
                )

        # Find common target (last action's target in each branch)
        targets = {b.actions[-1].target for b in branches if b.actions and b.actions[-1].target}
        target = targets.pop() if len(targets) == 1 else None

        return RappelConditional(branches=branches, target=target, location=location)

    def _extract_conditional_branches(
        self, stmt: ast.If, parent_guard: str | None
    ) -> list[RappelBranch]:
        """Extract all branches from an if/elif/else chain."""
        branches: list[RappelBranch] = []
        location = SourceLocation.from_node(stmt)

        condition = f"({ast.unparse(stmt.test)})"
        if parent_guard:
            true_guard = f"({parent_guard}) and {condition}"
        else:
            true_guard = condition

        # Parse the if body
        true_branch = self._parse_branch_body(stmt.body, true_guard, location)
        branches.append(true_branch)

        # Handle elif/else
        if len(stmt.orelse) == 1 and isinstance(stmt.orelse[0], ast.If):
            # elif
            negated = (
                f"not {condition}"
                if not parent_guard
                else f"({parent_guard}) and not {condition}"
            )
            branches.extend(
                self._extract_conditional_branches(stmt.orelse[0], negated)
            )
        elif stmt.orelse:
            # else
            false_guard = (
                f"not {condition}"
                if not parent_guard
                else f"({parent_guard}) and not {condition}"
            )
            else_location = SourceLocation.from_node(stmt.orelse[0])
            false_branch = self._parse_branch_body(
                stmt.orelse, false_guard, else_location
            )
            branches.append(false_branch)

        return branches

    def _parse_branch_body(
        self, body: list[ast.stmt], guard: str, location: SourceLocation
    ) -> RappelBranch:
        """Parse a branch body into preamble, actions, postamble."""
        preamble: list[RappelPythonBlock] = []
        actions: list[RappelActionCall] = []
        postamble: list[RappelPythonBlock] = []

        branch_known_vars = set(self._known_vars)
        found_first_action = False
        in_postamble = False

        for stmt in body:
            stmt_location = SourceLocation.from_node(stmt)
            stmt_action = self._extract_statement_action(stmt)

            if stmt_action is not None:
                if in_postamble:
                    # Action after postamble started - not allowed
                    raise RappelParseError(
                        "Actions cannot appear after non-action statements in a branch",
                        stmt,
                        stmt_location,
                    )
                stmt_action.location = stmt_location
                actions.append(stmt_action)
                if stmt_action.target:
                    branch_known_vars.add(stmt_action.target)
                found_first_action = True
            elif not found_first_action:
                # Before any action - this is preamble
                block = self._make_python_block([stmt], stmt_location, branch_known_vars)
                preamble.append(block)
                branch_known_vars.update(block.outputs)
            else:
                # After action(s) - this is postamble
                in_postamble = True
                block = self._make_python_block([stmt], stmt_location, branch_known_vars)
                postamble.append(block)
                branch_known_vars.update(block.outputs)

        if not actions:
            raise RappelParseError(
                f"Conditional branch with guard '{guard}' must have at least one action",
                location=location,
            )

        return RappelBranch(
            guard=guard,
            preamble=preamble,
            actions=actions,
            postamble=postamble,
            location=location,
        )

    def _extract_statement_action(self, stmt: ast.stmt) -> RappelActionCall | None:
        """Extract action call from a statement if present."""
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
            target = stmt.targets[0]
            if isinstance(target, ast.Name):
                action = self._extract_action_call(stmt.value)
                if action:
                    action.target = target.id
                    return action
        elif isinstance(stmt, ast.Expr):
            return self._extract_action_call(stmt.value)
        return None

    def _parse_try_except(
        self, stmt: ast.Try, location: SourceLocation
    ) -> RappelTryExcept:
        """Parse a try/except statement."""
        if stmt.finalbody:
            raise RappelParseError("finally blocks are not supported", stmt, location)
        if stmt.orelse:
            raise RappelParseError("try/else is not supported", stmt, location)
        if not stmt.handlers:
            raise RappelParseError("try must have except handlers", stmt, location)

        # Parse try body - must contain actions
        try_actions: list[RappelActionCall] = []
        try_known_vars = set(self._known_vars)

        for body_stmt in stmt.body:
            stmt_location = SourceLocation.from_node(body_stmt)
            action = self._extract_statement_action(body_stmt)
            if action:
                action.location = stmt_location
                try_actions.append(action)
                if action.target:
                    try_known_vars.add(action.target)
            else:
                raise RappelParseError(
                    "try block must contain only action calls", body_stmt, stmt_location
                )

        if not try_actions:
            raise RappelParseError(
                "try block must contain at least one action", stmt, location
            )

        # Parse handlers
        handlers: list[RappelExceptHandler] = []
        for handler in stmt.handlers:
            handler_location = SourceLocation.from_node(handler)

            if handler.name:
                raise RappelParseError(
                    "except clauses cannot bind exception to variable",
                    handler,
                    handler_location,
                )

            exc_types = self._extract_exception_types(handler.type)

            handler_actions: list[RappelActionCall] = []
            for handler_stmt in handler.body:
                stmt_location = SourceLocation.from_node(handler_stmt)
                action = self._extract_statement_action(handler_stmt)
                if action:
                    action.location = stmt_location
                    handler_actions.append(action)
                else:
                    raise RappelParseError(
                        "except block must contain only action calls",
                        handler_stmt,
                        stmt_location,
                    )

            handlers.append(
                RappelExceptHandler(
                    exception_types=exc_types,
                    body=handler_actions,
                    location=handler_location,
                )
            )

        return RappelTryExcept(
            try_body=try_actions, handlers=handlers, location=location
        )

    def _extract_exception_types(
        self, node: ast.expr | None
    ) -> list[tuple[str | None, str | None]]:
        """Extract exception types from except clause."""
        if node is None:
            return [(None, None)]  # Bare except

        if isinstance(node, ast.Tuple):
            return [self._parse_exception_type(elt) for elt in node.elts]

        return [self._parse_exception_type(node)]

    def _parse_exception_type(self, node: ast.expr) -> tuple[str | None, str | None]:
        """Parse a single exception type into (module, name)."""
        if isinstance(node, ast.Name):
            return (None, node.id)
        if isinstance(node, ast.Attribute):
            # module.ExceptionType
            module_parts = []
            current = node.value
            while isinstance(current, ast.Attribute):
                module_parts.insert(0, current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                module_parts.insert(0, current.id)
            return (".".join(module_parts), node.attr)
        raise RappelParseError(
            f"Unsupported exception type: {ast.unparse(node)}",
            node,
            SourceLocation.from_node(node),
        )

    # =========================================================================
    # Extraction helpers
    # =========================================================================

    def _extract_action_call(self, expr: ast.expr) -> RappelActionCall | None:
        """Extract an action call from an expression."""
        # Unwrap await
        if isinstance(expr, ast.Await):
            expr = expr.value

        if not isinstance(expr, ast.Call):
            return None

        # Check for self.run_action(action(...), ...)
        run_action_info = self._extract_run_action(expr)
        if run_action_info:
            return run_action_info

        # Check for direct action call
        action_name = self._get_action_name(expr.func)
        if action_name is None or action_name not in self.action_names:
            return None

        # Get action definition for module and signature
        action_def = self.action_defs.get(action_name)
        module = action_def.module if action_def else None

        # Map arguments using signature
        kwargs = self._extract_kwargs_with_signature(expr, action_def)

        return RappelActionCall(
            action=action_def.name if action_def else action_name,
            module=module,
            kwargs=kwargs,
            target=None,
            config=None,
            location=SourceLocation.from_node(expr),
        )

    def _extract_kwargs_with_signature(
        self, call: ast.Call, action_def: ActionDefinition | None
    ) -> dict[str, str]:
        """Extract kwargs, mapping positional args using signature."""
        kwargs: dict[str, str] = {}

        # Handle keyword arguments first
        for kw in call.keywords:
            if kw.arg is not None:
                kwargs[kw.arg] = ast.unparse(kw.value)

        # Map positional arguments using signature
        if call.args:
            if action_def and action_def.param_names:
                param_names = action_def.param_names
                for i, arg in enumerate(call.args):
                    if i < len(param_names):
                        param_name = param_names[i]
                        if param_name not in kwargs:  # Don't override explicit kwargs
                            kwargs[param_name] = ast.unparse(arg)
                    else:
                        # More args than params - use positional placeholder
                        kwargs[f"__arg{i}"] = ast.unparse(arg)
            else:
                # No signature info - use positional placeholders
                for i, arg in enumerate(call.args):
                    kwargs[f"__arg{i}"] = ast.unparse(arg)

        return kwargs

    def _extract_run_action(self, call: ast.Call) -> RappelActionCall | None:
        """Extract action from self.run_action(action(...), ...) pattern."""
        func = call.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr == "run_action"
            and isinstance(func.value, ast.Name)
            and func.value.id == "self"
        ):
            return None

        if not call.args:
            raise RappelParseError(
                "run_action requires an action argument",
                call,
                SourceLocation.from_node(call),
            )

        inner = call.args[0]
        if isinstance(inner, ast.Await):
            inner = inner.value

        if not isinstance(inner, ast.Call):
            raise RappelParseError(
                "run_action argument must be an action call",
                call,
                SourceLocation.from_node(call),
            )

        action_name = self._get_action_name(inner.func)
        if action_name is None or action_name not in self.action_names:
            raise RappelParseError(
                f"Unknown action in run_action: {ast.unparse(inner.func)}",
                call,
                SourceLocation.from_node(call),
            )

        action_def = self.action_defs.get(action_name)
        kwargs = self._extract_kwargs_with_signature(inner, action_def)
        config = self._extract_run_action_config(call.keywords)

        return RappelActionCall(
            action=action_def.name if action_def else action_name,
            module=action_def.module if action_def else None,
            kwargs=kwargs,
            target=None,
            config=config,
            location=SourceLocation.from_node(call),
        )

    def _extract_run_action_config(
        self, keywords: list[ast.keyword]
    ) -> RunActionConfig | None:
        """Extract timeout/retry/backoff config from run_action keywords."""
        config = RunActionConfig()
        has_config = False

        for kw in keywords:
            if kw.arg == "timeout":
                config.timeout_seconds = self._eval_timeout(kw.value)
                has_config = True
            elif kw.arg == "retry":
                config.max_retries = self._eval_retry(kw.value)
                has_config = True
            elif kw.arg == "backoff":
                config.backoff = self._eval_backoff(kw.value)
                has_config = True

        return config if has_config else None

    def _eval_timeout(self, node: ast.expr) -> int:
        """Evaluate timeout value."""
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return int(node.value)
        # Handle timedelta
        if isinstance(node, ast.Call):
            func_name = self._get_simple_name(node.func)
            if func_name == "timedelta":
                return self._eval_timedelta(node)
        raise RappelParseError(
            f"Cannot evaluate timeout: {ast.unparse(node)}",
            node,
            SourceLocation.from_node(node),
        )

    def _eval_timedelta(self, call: ast.Call) -> int:
        """Evaluate timedelta to seconds."""
        seconds = 0.0
        conversions = {
            "days": 86400,
            "hours": 3600,
            "minutes": 60,
            "seconds": 1,
            "milliseconds": 0.001,
            "microseconds": 0.000001,
        }

        for kw in call.keywords:
            if kw.arg in conversions and isinstance(kw.value, ast.Constant):
                seconds += kw.value.value * conversions[kw.arg]

        # Handle positional args (days, seconds, microseconds)
        pos_keys = ["days", "seconds", "microseconds"]
        for i, arg in enumerate(call.args):
            if i < len(pos_keys) and isinstance(arg, ast.Constant):
                seconds += arg.value * conversions[pos_keys[i]]

        return int(seconds)

    def _eval_retry(self, node: ast.expr) -> int | None:
        """Evaluate retry value."""
        if isinstance(node, ast.Constant):
            if node.value is None:
                return 2_147_483_647  # UNLIMITED
            if isinstance(node.value, int):
                return node.value
        if isinstance(node, ast.Call):
            # RetryPolicy(attempts=N)
            for kw in node.keywords:
                if kw.arg in ("attempts", "max_attempts"):
                    if isinstance(kw.value, ast.Constant):
                        if kw.value.value is None:
                            return 2_147_483_647
                        return kw.value.value
        raise RappelParseError(
            f"Cannot evaluate retry: {ast.unparse(node)}",
            node,
            SourceLocation.from_node(node),
        )

    def _eval_backoff(self, node: ast.expr) -> BackoffConfig | None:
        """Evaluate backoff config."""
        if isinstance(node, ast.Constant) and node.value is None:
            return None
        if not isinstance(node, ast.Call):
            raise RappelParseError(
                f"Cannot evaluate backoff: {ast.unparse(node)}",
                node,
                SourceLocation.from_node(node),
            )

        func_name = self._get_simple_name(node.func)
        if func_name == "LinearBackoff":
            base_delay = self._get_keyword_int(node, "base_delay_ms")
            return BackoffConfig(kind="linear", base_delay_ms=base_delay)
        elif func_name == "ExponentialBackoff":
            base_delay = self._get_keyword_int(node, "base_delay_ms")
            multiplier = self._get_keyword_float(node, "multiplier", 2.0)
            return BackoffConfig(
                kind="exponential", base_delay_ms=base_delay, multiplier=multiplier
            )

        raise RappelParseError(
            f"Unknown backoff type: {func_name}",
            node,
            SourceLocation.from_node(node),
        )

    def _get_keyword_int(self, call: ast.Call, name: str, default: int = 0) -> int:
        for kw in call.keywords:
            if kw.arg == name and isinstance(kw.value, ast.Constant):
                return int(kw.value.value)
        return default

    def _get_keyword_float(
        self, call: ast.Call, name: str, default: float = 0.0
    ) -> float:
        for kw in call.keywords:
            if kw.arg == name and isinstance(kw.value, ast.Constant):
                return float(kw.value.value)
        return default

    def _extract_gather(self, expr: ast.expr) -> RappelGather | None:
        """Extract an asyncio.gather call."""
        if isinstance(expr, ast.Await):
            expr = expr.value

        if not isinstance(expr, ast.Call):
            return None

        func = expr.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr == "gather"
            and isinstance(func.value, ast.Name)
            and func.value.id == "asyncio"
        ):
            return None

        calls: list[RappelActionCall] = []
        for arg in expr.args:
            action = self._extract_action_call(arg)
            if action is None:
                raise RappelParseError(
                    f"gather argument must be an action call: {ast.unparse(arg)}",
                    arg,
                    SourceLocation.from_node(arg),
                )
            calls.append(action)

        return RappelGather(
            calls=calls, target=None, location=SourceLocation.from_node(expr)
        )

    def _extract_sleep(self, expr: ast.expr) -> RappelSleep | None:
        """Extract an asyncio.sleep call."""
        if isinstance(expr, ast.Await):
            expr = expr.value

        if not isinstance(expr, ast.Call):
            return None

        func = expr.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr == "sleep"
            and isinstance(func.value, ast.Name)
            and func.value.id == "asyncio"
        ):
            return None

        if not expr.args:
            raise RappelParseError(
                "asyncio.sleep requires a duration argument",
                expr,
                SourceLocation.from_node(expr),
            )

        duration_expr = ast.unparse(expr.args[0])
        return RappelSleep(
            duration_expr=duration_expr, location=SourceLocation.from_node(expr)
        )

    def _extract_comprehension_map(
        self, expr: ast.expr, target: str
    ) -> RappelComprehensionMap | None:
        """Extract [await action(x=v) for v in collection] pattern."""
        if not isinstance(expr, ast.ListComp):
            return None

        if len(expr.generators) != 1:
            return None

        gen = expr.generators[0]
        if gen.ifs or gen.is_async:
            return None

        if not isinstance(gen.target, ast.Name):
            return None

        loop_var = gen.target.id
        iterable = self._get_simple_name(gen.iter)
        if iterable is None:
            return None

        # Check if the element is an action call
        action = self._extract_action_call(expr.elt)
        if action is None:
            return None

        return RappelComprehensionMap(
            action=action,
            loop_var=loop_var,
            iterable=iterable,
            target=target,
            location=SourceLocation.from_node(expr),
        )

    def _extract_append(self, stmt: ast.stmt) -> tuple[str, str] | None:
        """Extract accumulator.append(expr) pattern."""
        if not isinstance(stmt, ast.Expr):
            return None

        call = stmt.value
        if not isinstance(call, ast.Call):
            return None

        func = call.func
        if not isinstance(func, ast.Attribute) or func.attr != "append":
            return None

        if not isinstance(func.value, ast.Name):
            return None

        if len(call.args) != 1:
            return None

        accumulator = func.value.id
        source_expr = ast.unparse(call.args[0])
        return (accumulator, source_expr)

    def _get_action_name(self, node: ast.expr) -> str | None:
        """Get action name from a call's func."""
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            return node.attr
        return None

    def _get_simple_name(self, node: ast.expr) -> str | None:
        """Get simple variable name from expression."""
        if isinstance(node, ast.Name):
            return node.id
        return None

    def _is_empty_list_init(self, stmt: ast.stmt) -> bool:
        """Check if statement is `x = []`."""
        if not isinstance(stmt, ast.Assign):
            return False
        if len(stmt.targets) != 1:
            return False
        if not isinstance(stmt.targets[0], ast.Name):
            return False
        if not isinstance(stmt.value, ast.List):
            return False
        return len(stmt.value.elts) == 0

    def _contains_action(self, node: ast.AST) -> bool:
        """Check if AST node contains any action calls."""
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                name = self._get_action_name(child.func)
                if name and name in self.action_names:
                    return True
        return False

    def _make_python_block(
        self,
        stmts: list[ast.stmt],
        location: SourceLocation | None = None,
        known_vars: set[str] | None = None,
    ) -> RappelPythonBlock:
        """Create a python block from statements with full analysis."""
        code = "\n".join(ast.unparse(s) for s in stmts)

        # Analyze inputs and outputs
        analyzer = VariableAnalyzer(known_vars or self._known_vars)
        inputs, outputs = analyzer.analyze(stmts)

        # Resolve imports and definitions if we have a module index
        imports: list[str] = []
        definitions: list[str] = []

        if self.module_index:
            # Find all referenced names
            referenced: set[str] = set()
            for stmt in stmts:
                for node in ast.walk(stmt):
                    if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                        referenced.add(node.id)
                    elif isinstance(node, ast.Attribute):
                        # Get the root name
                        root = node
                        while isinstance(root, ast.Attribute):
                            root = root.value
                        if isinstance(root, ast.Name):
                            referenced.add(root.id)

            # Filter to only names in module index
            to_resolve = referenced & self.module_index.symbols
            if to_resolve:
                imports, definitions = self.module_index.resolve(to_resolve)

        return RappelPythonBlock(
            code=code,
            imports=imports,
            definitions=definitions,
            inputs=inputs,
            outputs=outputs,
            location=location,
        )


# =============================================================================
# Serializer: Rappel IR -> Text
# =============================================================================


class RappelSerializer:
    """Serialize Rappel IR to a human-readable text format."""

    def __init__(self, include_locations: bool = False):
        self.include_locations = include_locations

    def serialize(self, workflow: RappelWorkflow) -> str:
        lines: list[str] = []

        # Header
        params_str = ", ".join(
            f"{name}: {typ}" if typ else name for name, typ in workflow.params
        )
        ret_str = f" -> {workflow.return_type}" if workflow.return_type else ""
        lines.append(f"workflow {workflow.name}({params_str}){ret_str}:")

        # Body
        for stmt in workflow.body:
            stmt_lines = self._serialize_statement(stmt)
            for line in stmt_lines:
                lines.append("    " + line)

        return "\n".join(lines)

    def _loc_comment(self, location: SourceLocation | None) -> str:
        if self.include_locations and location:
            return f"  # {location}"
        return ""

    def _serialize_statement(self, stmt: RappelStatement) -> list[str]:
        if isinstance(stmt, RappelActionCall):
            return self._serialize_action(stmt)
        if isinstance(stmt, RappelGather):
            return self._serialize_gather(stmt)
        if isinstance(stmt, RappelPythonBlock):
            return self._serialize_python_block(stmt)
        if isinstance(stmt, RappelLoop):
            return self._serialize_loop(stmt)
        if isinstance(stmt, RappelConditional):
            return self._serialize_conditional(stmt)
        if isinstance(stmt, RappelTryExcept):
            return self._serialize_try_except(stmt)
        if isinstance(stmt, RappelSleep):
            return self._serialize_sleep(stmt)
        if isinstance(stmt, RappelReturn):
            return self._serialize_return(stmt)
        if isinstance(stmt, RappelComprehensionMap):
            return self._serialize_comprehension_map(stmt)
        return [f"# UNKNOWN: {type(stmt).__name__}"]

    def _serialize_action(self, action: RappelActionCall) -> list[str]:
        kwargs_str = ", ".join(f"{k}={v}" for k, v in action.kwargs.items())
        module_prefix = f"{action.module}." if action.module else ""
        call_str = f"@{module_prefix}{action.action}({kwargs_str})"

        if action.config:
            config_parts = []
            if action.config.timeout_seconds:
                config_parts.append(f"timeout={action.config.timeout_seconds}s")
            if action.config.max_retries:
                config_parts.append(f"retry={action.config.max_retries}")
            if action.config.backoff:
                b = action.config.backoff
                if b.kind == "linear":
                    config_parts.append(f"backoff=linear({b.base_delay_ms}ms)")
                else:
                    config_parts.append(
                        f"backoff=exp({b.base_delay_ms}ms, {b.multiplier}x)"
                    )
            call_str += f" [policy: {', '.join(config_parts)}]"

        loc = self._loc_comment(action.location)
        if action.target:
            return [f"{action.target} = {call_str}{loc}"]
        return [f"{call_str}{loc}"]

    def _serialize_gather(self, gather: RappelGather) -> list[str]:
        calls = []
        for call in gather.calls:
            kwargs_str = ", ".join(f"{k}={v}" for k, v in call.kwargs.items())
            module_prefix = f"{call.module}." if call.module else ""
            calls.append(f"@{module_prefix}{call.action}({kwargs_str})")

        loc = self._loc_comment(gather.location)
        if gather.target:
            return [f"{gather.target} = parallel({', '.join(calls)}){loc}"]
        return [f"parallel({', '.join(calls)}){loc}"]

    def _serialize_python_block(self, block: RappelPythonBlock) -> list[str]:
        lines = []

        # Show inputs/outputs
        io_parts = []
        if block.inputs:
            io_parts.append(f"reads: {', '.join(block.inputs)}")
        if block.outputs:
            io_parts.append(f"writes: {', '.join(block.outputs)}")
        io_str = f" ({'; '.join(io_parts)})" if io_parts else ""

        lines.append(f"python{io_str} {{")

        # Show imports if any
        if block.imports:
            for imp in block.imports:
                lines.append(f"    # import: {imp.strip()}")

        # Show definitions if any
        if block.definitions:
            for defn in block.definitions:
                first_line = defn.split("\n")[0]
                lines.append(f"    # def: {first_line}...")

        for line in block.code.split("\n"):
            lines.append("    " + line)
        lines.append("}")
        return lines

    def _serialize_loop(self, loop: RappelLoop) -> list[str]:
        lines = []

        # Loop header with accumulators declared
        acc_str = ", ".join(loop.accumulators)
        loc = self._loc_comment(loop.location)
        lines.append(f"loop {loop.loop_var} in {loop.iterator_expr} -> [{acc_str}]:{loc}")

        # Preamble (marked as python)
        if loop.preamble:
            for pre in loop.preamble:
                io_parts = []
                if pre.inputs:
                    io_parts.append(f"reads: {', '.join(pre.inputs)}")
                if pre.outputs:
                    io_parts.append(f"writes: {', '.join(pre.outputs)}")
                io_str = f" ({'; '.join(io_parts)})" if io_parts else ""
                lines.append(f"    # preamble{io_str}")
                for line in pre.code.split("\n"):
                    lines.append("    " + line)

        # Actions
        for action in loop.body:
            action_lines = self._serialize_action(action)
            for line in action_lines:
                lines.append("    " + line)

        # Yields (what gets collected)
        for acc, source in loop.append_exprs:
            lines.append(f"    yield {source} -> {acc}")

        return lines

    def _serialize_conditional(self, cond: RappelConditional) -> list[str]:
        lines = []
        for i, branch in enumerate(cond.branches):
            loc = self._loc_comment(branch.location)
            if i == 0:
                lines.append(f"branch if {branch.guard}:{loc}")
            elif i == len(cond.branches) - 1 and "not " in branch.guard:
                lines.append(f"branch else:{loc}")
            else:
                lines.append(f"branch elif {branch.guard}:{loc}")

            # Preamble
            if branch.preamble:
                for pre in branch.preamble:
                    io_parts = []
                    if pre.inputs:
                        io_parts.append(f"reads: {', '.join(pre.inputs)}")
                    if pre.outputs:
                        io_parts.append(f"writes: {', '.join(pre.outputs)}")
                    io_str = f" ({'; '.join(io_parts)})" if io_parts else ""
                    lines.append(f"    # preamble{io_str}")
                    for line in pre.code.split("\n"):
                        lines.append("    " + line)

            # Actions (can be multiple, chained in DAG)
            for action in branch.actions:
                action_lines = self._serialize_action(action)
                for line in action_lines:
                    lines.append("    " + line)

            # Postamble
            if branch.postamble:
                for post in branch.postamble:
                    io_parts = []
                    if post.inputs:
                        io_parts.append(f"reads: {', '.join(post.inputs)}")
                    if post.outputs:
                        io_parts.append(f"writes: {', '.join(post.outputs)}")
                    io_str = f" ({'; '.join(io_parts)})" if io_parts else ""
                    lines.append(f"    # postamble{io_str}")
                    for line in post.code.split("\n"):
                        lines.append("    " + line)

        return lines

    def _serialize_try_except(self, te: RappelTryExcept) -> list[str]:
        loc = self._loc_comment(te.location)
        lines = [f"try:{loc}"]
        for action in te.try_body:
            action_lines = self._serialize_action(action)
            for line in action_lines:
                lines.append("    " + line)

        for handler in te.handlers:
            hloc = self._loc_comment(handler.location)
            if handler.exception_types == [(None, None)]:
                lines.append(f"except:{hloc}")
            else:
                types = []
                for mod, typ in handler.exception_types:
                    if mod:
                        types.append(f"{mod}.{typ}")
                    elif typ:
                        types.append(typ)
                if len(types) == 1:
                    lines.append(f"except {types[0]}:{hloc}")
                else:
                    lines.append(f"except ({', '.join(types)}):{hloc}")

            for action in handler.body:
                action_lines = self._serialize_action(action)
                for line in action_lines:
                    lines.append("    " + line)

        return lines

    def _serialize_sleep(self, sleep: RappelSleep) -> list[str]:
        loc = self._loc_comment(sleep.location)
        return [f"@sleep({sleep.duration_expr}){loc}"]

    def _serialize_return(self, ret: RappelReturn) -> list[str]:
        loc = self._loc_comment(ret.location)
        if ret.value is None:
            return [f"return{loc}"]
        if isinstance(ret.value, str):
            return [f"return {ret.value}{loc}"]
        if isinstance(ret.value, RappelActionCall):
            action_lines = self._serialize_action(ret.value)
            return [f"return {action_lines[0]}"]
        if isinstance(ret.value, RappelGather):
            gather_lines = self._serialize_gather(ret.value)
            return [f"return {gather_lines[0]}"]
        return [f"return {ret.value}{loc}"]

    def _serialize_comprehension_map(self, comp: RappelComprehensionMap) -> list[str]:
        kwargs_str = ", ".join(f"{k}={v}" for k, v in comp.action.kwargs.items())
        module_prefix = f"{comp.action.module}." if comp.action.module else ""
        call_str = f"@{module_prefix}{comp.action.action}({kwargs_str})"
        loc = self._loc_comment(comp.location)
        if comp.target:
            return [
                f"{comp.target} = spread {call_str} over {comp.iterable} as {comp.loop_var}{loc}"
            ]
        return [f"spread {call_str} over {comp.iterable} as {comp.loop_var}{loc}"]


# =============================================================================
# Example Workflows
# =============================================================================

EXAMPLE_WORKFLOWS = '''
import asyncio
import math
from datetime import timedelta
from dataclasses import dataclass

# Fake action definitions (in real code these would be @action decorated)
async def fetch_left() -> int: ...
async def fetch_right() -> int: ...
async def double(value: int) -> int: ...
async def summarize(values: list) -> float: ...
async def persist_summary(total: float) -> None: ...
async def fetch_number(idx: int) -> int: ...
async def validate_order(order: dict) -> dict: ...
async def process_payment(validated: dict) -> dict: ...
async def send_confirmation(payment: dict) -> str: ...
async def evaluate_high(value: int) -> str: ...
async def evaluate_medium(value: int) -> str: ...
async def evaluate_low(value: int) -> str: ...
async def risky_action() -> int: ...
async def fallback_action() -> int: ...
async def get_timestamp() -> str: ...
async def positional_action(prefix: str, count: int, suffix: str = "") -> str: ...


@dataclass
class Record:
    amount: float


def helper_threshold(record: Record) -> bool:
    return record.amount > 10


class Workflow:
    pass


# Example 1: Sequential actions
class SequentialWorkflow(Workflow):
    async def run(self) -> float:
        records = await fetch_left()
        total = await summarize(values=[records])
        await persist_summary(total=total)
        return total


# Example 2: Parallel gather
class GatherWorkflow(Workflow):
    async def run(self) -> tuple:
        results = await asyncio.gather(fetch_left(), fetch_right())
        return results


# Example 3: Single-action loop with accumulator
class SingleActionLoopWorkflow(Workflow):
    async def run(self, numbers: list) -> list:
        results = []
        for number in numbers:
            doubled = await double(value=number)
            results.append(doubled)
        return results


# Example 4: Multi-action loop
class MultiActionLoopWorkflow(Workflow):
    async def run(self, orders: list) -> list:
        confirmations = []
        for order in orders:
            validated = await validate_order(order=order)
            payment = await process_payment(validated=validated)
            confirmation = await send_confirmation(payment=payment)
            confirmations.append(confirmation)
        return confirmations


# Example 5: Loop with preamble
class LoopWithPreambleWorkflow(Workflow):
    async def run(self, items: list) -> list:
        results = []
        for item in items:
            adjusted = item * 2
            processed = await double(value=adjusted)
            results.append(processed)
        return results


# Example 6: Conditional branches
class ConditionalWorkflow(Workflow):
    async def run(self, value: int) -> str:
        if value >= 75:
            result = await evaluate_high(value=value)
        elif value >= 25:
            result = await evaluate_medium(value=value)
        else:
            result = await evaluate_low(value=value)
        return result


# Example 7: Conditional with preamble/postamble
class ConditionalWithExtrasWorkflow(Workflow):
    async def run(self, value: int) -> str:
        if value > 50:
            adjusted = value * 2
            result = await evaluate_high(value=adjusted)
            label = "high"
        else:
            adjusted = value
            result = await evaluate_low(value=adjusted)
            label = "low"
        return f"{label}:{result}"


# Example 8: Try/except
class TryExceptWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except ValueError:
            result = await fallback_action()
        return result


# Example 9: Try with multiple exception types
class TryMultiExceptWorkflow(Workflow):
    async def run(self) -> int:
        try:
            result = await risky_action()
        except (ValueError, TypeError):
            result = await fallback_action()
        return result


# Example 10: Sleep
class SleepWorkflow(Workflow):
    async def run(self) -> str:
        started = await get_timestamp()
        await asyncio.sleep(60)
        ended = await get_timestamp()
        return f"{started}-{ended}"


# Example 11: Gather then map comprehension
class GatherAndMapWorkflow(Workflow):
    async def run(self) -> list:
        numbers = await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2))
        doubled = [await double(value=n) for n in numbers]
        return doubled


# Example 12: Complex mixed workflow with python block using imports
class ComplexWorkflow(Workflow):
    async def run(self, threshold: int) -> str:
        # Parallel fetch
        raw = await asyncio.gather(fetch_left(), fetch_right())

        # Pure python computation using math module
        combined = int(math.sqrt(sum(raw)))

        # Conditional
        if combined > threshold:
            result = await evaluate_high(value=combined)
        else:
            result = await evaluate_low(value=combined)

        return result


# Example 13: run_action with policies
class PolicyWorkflow(Workflow):
    async def run(self) -> int:
        result = await self.run_action(
            risky_action(),
            timeout=300,
            retry=RetryPolicy(attempts=3),
            backoff=LinearBackoff(base_delay_ms=1000),
        )
        return result


# Example 14: Multi-accumulator loop
class MultiAccumulatorWorkflow(Workflow):
    async def run(self, items: list) -> tuple:
        results = []
        metrics = []
        for item in items:
            processed = await double(value=item)
            results.append(processed)
            metrics.append(processed * 2)
        return (results, metrics)


# Example 15: Nested workflow - gather then loop
class GatherThenLoopWorkflow(Workflow):
    async def run(self) -> list:
        # First gather some data
        seeds = await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2), fetch_number(idx=3))

        # Then process each with a loop
        results = []
        for seed in seeds:
            processed = await double(value=seed)
            results.append(processed)

        return results


# Example 16: Multiple sequential gathers
class MultiGatherWorkflow(Workflow):
    async def run(self) -> list:
        batch1 = await asyncio.gather(fetch_number(idx=1), fetch_number(idx=2))
        batch2 = await asyncio.gather(fetch_number(idx=3), fetch_number(idx=4))
        combined = list(batch1) + list(batch2)
        return combined


# Example 17: Try with multiple actions in try block
class TryMultiActionWorkflow(Workflow):
    async def run(self) -> int:
        try:
            a = await fetch_number(idx=1)
            b = await double(value=a)
            c = await double(value=b)
        except ValueError:
            c = await fallback_action()
        return c


# Example 18: Positional arguments
class PositionalArgsWorkflow(Workflow):
    async def run(self) -> str:
        result = await positional_action("hello", 42, suffix="!")
        return result


# Example 19: Python block with helper function
class HelperFunctionWorkflow(Workflow):
    async def run(self) -> float:
        records = [Record(5), Record(20), Record(3)]
        positives = [r.amount for r in records if helper_threshold(r)]
        total = await summarize(values=positives)
        return total


# Example 20: Conditional with multiple actions per branch
class MultiActionConditionalWorkflow(Workflow):
    async def run(self, mode: str) -> str:
        if mode == "full":
            # Multiple actions in the "if" branch
            a = await fetch_left()
            b = await double(value=a)
            result = await summarize(values=[b])
        else:
            # Single action in the "else" branch
            result = await fetch_right()
        return result


# Fake classes for policy parsing
class RetryPolicy:
    def __init__(self, attempts=None): pass

class LinearBackoff:
    def __init__(self, base_delay_ms=0): pass

class ExponentialBackoff:
    def __init__(self, base_delay_ms=0, multiplier=2.0): pass
'''


def main():
    # Parse the example module
    tree = ast.parse(EXAMPLE_WORKFLOWS)

    # Build action definitions with signatures
    action_defs: dict[str, ActionDefinition] = {}
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef) and not any(
            isinstance(d, ast.Name) and d.id == "action" for d in node.decorator_list
        ):
            # Extract parameter names from function def
            param_names = [
                arg.arg for arg in node.args.args if arg.arg != "self"
            ]
            action_defs[node.name] = ActionDefinition(
                name=node.name,
                module="example_module",
                signature=None,  # Would come from inspect in real code
                param_names=param_names,
            )

    print(f"Discovered actions: {sorted(action_defs.keys())}\n")

    # Create module index for import/definition resolution
    module_index = ModuleIndex(EXAMPLE_WORKFLOWS)
    print(f"Module symbols: {sorted(module_index.symbols)}\n")

    # Find and parse all workflow classes
    serializer = RappelSerializer(include_locations=True)

    for node in tree.body:
        if (
            isinstance(node, ast.ClassDef)
            and node.name.endswith("Workflow")
            and node.name != "Workflow"
        ):
            print(f"{'=' * 70}")
            print(f"Parsing: {node.name}")
            print("=" * 70)

            # Find the run method
            run_method = None
            for item in node.body:
                if isinstance(item, ast.AsyncFunctionDef) and item.name == "run":
                    run_method = item
                    break

            if run_method is None:
                print("  No run() method found\n")
                continue

            try:
                parser = RappelParser(
                    action_defs=action_defs,
                    module_index=module_index,
                    source=EXAMPLE_WORKFLOWS,
                )
                workflow = parser.parse_workflow(run_method)
                workflow.name = node.name  # Use class name

                print("\nOriginal Python:")
                print("-" * 40)
                print(textwrap.indent(ast.unparse(run_method), "  "))

                print("\nRappel IR (text):")
                print("-" * 40)
                print(textwrap.indent(serializer.serialize(workflow), "  "))

            except RappelParseError as e:
                print(f"\n  PARSE ERROR: {e}")

            print()


if __name__ == "__main__":
    main()
