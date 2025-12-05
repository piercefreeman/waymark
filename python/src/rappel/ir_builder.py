"""
IR Builder - Converts Python workflow AST to Rappel IR (ast.proto).

This module parses Python workflow classes and produces the IR representation
that can be sent to the Rust runtime for execution.

The IR builder performs deep transformations to convert Python patterns into
valid Rappel IR structures. Each control flow body (try, for, if branches)
should have at most ONE action/function call. When bodies have multiple
action calls, they are wrapped into synthetic functions.

Transformations:
1. **Try body wrapping**: Wraps multi-action try bodies into synthetic functions
2. **For loop body wrapping**: Wraps multi-action for bodies into synthetic functions
3. **If branch wrapping**: Wraps multi-action if/elif/else branches into synthetic functions
4. **Exception handler wrapping**: Wraps multi-action handlers into synthetic functions

Validation:
The IR builder proactively detects unsupported Python patterns and raises
UnsupportedPatternError with clear recommendations for how to rewrite the code.
"""

import ast
import inspect
import textwrap
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from proto import ast_pb2 as ir


class UnsupportedPatternError(Exception):
    """Raised when the IR builder encounters an unsupported Python pattern.

    This error includes a recommendation for how to rewrite the code to use
    supported patterns.
    """

    def __init__(
        self,
        message: str,
        recommendation: str,
        line: Optional[int] = None,
        col: Optional[int] = None,
    ):
        self.message = message
        self.recommendation = recommendation
        self.line = line
        self.col = col

        location = f" (line {line})" if line else ""
        full_message = f"{message}{location}\n\nRecommendation: {recommendation}"
        super().__init__(full_message)


# Recommendations for common unsupported patterns
RECOMMENDATIONS = {
    "fstring": (
        "F-strings are not supported in workflow code because they require "
        "runtime string interpolation.\n"
        "Use an @action to perform string formatting:\n\n"
        "    @action\n"
        "    async def format_message(value: int) -> str:\n"
        "        return f'Result: {value}'"
    ),
    "delete": (
        "The 'del' statement is not supported in workflow code.\n"
        "Use an @action to perform mutations:\n\n"
        "    @action\n"
        "    async def remove_key(data: dict, key: str) -> dict:\n"
        "        del data[key]\n"
        "        return data"
    ),
    "while_loop": (
        "While loops are not supported in workflow code because they can run "
        "indefinitely.\n"
        "Use a for loop with a fixed range, or restructure as recursive workflow calls."
    ),
    "with_statement": (
        "Context managers (with statements) are not supported in workflow code.\n"
        "Use an @action to handle resource management:\n\n"
        "    @action\n"
        "    async def read_file(path: str) -> str:\n"
        "        with open(path) as f:\n"
        "            return f.read()"
    ),
    "raise_statement": (
        "The 'raise' statement is not supported directly in workflow code.\n"
        "Use an @action that raises exceptions, or return error values."
    ),
    "assert_statement": (
        "Assert statements are not supported in workflow code.\n"
        "Use an @action for validation, or use if statements with explicit error handling."
    ),
    "lambda": (
        "Lambda expressions are not supported in workflow code.\n"
        "Use an @action to define the function logic."
    ),
    "list_comprehension": (
        "List comprehensions are only supported inside asyncio.gather(*[...]).\n"
        "For other cases, use a for loop or an @action."
    ),
    "dict_comprehension": (
        "Dict comprehensions are not supported in workflow code.\n"
        "Use an @action to build dictionaries:\n\n"
        "    @action\n"
        "    async def build_dict(items: list) -> dict:\n"
        "        return {k: v for k, v in items}"
    ),
    "set_comprehension": (
        "Set comprehensions are not supported in workflow code.\nUse an @action to build sets."
    ),
    "generator": (
        "Generator expressions are not supported in workflow code.\n"
        "Use a list or an @action instead."
    ),
    "walrus": (
        "The walrus operator (:=) is not supported in workflow code.\n"
        "Use separate assignment statements instead."
    ),
    "match": (
        "Match statements are not supported in workflow code.\nUse if/elif/else chains instead."
    ),
    "gather_variable_spread": (
        "Spreading a variable in asyncio.gather() is not supported because it requires "
        "data flow analysis to determine the contents.\n"
        "Use a list comprehension directly in gather:\n\n"
        "    # Instead of:\n"
        "    tasks = []\n"
        "    for i in range(count):\n"
        "        tasks.append(process(value=i))\n"
        "    results = await asyncio.gather(*tasks)\n\n"
        "    # Use:\n"
        "    results = await asyncio.gather(*[process(value=i) for i in range(count)])"
    ),
    "for_loop_append_pattern": (
        "Building a task list in a for loop then spreading in asyncio.gather() is not "
        "supported.\n"
        "Use a list comprehension directly in gather:\n\n"
        "    # Instead of:\n"
        "    tasks = []\n"
        "    for i in range(count):\n"
        "        tasks.append(process(value=i))\n"
        "    results = await asyncio.gather(*tasks)\n\n"
        "    # Use:\n"
        "    results = await asyncio.gather(*[process(value=i) for i in range(count)])"
    ),
    "global_statement": (
        "Global statements are not supported in workflow code.\n"
        "Use workflow state or pass values explicitly."
    ),
    "nonlocal_statement": (
        "Nonlocal statements are not supported in workflow code.\n"
        "Use explicit parameter passing instead."
    ),
    "import_statement": (
        "Import statements inside workflow run() are not supported.\n"
        "Place imports at the module level."
    ),
    "class_def": (
        "Class definitions inside workflow run() are not supported.\n"
        "Define classes at the module level."
    ),
    "function_def": (
        "Nested function definitions inside workflow run() are not supported.\n"
        "Define functions at the module level or use @action."
    ),
    "yield_statement": (
        "Yield statements are not supported in workflow code.\n"
        "Workflows must return a complete result, not generate values incrementally."
    ),
}


if TYPE_CHECKING:
    from .workflow import Workflow


@dataclass
class ActionDefinition:
    """Definition of an action function."""

    action_name: str
    module_name: Optional[str]
    signature: inspect.Signature


@dataclass
class TransformContext:
    """Context for IR transformations."""

    # Counter for generating unique function names
    implicit_fn_counter: int = 0
    # Implicit functions generated during transformation
    implicit_functions: List[ir.FunctionDef] = None  # type: ignore

    def __post_init__(self) -> None:
        if self.implicit_functions is None:
            self.implicit_functions = []

    def next_implicit_fn_name(self, prefix: str = "implicit") -> str:
        """Generate a unique implicit function name."""
        self.implicit_fn_counter += 1
        return f"__{prefix}_{self.implicit_fn_counter}__"


def build_workflow_ir(workflow_cls: type["Workflow"]) -> ir.Program:
    """Build an IR Program from a workflow class.

    Args:
        workflow_cls: The workflow class to convert.

    Returns:
        An IR Program proto message.
    """
    original_run = getattr(workflow_cls, "__workflow_run_impl__", None)
    if original_run is None:
        original_run = workflow_cls.__dict__.get("run")
    if original_run is None:
        raise ValueError(f"workflow {workflow_cls!r} missing run implementation")

    module = inspect.getmodule(original_run)
    if module is None:
        raise ValueError(f"unable to locate module for workflow {workflow_cls!r}")

    # Get the function source and parse it
    function_source = textwrap.dedent(inspect.getsource(original_run))
    tree = ast.parse(function_source)

    # Discover actions in the module
    action_defs = _discover_action_names(module)

    # Discover imports for built-in detection (e.g., from asyncio import sleep)
    imported_names = _discover_module_imports(module)

    # Build the IR with transformation context
    ctx = TransformContext()
    builder = IRBuilder(action_defs, ctx, imported_names)
    builder.visit(tree)

    # Create the Program with the main function and any implicit functions
    program = ir.Program()

    # Add implicit functions first (they may be called by the main function)
    for implicit_fn in ctx.implicit_functions:
        program.functions.append(implicit_fn)

    # Add the main function
    if builder.function_def:
        program.functions.append(builder.function_def)

    return program


def _discover_action_names(module: Any) -> Dict[str, ActionDefinition]:
    """Discover all @action decorated functions in a module."""
    names: Dict[str, ActionDefinition] = {}
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        action_name = getattr(attr, "__rappel_action_name__", None)
        action_module = getattr(attr, "__rappel_action_module__", None)
        if callable(attr) and action_name:
            signature = inspect.signature(attr)
            names[attr_name] = ActionDefinition(
                action_name=action_name,
                module_name=action_module or module.__name__,
                signature=signature,
            )
    return names


@dataclass
class ImportedName:
    """Tracks an imported name and its source module."""

    local_name: str  # Name used in code (e.g., "sleep")
    module: str  # Source module (e.g., "asyncio")
    original_name: str  # Original name in source module (e.g., "sleep")


def _discover_module_imports(module: Any) -> Dict[str, ImportedName]:
    """Discover imports in a module by parsing its source.

    Tracks imports like:
    - from asyncio import sleep  -> {"sleep": ImportedName("sleep", "asyncio", "sleep")}
    - from asyncio import sleep as s -> {"s": ImportedName("s", "asyncio", "sleep")}
    """
    imported: Dict[str, ImportedName] = {}

    try:
        source = inspect.getsource(module)
        tree = ast.parse(source)
    except (OSError, TypeError):
        # Can't get source (e.g., built-in module)
        return imported

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                local_name = alias.asname if alias.asname else alias.name
                imported[local_name] = ImportedName(
                    local_name=local_name,
                    module=node.module,
                    original_name=alias.name,
                )

    return imported


class IRBuilder(ast.NodeVisitor):
    """Builds IR from Python AST with deep transformations."""

    def __init__(
        self,
        action_defs: Dict[str, ActionDefinition],
        ctx: TransformContext,
        imported_names: Optional[Dict[str, ImportedName]] = None,
    ):
        self._action_defs = action_defs
        self._ctx = ctx
        self._imported_names = imported_names or {}
        self.function_def: Optional[ir.FunctionDef] = None
        self._statements: List[ir.Statement] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        """Visit a function definition (the workflow's run method)."""
        # Extract inputs from function parameters (skip 'self')
        inputs: List[str] = []
        for arg in node.args.args[1:]:  # Skip 'self'
            inputs.append(arg.arg)

        # Create the function definition
        self.function_def = ir.FunctionDef(
            name=node.name,
            io=ir.IoDecl(inputs=inputs, outputs=[]),
            span=_make_span(node),
        )

        # Visit the body - _visit_statement now returns a list
        self._statements = []
        for stmt in node.body:
            ir_stmts = self._visit_statement(stmt)
            self._statements.extend(ir_stmts)

        # Set the body
        self.function_def.body.CopyFrom(ir.Block(statements=self._statements))

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
        """Visit an async function definition (the workflow's run method)."""
        # Handle async the same way as sync for IR building
        inputs: List[str] = []
        for arg in node.args.args[1:]:  # Skip 'self'
            inputs.append(arg.arg)

        self.function_def = ir.FunctionDef(
            name=node.name,
            io=ir.IoDecl(inputs=inputs, outputs=[]),
            span=_make_span(node),
        )

        self._statements = []
        for stmt in node.body:
            ir_stmts = self._visit_statement(stmt)
            self._statements.extend(ir_stmts)

        self.function_def.body.CopyFrom(ir.Block(statements=self._statements))

    def _visit_statement(self, node: ast.stmt) -> List[ir.Statement]:
        """Convert a Python statement to IR Statement(s).

        Returns a list because some transformations (like try block hoisting)
        may expand a single Python statement into multiple IR statements.

        Raises UnsupportedPatternError for unsupported statement types.
        """
        if isinstance(node, ast.Assign):
            result = self._visit_assign(node)
            return [result] if result else []
        elif isinstance(node, ast.Expr):
            result = self._visit_expr_stmt(node)
            return [result] if result else []
        elif isinstance(node, ast.For):
            return self._visit_for(node)
        elif isinstance(node, ast.If):
            result = self._visit_if(node)
            return [result] if result else []
        elif isinstance(node, ast.Try):
            return self._visit_try(node)
        elif isinstance(node, ast.Return):
            result = self._visit_return(node)
            return [result] if result else []
        elif isinstance(node, ast.AugAssign):
            result = self._visit_aug_assign(node)
            return [result] if result else []
        elif isinstance(node, ast.Pass):
            # Pass statements are fine, they just don't produce IR
            return []

        # Check for unsupported statement types
        self._check_unsupported_statement(node)

        return []

    def _check_unsupported_statement(self, node: ast.stmt) -> None:
        """Check for unsupported statement types and raise descriptive errors."""
        line = getattr(node, "lineno", None)
        col = getattr(node, "col_offset", None)

        if isinstance(node, ast.While):
            raise UnsupportedPatternError(
                "While loops are not supported",
                RECOMMENDATIONS["while_loop"],
                line=line,
                col=col,
            )
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            raise UnsupportedPatternError(
                "Context managers (with statements) are not supported",
                RECOMMENDATIONS["with_statement"],
                line=line,
                col=col,
            )
        elif isinstance(node, ast.Raise):
            raise UnsupportedPatternError(
                "The 'raise' statement is not supported",
                RECOMMENDATIONS["raise_statement"],
                line=line,
                col=col,
            )
        elif isinstance(node, ast.Assert):
            raise UnsupportedPatternError(
                "Assert statements are not supported",
                RECOMMENDATIONS["assert_statement"],
                line=line,
                col=col,
            )
        elif isinstance(node, ast.Delete):
            raise UnsupportedPatternError(
                "The 'del' statement is not supported",
                RECOMMENDATIONS["delete"],
                line=line,
                col=col,
            )
        elif isinstance(node, ast.Global):
            raise UnsupportedPatternError(
                "Global statements are not supported",
                RECOMMENDATIONS["global_statement"],
                line=line,
                col=col,
            )
        elif isinstance(node, ast.Nonlocal):
            raise UnsupportedPatternError(
                "Nonlocal statements are not supported",
                RECOMMENDATIONS["nonlocal_statement"],
                line=line,
                col=col,
            )
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            raise UnsupportedPatternError(
                "Import statements inside workflow run() are not supported",
                RECOMMENDATIONS["import_statement"],
                line=line,
                col=col,
            )
        elif isinstance(node, ast.ClassDef):
            raise UnsupportedPatternError(
                "Class definitions inside workflow run() are not supported",
                RECOMMENDATIONS["class_def"],
                line=line,
                col=col,
            )
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            raise UnsupportedPatternError(
                "Nested function definitions are not supported",
                RECOMMENDATIONS["function_def"],
                line=line,
                col=col,
            )
        elif hasattr(ast, "Match") and isinstance(node, ast.Match):
            raise UnsupportedPatternError(
                "Match statements are not supported",
                RECOMMENDATIONS["match"],
                line=line,
                col=col,
            )

    def _visit_assign(self, node: ast.Assign) -> Optional[ir.Statement]:
        """Convert assignment to IR.

        All assignments with targets use the Assignment statement type.
        This provides uniform unpacking support for:
        - Action calls: a, b = @get_pair()
        - Parallel blocks: a, b = parallel: @x() @y()
        - Regular expressions: a, b = some_list
        """
        stmt = ir.Statement(span=_make_span(node))
        targets = self._get_assign_targets(node.targets)

        # Check for asyncio.gather() - convert to parallel expression
        if isinstance(node.value, ast.Await) and isinstance(node.value.value, ast.Call):
            gather_call = node.value.value
            if self._is_asyncio_gather_call(gather_call):
                parallel_expr = self._convert_asyncio_gather_to_parallel_expr(gather_call)
                if parallel_expr:
                    value = ir.Expr(parallel_expr=parallel_expr, span=_make_span(node))
                    assign = ir.Assignment(targets=targets, value=value)
                    stmt.assignment.CopyFrom(assign)
                    return stmt

        # Check if this is an action call - wrap in Assignment for uniform unpacking
        action_call = self._extract_action_call(node.value)
        if action_call:
            value = ir.Expr(action_call=action_call, span=_make_span(node))
            assign = ir.Assignment(targets=targets, value=value)
            stmt.assignment.CopyFrom(assign)
            return stmt

        # Regular assignment (variables, literals, expressions)
        value_expr = _expr_to_ir(node.value)
        if value_expr:
            assign = ir.Assignment(targets=targets, value=value_expr)
            stmt.assignment.CopyFrom(assign)
            return stmt

        return None

    def _visit_expr_stmt(self, node: ast.Expr) -> Optional[ir.Statement]:
        """Convert expression statement to IR (side effect only, no assignment)."""
        stmt = ir.Statement(span=_make_span(node))

        # Check for asyncio.gather() - convert to parallel block statement (side effect)
        if isinstance(node.value, ast.Await) and isinstance(node.value.value, ast.Call):
            gather_call = node.value.value
            if self._is_asyncio_gather_call(gather_call):
                parallel_expr = self._convert_asyncio_gather_to_parallel_expr(gather_call)
                if parallel_expr:
                    # Side effect only - use ParallelBlock statement
                    parallel = ir.ParallelBlock()
                    parallel.calls.extend(parallel_expr.calls)
                    stmt.parallel_block.CopyFrom(parallel)
                    return stmt

        # Check if this is an action call (side effect only)
        action_call = self._extract_action_call(node.value)
        if action_call:
            stmt.action_call.CopyFrom(action_call)
            return stmt

        # Regular expression
        expr = _expr_to_ir(node.value)
        if expr:
            stmt.expr_stmt.CopyFrom(ir.ExprStmt(expr=expr))
            return stmt

        return None

    def _visit_for(self, node: ast.For) -> List[ir.Statement]:
        """Convert for loop to IR with body wrapping transformation.

        If the for loop body has multiple action/function calls, we wrap them
        into a synthetic function and replace the body with a single call.

        Python:
            for item in items:
                a = await step_one(item)
                b = await step_two(a)

        Becomes IR equivalent of:
            fn __for_body_1__(item):
                a = @step_one(item=item)
                b = @step_two(a=a)
                return b

            for item in items:
                __for_body_1__(item=item)
        """
        # Get loop variables
        loop_vars: List[str] = []
        if isinstance(node.target, ast.Name):
            loop_vars.append(node.target.id)
        elif isinstance(node.target, ast.Tuple):
            for elt in node.target.elts:
                if isinstance(elt, ast.Name):
                    loop_vars.append(elt.id)

        # Get iterable
        iterable = _expr_to_ir(node.iter)
        if not iterable:
            return []

        # Build body statements (recursively transforms nested structures)
        body_stmts: List[ir.Statement] = []
        for body_node in node.body:
            stmts = self._visit_statement(body_node)
            body_stmts.extend(stmts)

        # Wrap for loop body if multiple calls (use loop_vars as inputs)
        if self._count_calls(body_stmts) > 1:
            body_stmts = self._wrap_body_as_function(body_stmts, "for_body", node, inputs=loop_vars)

        # For loops use SingleCallBody (at most one action/function call per iteration)
        # Use spread for parallel iteration over collections.
        stmt = ir.Statement(span=_make_span(node))
        for_loop = ir.ForLoop(
            loop_vars=loop_vars,
            iterable=iterable,
            body=self._stmts_to_single_call_body(body_stmts, _make_span(node)),
        )
        stmt.for_loop.CopyFrom(for_loop)
        return [stmt]

    def _visit_if(self, node: ast.If) -> Optional[ir.Statement]:
        """Convert if statement to IR conditional with branch wrapping.

        If any branch has multiple action calls, we wrap it into a synthetic
        function to ensure each branch has at most one call.

        Python:
            if condition:
                a = await action_a()
                b = await action_b(a)
            else:
                c = await action_c()

        Becomes IR equivalent of:
            fn __if_then_1__():
                a = @action_a()
                b = @action_b(a=a)
                return b

            if condition:
                __if_then_1__()
            else:
                @action_c()
        """
        stmt = ir.Statement(span=_make_span(node))

        # Build if branch
        condition = _expr_to_ir(node.test)
        if not condition:
            return None

        body_stmts: List[ir.Statement] = []
        for body_node in node.body:
            stmts = self._visit_statement(body_node)
            body_stmts.extend(stmts)

        # Wrap if branch body if multiple calls
        if self._count_calls(body_stmts) > 1:
            body_stmts = self._wrap_body_as_function(body_stmts, "if_then", node)

        if_branch = ir.IfBranch(
            condition=condition,
            body=self._stmts_to_single_call_body(body_stmts, _make_span(node)),
            span=_make_span(node),
        )

        conditional = ir.Conditional(if_branch=if_branch)

        # Handle elif/else chains
        current = node
        while current.orelse:
            if len(current.orelse) == 1 and isinstance(current.orelse[0], ast.If):
                # elif branch
                elif_node = current.orelse[0]
                elif_condition = _expr_to_ir(elif_node.test)
                if elif_condition:
                    elif_body: List[ir.Statement] = []
                    for body_node in elif_node.body:
                        stmts = self._visit_statement(body_node)
                        elif_body.extend(stmts)

                    # Wrap elif body if multiple calls
                    if self._count_calls(elif_body) > 1:
                        elif_body = self._wrap_body_as_function(elif_body, "if_elif", elif_node)

                    elif_branch = ir.ElifBranch(
                        condition=elif_condition,
                        body=self._stmts_to_single_call_body(elif_body, _make_span(elif_node)),
                        span=_make_span(elif_node),
                    )
                    conditional.elif_branches.append(elif_branch)
                current = elif_node
            else:
                # else branch
                else_body: List[ir.Statement] = []
                for else_node in current.orelse:
                    stmts = self._visit_statement(else_node)
                    else_body.extend(stmts)

                # Wrap else body if multiple calls
                if self._count_calls(else_body) > 1:
                    else_body = self._wrap_body_as_function(else_body, "if_else", current.orelse[0])

                else_branch = ir.ElseBranch(
                    body=self._stmts_to_single_call_body(
                        else_body, _make_span(current.orelse[0]) if current.orelse else ir.Span()
                    ),
                    span=_make_span(current.orelse[0]) if current.orelse else None,
                )
                conditional.else_branch.CopyFrom(else_branch)
                break

        stmt.conditional.CopyFrom(conditional)
        return stmt

    def _visit_try(self, node: ast.Try) -> List[ir.Statement]:
        """Convert try/except to IR with body wrapping transformation.

        If the try body has multiple action calls, we wrap the entire body
        into a synthetic function, preserving exact semantics.

        Python:
            try:
                a = await setup_action()
                b = await risky_action(a)
                return f"success:{b}"
            except SomeError:
                ...

        Becomes IR equivalent of:
            fn __try_body_1__():
                a = @setup_action()
                b = @risky_action(a=a)
                return f"success:{b}"

            try:
                __try_body_1__()
            except SomeError:
                ...
        """
        # Build try body statements (recursively transforms nested structures)
        try_body: List[ir.Statement] = []
        for body_node in node.body:
            stmts = self._visit_statement(body_node)
            try_body.extend(stmts)

        # Count action calls and function calls (synthetic functions count too)
        call_count = self._count_calls(try_body)

        # If multiple calls, wrap into synthetic function
        if call_count > 1:
            try_body = self._wrap_body_as_function(try_body, "try_body", node)

        # Build exception handlers (with wrapping if needed)
        handlers: List[ir.ExceptHandler] = []
        for handler in node.handlers:
            exception_types: List[str] = []
            if handler.type:
                if isinstance(handler.type, ast.Name):
                    exception_types.append(handler.type.id)
                elif isinstance(handler.type, ast.Tuple):
                    for elt in handler.type.elts:
                        if isinstance(elt, ast.Name):
                            exception_types.append(elt.id)

            # Build handler body (recursively transforms nested structures)
            handler_body: List[ir.Statement] = []
            for handler_node in handler.body:
                stmts = self._visit_statement(handler_node)
                handler_body.extend(stmts)

            # Wrap handler if multiple calls
            handler_call_count = self._count_calls(handler_body)
            if handler_call_count > 1:
                handler_body = self._wrap_body_as_function(handler_body, "except_handler", node)

            except_handler = ir.ExceptHandler(
                exception_types=exception_types,
                body=self._stmts_to_single_call_body(handler_body, _make_span(handler)),
                span=_make_span(handler),
            )
            handlers.append(except_handler)

        # Build the try/except statement
        try_stmt = ir.Statement(span=_make_span(node))
        try_except = ir.TryExcept(
            try_body=self._stmts_to_single_call_body(try_body, _make_span(node)),
            handlers=handlers,
        )
        try_stmt.try_except.CopyFrom(try_except)

        return [try_stmt]

    def _count_calls(self, stmts: List[ir.Statement]) -> int:
        """Count action calls and function calls in statements.

        Both action calls and function calls (including synthetic functions)
        count toward the limit of one call per control flow body.
        """
        count = 0
        for stmt in stmts:
            if stmt.HasField("action_call"):
                count += 1
            elif stmt.HasField("assignment"):
                # Check if assignment value is an action call or function call
                if stmt.assignment.value.HasField("action_call"):
                    count += 1
                elif stmt.assignment.value.HasField("function_call"):
                    count += 1
            elif stmt.HasField("expr_stmt"):
                # Check if expression is a function call
                if stmt.expr_stmt.expr.HasField("function_call"):
                    count += 1
        return count

    def _stmts_to_single_call_body(
        self, stmts: List[ir.Statement], span: ir.Span
    ) -> ir.SingleCallBody:
        """Convert statements to SingleCallBody.

        Can contain EITHER:
        1. A single action or function call (with optional target)
        2. Pure data statements (no calls)
        """
        body = ir.SingleCallBody(span=span)

        # Look for a single call in the statements
        for stmt in stmts:
            if stmt.HasField("action_call"):
                # ActionCall as a statement has no target (side-effect only)
                action = stmt.action_call
                call = ir.Call()
                call.action.CopyFrom(action)
                body.call.CopyFrom(call)
                return body
            elif stmt.HasField("assignment"):
                # Check if assignment value is an action call or function call
                if stmt.assignment.value.HasField("action_call"):
                    action = stmt.assignment.value.action_call
                    # Copy all targets for tuple unpacking support
                    body.targets.extend(stmt.assignment.targets)
                    call = ir.Call()
                    call.action.CopyFrom(action)
                    body.call.CopyFrom(call)
                    return body
                elif stmt.assignment.value.HasField("function_call"):
                    fn_call = stmt.assignment.value.function_call
                    # Copy all targets for tuple unpacking support
                    body.targets.extend(stmt.assignment.targets)
                    call = ir.Call()
                    call.function.CopyFrom(fn_call)
                    body.call.CopyFrom(call)
                    return body
            elif stmt.HasField("expr_stmt") and stmt.expr_stmt.expr.HasField("function_call"):
                fn_call = stmt.expr_stmt.expr.function_call
                call = ir.Call()
                call.function.CopyFrom(fn_call)
                body.call.CopyFrom(call)
                return body

        # No call found - this is a pure data body
        # Add all statements as pure data
        body.statements.extend(stmts)
        return body

    def _wrap_body_as_function(
        self,
        body: List[ir.Statement],
        prefix: str,
        node: ast.AST,
        inputs: Optional[List[str]] = None,
    ) -> List[ir.Statement]:
        """Wrap a body with multiple calls into a synthetic function.

        Returns a list containing a single function call statement.
        """
        fn_name = self._ctx.next_implicit_fn_name(prefix)
        fn_inputs = inputs or []

        # Create the synthetic function
        implicit_fn = ir.FunctionDef(
            name=fn_name,
            io=ir.IoDecl(inputs=fn_inputs, outputs=[]),
            body=ir.Block(statements=body),
            span=_make_span(node),
        )
        self._ctx.implicit_functions.append(implicit_fn)

        # Create a function call statement
        kwargs = [
            ir.Kwarg(name=var, value=ir.Expr(variable=ir.Variable(name=var))) for var in fn_inputs
        ]
        fn_call_expr = ir.Expr(
            function_call=ir.FunctionCall(name=fn_name, kwargs=kwargs),
            span=_make_span(node),
        )
        call_stmt = ir.Statement(span=_make_span(node))
        call_stmt.expr_stmt.CopyFrom(ir.ExprStmt(expr=fn_call_expr))

        return [call_stmt]

    def _visit_return(self, node: ast.Return) -> Optional[ir.Statement]:
        """Convert return statement to IR."""
        stmt = ir.Statement(span=_make_span(node))

        if node.value:
            # Check if returning an action call
            action_call = self._extract_action_call(node.value)
            if action_call:
                # Return with action call
                return_stmt = ir.ReturnStmt()
                # Action calls in returns need special handling
                expr = _expr_to_ir(node.value)
                if expr:
                    return_stmt.value.CopyFrom(expr)
                stmt.return_stmt.CopyFrom(return_stmt)
                return stmt

            expr = _expr_to_ir(node.value)
            if expr:
                return_stmt = ir.ReturnStmt(value=expr)
                stmt.return_stmt.CopyFrom(return_stmt)
        else:
            stmt.return_stmt.CopyFrom(ir.ReturnStmt())

        return stmt

    def _visit_aug_assign(self, node: ast.AugAssign) -> Optional[ir.Statement]:
        """Convert augmented assignment (+=, -=, etc.) to IR."""
        # For now, we can represent this as a regular assignment with binary op
        # target op= value  ->  target = target op value
        stmt = ir.Statement(span=_make_span(node))

        targets: List[str] = []
        if isinstance(node.target, ast.Name):
            targets.append(node.target.id)

        left = _expr_to_ir(node.target)
        right = _expr_to_ir(node.value)
        if left and right:
            op = _bin_op_to_ir(node.op)
            if op:
                binary = ir.BinaryOp(left=left, op=op, right=right)
                value = ir.Expr(binary_op=binary)
                assign = ir.Assignment(targets=targets, value=value)
                stmt.assignment.CopyFrom(assign)
                return stmt

        return None

    def _extract_action_call(self, node: ast.expr) -> Optional[ir.ActionCall]:
        """Extract an action call from an expression if present."""
        if not isinstance(node, ast.Await):
            return None

        awaited = node.value
        # Handle self.run_action(...) wrapper
        if isinstance(awaited, ast.Call):
            if self._is_run_action_call(awaited):
                # Extract the actual action call from run_action
                if awaited.args:
                    action_call = self._extract_action_call_from_awaitable(awaited.args[0])
                    if action_call:
                        # Extract policies from run_action kwargs (retry, timeout)
                        self._extract_policies_from_run_action(awaited, action_call)
                    return action_call
            # Check for asyncio.sleep() - convert to @sleep action
            if self._is_asyncio_sleep_call(awaited):
                return self._convert_asyncio_sleep_to_action(awaited)
            # Direct action call
            return self._extract_action_call_from_call(awaited)

        return None

    def _is_run_action_call(self, node: ast.Call) -> bool:
        """Check if this is a self.run_action(...) call."""
        if isinstance(node.func, ast.Attribute):
            return node.func.attr == "run_action"
        return False

    def _extract_policies_from_run_action(
        self, run_action_call: ast.Call, action_call: ir.ActionCall
    ) -> None:
        """Extract retry and timeout policies from run_action kwargs.

        Parses patterns like:
        - self.run_action(action(), retry=RetryPolicy(attempts=3))
        - self.run_action(action(), timeout=timedelta(seconds=30))
        - self.run_action(action(), timeout=60)
        """
        for kw in run_action_call.keywords:
            if kw.arg == "retry":
                retry_policy = self._parse_retry_policy(kw.value)
                if retry_policy:
                    policy_bracket = ir.PolicyBracket()
                    policy_bracket.retry.CopyFrom(retry_policy)
                    action_call.policies.append(policy_bracket)
            elif kw.arg == "timeout":
                timeout_policy = self._parse_timeout_policy(kw.value)
                if timeout_policy:
                    policy_bracket = ir.PolicyBracket()
                    policy_bracket.timeout.CopyFrom(timeout_policy)
                    action_call.policies.append(policy_bracket)

    def _parse_retry_policy(self, node: ast.expr) -> Optional[ir.RetryPolicy]:
        """Parse a RetryPolicy(...) call into IR.

        Supports:
        - RetryPolicy(attempts=3)
        - RetryPolicy(attempts=3, exception_types=["ValueError"])
        - RetryPolicy(attempts=3, backoff_seconds=5)
        """
        if not isinstance(node, ast.Call):
            return None

        # Check if it's a RetryPolicy call
        func_name = None
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            func_name = node.func.attr

        if func_name != "RetryPolicy":
            return None

        policy = ir.RetryPolicy()

        for kw in node.keywords:
            if kw.arg == "attempts" and isinstance(kw.value, ast.Constant):
                policy.max_retries = kw.value.value
            elif kw.arg == "exception_types" and isinstance(kw.value, ast.List):
                for elt in kw.value.elts:
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                        policy.exception_types.append(elt.value)
            elif kw.arg == "backoff_seconds" and isinstance(kw.value, ast.Constant):
                policy.backoff.seconds = int(kw.value.value)

        return policy

    def _parse_timeout_policy(self, node: ast.expr) -> Optional[ir.TimeoutPolicy]:
        """Parse a timeout value into IR.

        Supports:
        - timeout=60 (int seconds)
        - timeout=30.5 (float seconds)
        - timeout=timedelta(seconds=30)
        - timeout=timedelta(minutes=2)
        """
        policy = ir.TimeoutPolicy()

        # Direct numeric value (seconds)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            policy.timeout.seconds = int(node.value)
            return policy

        # timedelta(...) call
        if isinstance(node, ast.Call):
            func_name = None
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                func_name = node.func.attr

            if func_name == "timedelta":
                total_seconds = 0
                for kw in node.keywords:
                    if isinstance(kw.value, ast.Constant):
                        val = kw.value.value
                        if kw.arg == "seconds":
                            total_seconds += int(val)
                        elif kw.arg == "minutes":
                            total_seconds += int(val) * 60
                        elif kw.arg == "hours":
                            total_seconds += int(val) * 3600
                        elif kw.arg == "days":
                            total_seconds += int(val) * 86400
                policy.timeout.seconds = total_seconds
                return policy

        return None

    def _is_asyncio_sleep_call(self, node: ast.Call) -> bool:
        """Check if this is an asyncio.sleep(...) call.

        Supports both patterns:
        - import asyncio; asyncio.sleep(1)
        - from asyncio import sleep; sleep(1)
        - from asyncio import sleep as s; s(1)
        """
        if isinstance(node.func, ast.Attribute):
            # asyncio.sleep(...) pattern
            if node.func.attr == "sleep" and isinstance(node.func.value, ast.Name):
                return node.func.value.id == "asyncio"
        elif isinstance(node.func, ast.Name):
            # sleep(...) pattern - check if it's imported from asyncio
            func_name = node.func.id
            if func_name in self._imported_names:
                imported = self._imported_names[func_name]
                return imported.module == "asyncio" and imported.original_name == "sleep"
        return False

    def _convert_asyncio_sleep_to_action(self, node: ast.Call) -> ir.ActionCall:
        """Convert asyncio.sleep(duration) to @sleep(duration=X) action call.

        This creates a built-in sleep action that the scheduler handles as a
        durable sleep - stored in the DB with a future scheduled_at time.
        """
        action_call = ir.ActionCall(action_name="sleep")

        # Extract duration argument (positional or keyword)
        if node.args:
            # asyncio.sleep(1) - positional
            expr = _expr_to_ir(node.args[0])
            if expr:
                action_call.kwargs.append(ir.Kwarg(name="duration", value=expr))
        elif node.keywords:
            # asyncio.sleep(seconds=1) - keyword (less common)
            for kw in node.keywords:
                if kw.arg in ("seconds", "delay", "duration"):
                    expr = _expr_to_ir(kw.value)
                    if expr:
                        action_call.kwargs.append(ir.Kwarg(name="duration", value=expr))
                    break

        return action_call

    def _is_asyncio_gather_call(self, node: ast.Call) -> bool:
        """Check if this is an asyncio.gather(...) call.

        Supports both patterns:
        - import asyncio; asyncio.gather(a(), b())
        - from asyncio import gather; gather(a(), b())
        - from asyncio import gather as g; g(a(), b())
        """
        if isinstance(node.func, ast.Attribute):
            # asyncio.gather(...) pattern
            if node.func.attr == "gather" and isinstance(node.func.value, ast.Name):
                return node.func.value.id == "asyncio"
        elif isinstance(node.func, ast.Name):
            # gather(...) pattern - check if it's imported from asyncio
            func_name = node.func.id
            if func_name in self._imported_names:
                imported = self._imported_names[func_name]
                return imported.module == "asyncio" and imported.original_name == "gather"
        return False

    def _convert_asyncio_gather_to_parallel_expr(self, node: ast.Call) -> Optional[ir.ParallelExpr]:
        """Convert asyncio.gather(...) to ParallelExpr IR.

        Handles the static gather pattern: asyncio.gather(a(), b(), c())
        Returns a ParallelExpr that can be used in an Assignment for unpacking.

        For dynamic spread (asyncio.gather(*[...])), raises UnsupportedPatternError
        with guidance to use spread syntax.

        Args:
            node: The asyncio.gather() Call node

        Returns:
            A ParallelExpr, or None if conversion fails.
        """
        # Check for starred expressions - spread pattern
        if len(node.args) == 1 and isinstance(node.args[0], ast.Starred):
            starred = node.args[0]
            # Only list comprehensions are supported
            if isinstance(starred.value, ast.ListComp):
                # TODO: Convert to SpreadExpr when we add full spread support
                # For now, raise an error with guidance
                line = getattr(node, "lineno", None)
                col = getattr(node, "col_offset", None)
                raise UnsupportedPatternError(
                    "Spread pattern in asyncio.gather() requires spread syntax",
                    RECOMMENDATIONS["gather_variable_spread"],
                    line=line,
                    col=col,
                )
            else:
                # Spreading a variable or other expression is not supported
                line = getattr(node, "lineno", None)
                col = getattr(node, "col_offset", None)
                if isinstance(starred.value, ast.Name):
                    var_name = starred.value.id
                    raise UnsupportedPatternError(
                        f"Spreading variable '{var_name}' in asyncio.gather() is not supported",
                        RECOMMENDATIONS["gather_variable_spread"],
                        line=line,
                        col=col,
                    )
                else:
                    raise UnsupportedPatternError(
                        "Spreading non-list-comprehension expressions in asyncio.gather() is not supported",
                        RECOMMENDATIONS["gather_variable_spread"],
                        line=line,
                        col=col,
                    )

        # Standard case: gather(a(), b(), c()) -> ParallelExpr
        parallel = ir.ParallelExpr()

        # Each argument to gather() should be an action call
        for arg in node.args:
            call = self._convert_gather_arg_to_call(arg)
            if call:
                parallel.calls.append(call)

        # Only return if we have calls
        if not parallel.calls:
            return None

        return parallel

    def _convert_gather_arg_to_call(self, node: ast.expr) -> Optional[ir.Call]:
        """Convert a gather argument to an IR Call.

        Handles both action calls and regular function calls.
        """
        if not isinstance(node, ast.Call):
            return None

        # Try to extract as an action call first
        action_call = self._extract_action_call_from_call(node)
        if action_call:
            call = ir.Call()
            call.action.CopyFrom(action_call)
            return call

        # Fall back to regular function call
        func_call = self._convert_to_function_call(node)
        if func_call:
            call = ir.Call()
            call.function.CopyFrom(func_call)
            return call

        return None

    def _convert_to_function_call(self, node: ast.Call) -> Optional[ir.FunctionCall]:
        """Convert an AST Call to IR FunctionCall."""
        func_name = self._get_func_name(node.func)
        if not func_name:
            return None

        fn_call = ir.FunctionCall(name=func_name)

        # Add positional args
        for arg in node.args:
            expr = _expr_to_ir(arg)
            if expr:
                fn_call.args.append(expr)

        # Add keyword args
        for kw in node.keywords:
            if kw.arg:
                expr = _expr_to_ir(kw.value)
                if expr:
                    fn_call.kwargs.append(ir.Kwarg(name=kw.arg, value=expr))

        return fn_call

    def _get_func_name(self, node: ast.expr) -> Optional[str]:
        """Get function name from a func node."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            # Handle chained attributes like obj.method
            parts = []
            current = node
            while isinstance(current, ast.Attribute):
                parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                parts.append(current.id)
            return ".".join(reversed(parts))
        return None

    def _extract_action_call_from_awaitable(self, node: ast.expr) -> Optional[ir.ActionCall]:
        """Extract action call from an awaitable expression."""
        if isinstance(node, ast.Call):
            return self._extract_action_call_from_call(node)
        return None

    def _extract_action_call_from_call(self, node: ast.Call) -> Optional[ir.ActionCall]:
        """Extract action call info from a Call node.

        Converts positional arguments to keyword arguments using the action's
        signature introspection. This ensures all arguments are named in the IR.
        """
        action_name = self._get_action_name(node.func)
        if not action_name:
            return None

        if action_name not in self._action_defs:
            return None

        action_def = self._action_defs[action_name]
        action_call = ir.ActionCall(action_name=action_def.action_name)

        # Set the module name so the worker knows where to find the action
        if action_def.module_name:
            action_call.module_name = action_def.module_name

        # Get parameter names from signature for positional arg conversion
        param_names = list(action_def.signature.parameters.keys())

        # Convert positional args to kwargs using signature introspection
        for i, arg in enumerate(node.args):
            if i < len(param_names):
                expr = _expr_to_ir(arg)
                if expr:
                    kwarg = ir.Kwarg(name=param_names[i], value=expr)
                    action_call.kwargs.append(kwarg)

        # Add explicit kwargs
        for kw in node.keywords:
            if kw.arg:
                expr = _expr_to_ir(kw.value)
                if expr:
                    kwarg = ir.Kwarg(name=kw.arg, value=expr)
                    action_call.kwargs.append(kwarg)

        return action_call

    def _get_action_name(self, func: ast.expr) -> Optional[str]:
        """Get the action name from a function expression."""
        if isinstance(func, ast.Name):
            return func.id
        elif isinstance(func, ast.Attribute):
            return func.attr
        return None

    def _get_assign_target(self, targets: List[ast.expr]) -> Optional[str]:
        """Get the target variable name from assignment targets (single target only)."""
        if targets and isinstance(targets[0], ast.Name):
            return targets[0].id
        return None

    def _get_assign_targets(self, targets: List[ast.expr]) -> List[str]:
        """Get all target variable names from assignment targets (including tuple unpacking)."""
        result: List[str] = []
        for t in targets:
            if isinstance(t, ast.Name):
                result.append(t.id)
            elif isinstance(t, ast.Tuple):
                for elt in t.elts:
                    if isinstance(elt, ast.Name):
                        result.append(elt.id)
        return result


def _make_span(node: ast.AST) -> ir.Span:
    """Create a Span from an AST node."""
    return ir.Span(
        start_line=getattr(node, "lineno", 0),
        start_col=getattr(node, "col_offset", 0),
        end_line=getattr(node, "end_lineno", 0) or 0,
        end_col=getattr(node, "end_col_offset", 0) or 0,
    )


def _expr_to_ir(expr: ast.AST) -> Optional[ir.Expr]:
    """Convert Python AST expression to IR Expr."""
    result = ir.Expr(span=_make_span(expr))

    if isinstance(expr, ast.Name):
        result.variable.CopyFrom(ir.Variable(name=expr.id))
        return result

    if isinstance(expr, ast.Constant):
        literal = _constant_to_literal(expr.value)
        if literal:
            result.literal.CopyFrom(literal)
            return result

    if isinstance(expr, ast.BinOp):
        left = _expr_to_ir(expr.left)
        right = _expr_to_ir(expr.right)
        op = _bin_op_to_ir(expr.op)
        if left and right and op:
            result.binary_op.CopyFrom(ir.BinaryOp(left=left, op=op, right=right))
            return result

    if isinstance(expr, ast.UnaryOp):
        operand = _expr_to_ir(expr.operand)
        op = _unary_op_to_ir(expr.op)
        if operand and op:
            result.unary_op.CopyFrom(ir.UnaryOp(op=op, operand=operand))
            return result

    if isinstance(expr, ast.Compare):
        left = _expr_to_ir(expr.left)
        if not left:
            return None
        # For simplicity, handle single comparison
        if expr.ops and expr.comparators:
            op = _cmp_op_to_ir(expr.ops[0])
            right = _expr_to_ir(expr.comparators[0])
            if op and right:
                result.binary_op.CopyFrom(ir.BinaryOp(left=left, op=op, right=right))
                return result

    if isinstance(expr, ast.BoolOp):
        values = [_expr_to_ir(v) for v in expr.values]
        if all(v for v in values):
            op = _bool_op_to_ir(expr.op)
            if op and len(values) >= 2:
                # Chain boolean ops: a and b and c -> (a and b) and c
                result_expr = values[0]
                for v in values[1:]:
                    if result_expr and v:
                        new_result = ir.Expr()
                        new_result.binary_op.CopyFrom(ir.BinaryOp(left=result_expr, op=op, right=v))
                        result_expr = new_result
                return result_expr

    if isinstance(expr, ast.List):
        elements = [_expr_to_ir(e) for e in expr.elts]
        if all(e for e in elements):
            list_expr = ir.ListExpr(elements=[e for e in elements if e])
            result.list.CopyFrom(list_expr)
            return result

    if isinstance(expr, ast.Dict):
        entries: List[ir.DictEntry] = []
        for k, v in zip(expr.keys, expr.values, strict=False):
            if k:
                key_expr = _expr_to_ir(k)
                value_expr = _expr_to_ir(v)
                if key_expr and value_expr:
                    entries.append(ir.DictEntry(key=key_expr, value=value_expr))
        result.dict.CopyFrom(ir.DictExpr(entries=entries))
        return result

    if isinstance(expr, ast.Subscript):
        obj = _expr_to_ir(expr.value)
        index = _expr_to_ir(expr.slice) if isinstance(expr.slice, ast.AST) else None
        if obj and index:
            result.index.CopyFrom(ir.IndexAccess(object=obj, index=index))
            return result

    if isinstance(expr, ast.Attribute):
        obj = _expr_to_ir(expr.value)
        if obj:
            result.dot.CopyFrom(ir.DotAccess(object=obj, attribute=expr.attr))
            return result

    if isinstance(expr, ast.Call):
        # Function call
        func_name = _get_func_name(expr.func)
        if func_name:
            args = [_expr_to_ir(a) for a in expr.args]
            kwargs: List[ir.Kwarg] = []
            for kw in expr.keywords:
                if kw.arg:
                    kw_expr = _expr_to_ir(kw.value)
                    if kw_expr:
                        kwargs.append(ir.Kwarg(name=kw.arg, value=kw_expr))
            func_call = ir.FunctionCall(
                name=func_name,
                args=[a for a in args if a],
                kwargs=kwargs,
            )
            result.function_call.CopyFrom(func_call)
            return result

    if isinstance(expr, ast.Tuple):
        # Handle tuple as list for now
        elements = [_expr_to_ir(e) for e in expr.elts]
        if all(e for e in elements):
            list_expr = ir.ListExpr(elements=[e for e in elements if e])
            result.list.CopyFrom(list_expr)
            return result

    # Check for unsupported expression types
    _check_unsupported_expression(expr)

    return None


def _check_unsupported_expression(expr: ast.AST) -> None:
    """Check for unsupported expression types and raise descriptive errors."""
    line = getattr(expr, "lineno", None)
    col = getattr(expr, "col_offset", None)

    if isinstance(expr, ast.JoinedStr):
        raise UnsupportedPatternError(
            "F-strings are not supported",
            RECOMMENDATIONS["fstring"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.Lambda):
        raise UnsupportedPatternError(
            "Lambda expressions are not supported",
            RECOMMENDATIONS["lambda"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.ListComp):
        raise UnsupportedPatternError(
            "List comprehensions are not supported in this context",
            RECOMMENDATIONS["list_comprehension"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.DictComp):
        raise UnsupportedPatternError(
            "Dict comprehensions are not supported",
            RECOMMENDATIONS["dict_comprehension"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.SetComp):
        raise UnsupportedPatternError(
            "Set comprehensions are not supported",
            RECOMMENDATIONS["set_comprehension"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.GeneratorExp):
        raise UnsupportedPatternError(
            "Generator expressions are not supported",
            RECOMMENDATIONS["generator"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.NamedExpr):
        raise UnsupportedPatternError(
            "The walrus operator (:=) is not supported",
            RECOMMENDATIONS["walrus"],
            line=line,
            col=col,
        )
    elif isinstance(expr, ast.Yield) or isinstance(expr, ast.YieldFrom):
        raise UnsupportedPatternError(
            "Yield expressions are not supported",
            RECOMMENDATIONS["yield_statement"],
            line=line,
            col=col,
        )


def _constant_to_literal(value: Any) -> Optional[ir.Literal]:
    """Convert a Python constant to IR Literal."""
    literal = ir.Literal()
    if value is None:
        literal.is_none = True
    elif isinstance(value, bool):
        literal.bool_value = value
    elif isinstance(value, int):
        literal.int_value = value
    elif isinstance(value, float):
        literal.float_value = value
    elif isinstance(value, str):
        literal.string_value = value
    else:
        return None
    return literal


def _bin_op_to_ir(op: ast.operator) -> Optional[ir.BinaryOperator]:
    """Convert Python binary operator to IR BinaryOperator."""
    mapping = {
        ast.Add: ir.BinaryOperator.BINARY_OP_ADD,
        ast.Sub: ir.BinaryOperator.BINARY_OP_SUB,
        ast.Mult: ir.BinaryOperator.BINARY_OP_MUL,
        ast.Div: ir.BinaryOperator.BINARY_OP_DIV,
        ast.FloorDiv: ir.BinaryOperator.BINARY_OP_FLOOR_DIV,
        ast.Mod: ir.BinaryOperator.BINARY_OP_MOD,
    }
    return mapping.get(type(op))


def _unary_op_to_ir(op: ast.unaryop) -> Optional[ir.UnaryOperator]:
    """Convert Python unary operator to IR UnaryOperator."""
    mapping = {
        ast.USub: ir.UnaryOperator.UNARY_OP_NEG,
        ast.Not: ir.UnaryOperator.UNARY_OP_NOT,
    }
    return mapping.get(type(op))


def _cmp_op_to_ir(op: ast.cmpop) -> Optional[ir.BinaryOperator]:
    """Convert Python comparison operator to IR BinaryOperator."""
    mapping = {
        ast.Eq: ir.BinaryOperator.BINARY_OP_EQ,
        ast.NotEq: ir.BinaryOperator.BINARY_OP_NE,
        ast.Lt: ir.BinaryOperator.BINARY_OP_LT,
        ast.LtE: ir.BinaryOperator.BINARY_OP_LE,
        ast.Gt: ir.BinaryOperator.BINARY_OP_GT,
        ast.GtE: ir.BinaryOperator.BINARY_OP_GE,
        ast.In: ir.BinaryOperator.BINARY_OP_IN,
        ast.NotIn: ir.BinaryOperator.BINARY_OP_NOT_IN,
    }
    return mapping.get(type(op))


def _bool_op_to_ir(op: ast.boolop) -> Optional[ir.BinaryOperator]:
    """Convert Python boolean operator to IR BinaryOperator."""
    mapping = {
        ast.And: ir.BinaryOperator.BINARY_OP_AND,
        ast.Or: ir.BinaryOperator.BINARY_OP_OR,
    }
    return mapping.get(type(op))


def _get_func_name(func: ast.expr) -> Optional[str]:
    """Get function name from a Call's func attribute."""
    if isinstance(func, ast.Name):
        return func.id
    elif isinstance(func, ast.Attribute):
        # For method calls like obj.method, return full dotted name
        parts = []
        current = func
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            parts.append(current.id)
        return ".".join(reversed(parts))
    return None
