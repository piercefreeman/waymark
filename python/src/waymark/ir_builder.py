"""
IR Builder - Converts Python workflow AST to Waymark IR (ast.proto).

This module parses Python workflow classes and produces the IR representation
that can be sent to the Rust runtime for execution.

The IR builder performs deep transformations to convert Python patterns into
valid Waymark IR structures. Control flow bodies (try, for, while, if branches)
are represented as full blocks, so they can contain arbitrary statements and
`return` behaves consistently with Python (returns from the entry function end
the workflow).

Validation:
The IR builder proactively detects unsupported Python patterns and raises
UnsupportedPatternError with clear recommendations for how to rewrite the code.
"""

from __future__ import annotations

import ast
import builtins
import copy
import inspect
import textwrap
from dataclasses import dataclass, field
from enum import EnumMeta
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, NoReturn, Optional, Set

from waymark.call_binding import bind_call
from waymark.compiler_analysis import SourceAnalysis
from waymark.compiler_ast import make_span, rewrite_load_names
from waymark.compiler_context import CompilerContext
from waymark.compiler_errors import UnsupportedPatternError
from waymark.compiler_literals import constant_to_literal, literal_expression
from waymark.compiler_operators import binary_operator
from waymark.compiler_recommendations import RECOMMENDATIONS
from waymark.compiler_targets import assignment_targets
from waymark.compiler_types import has_open_type_parameters
from waymark.compiler_validation import SourceValidator
from waymark.global_handlers import GLOBAL_HANDLERS
from waymark.proto import ast_pb2 as ir
from waymark.syntax_handlers import SYNTAX_HANDLERS

if TYPE_CHECKING:
    from waymark.workflow import Workflow


@dataclass
class TransformContext:
    """Context for IR transformations."""

    # Counter for generating unique function names
    implicit_fn_counter: int = 0
    # Implicit functions generated during transformation
    implicit_functions: List[ir.FunctionDef] = field(default_factory=list)
    # Function signatures for workflow methods (used to fill in default arguments)
    function_signatures: Dict[str, inspect.Signature] = field(default_factory=dict)

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

    # Build the IR with transformation context
    ctx = TransformContext()
    program = ir.Program()
    function_defs: Dict[str, ir.FunctionDef] = {}

    # Extract instance attributes from __init__ for policy resolution
    instance_attrs = _extract_instance_attrs(workflow_cls)

    def parse_function(fn: Any) -> tuple[ast.AST, Optional[str], int]:
        source_lines, start_line = inspect.getsourcelines(fn)
        function_source = textwrap.dedent("".join(source_lines))
        filename = inspect.getsourcefile(fn)
        if filename is None:
            filename = inspect.getfile(fn)
        tree = ast.parse(function_source, filename=filename or "<unknown>")
        parsed_lines = function_source.splitlines(keepends=True)
        # Parsing removes method indentation; diagnostics still point into the original file.
        for node in ast.walk(tree):
            if isinstance(node, (ast.stmt, ast.expr, ast.arg, ast.keyword, ast.ExceptHandler)):
                node.col_offset += len(source_lines[node.lineno - 1]) - len(
                    parsed_lines[node.lineno - 1]
                )
                if node.end_lineno is not None and node.end_col_offset is not None:
                    node.end_col_offset += len(source_lines[node.end_lineno - 1]) - len(
                        parsed_lines[node.end_lineno - 1]
                    )
        return tree, filename, start_line

    def _with_source_location(
        err: UnsupportedPatternError,
        filename: Optional[str],
        start_line: int,
    ) -> UnsupportedPatternError:
        line = err.line
        col = err.col
        if line is not None:
            line = start_line + line - 1
        if col is not None:
            col = col + 1
        return UnsupportedPatternError(
            err.message,
            err.recommendation,
            line=line,
            col=col,
            filename=filename,
        )

    def add_function_def(
        fn: Any,
        fn_tree: ast.AST,
        filename: Optional[str],
        start_line: int,
        override_name: Optional[str] = None,
    ) -> None:
        fn_module = inspect.getmodule(fn)
        if fn_module is None:
            raise ValueError(f"unable to locate module for function {fn!r}")

        builder = IRBuilder(
            ctx,
            {**fn_module.__dict__, **inspect.getclosurevars(fn).nonlocals},
            instance_attrs,
            function_locals=fn.__code__.co_varnames + fn.__code__.co_cellvars,
            receiver_type=workflow_cls,
        )
        try:
            builder.visit(fn_tree)
        except UnsupportedPatternError as err:
            raise _with_source_location(err, filename, start_line) from err
        if builder.function_def:
            if override_name:
                builder.function_def.name = override_name
            function_defs[builder.function_def.name] = builder.function_def

    # Discover all reachable helper methods first so we can pre-collect their signatures.
    # This is needed because when we process a function that calls another function,
    # we need to know the callee's signature to fill in default arguments.
    run_tree, run_filename, run_start_line = parse_function(original_run)

    # Collect all reachable methods and their trees
    methods_to_process: List[tuple[Any, ast.AST, Optional[str], int, Optional[str]]] = [
        (original_run, run_tree, run_filename, run_start_line, "main")
    ]
    pending = _collect_self_method_calls(run_tree)
    pending_index = 0
    visited: Set[str] = set()

    while pending_index < len(pending):
        method_name = pending[pending_index]
        pending_index += 1
        if method_name in visited or method_name == "run":
            continue
        if (
            GLOBAL_HANDLERS.resolve(
                ast.Attribute(value=ast.Name(id="self"), attr=method_name), {"self": workflow_cls}
            )
            is not None
        ):
            continue
        visited.add(method_name)

        method = _find_workflow_method(workflow_cls, method_name)
        if method is None:
            continue

        method_tree, method_filename, method_start_line = parse_function(method)
        methods_to_process.append((method, method_tree, method_filename, method_start_line, None))
        pending.extend(_collect_self_method_calls(method_tree))

    # Pre-collect signatures for all methods before processing IR.
    # This ensures that when we encounter a function call, we can fill in default args.
    for fn, _fn_tree, _filename, _start_line, override_name in methods_to_process:
        fn_name = override_name if override_name else fn.__name__
        sig = inspect.signature(fn)
        ctx.function_signatures[fn_name] = sig.replace(
            parameters=[parameter for name, parameter in sig.parameters.items() if name != "self"]
        )

    # Now process all functions with signatures already available
    for fn, fn_tree, filename, start_line, override_name in methods_to_process:
        add_function_def(fn, fn_tree, filename, start_line, override_name)

    # Add implicit functions first (they may be called by the main function)
    for implicit_fn in ctx.implicit_functions:
        program.functions.append(implicit_fn)

    # Add all function definitions (run + reachable helper methods)
    for fn_def in function_defs.values():
        program.functions.append(fn_def)

    return program


def _collect_self_method_calls(tree: ast.AST) -> List[str]:
    """Collect self.method(...) call names from a parsed function AST in source order."""

    class SelfMethodCallCollector(ast.NodeVisitor):
        def __init__(self) -> None:
            self.calls: List[str] = []
            self._seen: Set[str] = set()

        def visit_Call(self, node: ast.Call) -> None:
            func = node.func
            if (
                isinstance(func, ast.Attribute)
                and isinstance(func.value, ast.Name)
                and func.value.id == "self"
                and func.attr not in self._seen
            ):
                self._seen.add(func.attr)
                self.calls.append(func.attr)

            self.generic_visit(node)

    collector = SelfMethodCallCollector()
    collector.visit(tree)
    return collector.calls


def _find_workflow_method(workflow_cls: type["Workflow"], name: str) -> Optional[Any]:
    """Find a workflow method by name across the class MRO."""
    for base in workflow_cls.__mro__:
        if name not in base.__dict__:
            continue
        value = base.__dict__[name]
        if isinstance(value, staticmethod) or isinstance(value, classmethod):
            return value.__func__
        if inspect.isfunction(value):
            return value
    return None


@dataclass(frozen=True)
class InstanceAttrs:
    """What `__init__` assigns to `self`, as far as the compiler can read it."""

    literals: Dict[str, ast.expr]
    """Attributes assigned exactly once, at the top level of `__init__`, with the value."""

    ambiguous: Set[str]
    """Attributes assigned more than once, or under a branch or loop: unreadable."""


def _extract_instance_attrs(workflow_cls: type["Workflow"]) -> InstanceAttrs:
    """Read the `self.attr = value` assignments of the workflow's `__init__`.

    Only an attribute assigned exactly once, at the top level of the method,
    has a value the compiler can read, such as:
        self.retry_policy = RetryPolicy(attempts=3)
        self.timeout = 30

    One assigned twice, or inside an `if`, a loop or a `try`, is recorded as
    ambiguous: the source order would otherwise decide which value wins, so
    a policy that names it is rejected instead.
    """
    init_method = _find_workflow_method(workflow_cls, "__init__")
    if init_method is None:
        return InstanceAttrs({}, set())

    try:
        source_lines, _ = inspect.getsourcelines(init_method)
        source = textwrap.dedent("".join(source_lines))
        tree = ast.parse(source)
    except (OSError, TypeError, SyntaxError):
        return InstanceAttrs({}, set())

    function_def = tree.body[0]
    if not isinstance(function_def, ast.FunctionDef):
        return InstanceAttrs({}, set())

    literals: Dict[str, ast.expr] = {}
    ambiguous: Set[str] = set()
    for statement in function_def.body:
        top_level = _self_attribute_assignment(statement)
        if top_level is not None:
            name, value = top_level
            if name in literals or name in ambiguous:
                literals.pop(name, None)
                ambiguous.add(name)
            else:
                literals[name] = value
            continue
        for nested in ast.walk(statement):
            nested_assignment = _self_attribute_assignment(nested)
            if nested_assignment is not None:
                literals.pop(nested_assignment[0], None)
                ambiguous.add(nested_assignment[0])

    return InstanceAttrs(literals, ambiguous)


def _self_attribute_assignment(node: ast.AST) -> Optional[tuple[str, ast.expr]]:
    """The name and value of a `self.name = value` assignment, annotated or not; else None."""
    if isinstance(node, ast.Assign) and len(node.targets) == 1:
        target, value = node.targets[0], node.value
    elif isinstance(node, ast.AnnAssign) and node.value is not None:
        target, value = node.target, node.value
    else:
        return None
    if (
        isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id == "self"
    ):
        return target.attr, value
    return None


class IRBuilder(ast.NodeVisitor):
    """Builds IR from Python AST with deep transformations."""

    def __init__(
        self,
        ctx: TransformContext,
        module_globals: Optional[Mapping[str, Any]] = None,
        instance_attrs: Optional[InstanceAttrs] = None,
        function_locals: tuple[str, ...] = (),
        receiver_type: type | None = None,
    ):
        self._ctx = ctx
        self._module_globals = module_globals or {}
        self._language_call_namespace = {
            name: value
            for name, value in {**vars(builtins), **self._module_globals}.items()
            if name not in function_locals
        }
        if receiver_type is not None:
            self._language_call_namespace["self"] = receiver_type
        self._instance_attrs = instance_attrs.literals if instance_attrs else {}
        self._ambiguous_instance_attrs = instance_attrs.ambiguous if instance_attrs else set()
        self._language_calls = GLOBAL_HANDLERS
        self._compiler_context = CompilerContext(
            expression=self._compile_expression,
            statement=self._visit_statement,
            action=self._extract_action_call,
            function=self._convert_to_function_call,
            next_name=self._ctx.next_implicit_fn_name,
            function_names=self._ctx.function_signatures.keys(),
            resolve=lambda function: self._language_calls.resolve(
                function, self._language_call_namespace
            ),
            namespace=self._language_call_namespace,
            synchronous_call=self._convert_registered_call,
            validate_syntax=lambda node: SourceValidator(
                self._compiler_context, SYNTAX_HANDLERS
            ).visit(node),
            instance_attrs=self._instance_attrs,
            ambiguous_instance_attrs=self._ambiguous_instance_attrs,
        )
        self.function_def: Optional[ir.FunctionDef] = None
        self._statements: List[ir.Statement] = []

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> Any:
        """Visit a function definition (the workflow's run method)."""
        annotations = [
            argument.annotation
            for argument in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs)
            if argument.annotation is not None
        ]
        if node.returns is not None:
            annotations.append(node.returns)
        for annotation in annotations:
            if has_open_type_parameters(ast.unparse(annotation), self._language_call_namespace):
                raise UnsupportedPatternError(
                    "Open generic annotations are unsupported",
                    "Specialize all type parameters before compiling the workflow.",
                    line=annotation.lineno,
                    col=annotation.col_offset,
                )
        analysis = SourceAnalysis()
        analysis.analyze(node)
        self._compiler_context.analysis = analysis
        self._language_call_namespace.update(
            {name: list for name in analysis.types if analysis.types.get(name) == "list"}
        )
        SourceValidator(self._compiler_context, SYNTAX_HANDLERS).validate(node)
        inputs = self._collect_function_inputs(node)

        # Create the function definition
        self.function_def = ir.FunctionDef(
            name=node.name,
            io=ir.IoDecl(inputs=inputs, outputs=[]),
            span=make_span(node),
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
        self.visit_FunctionDef(node)

    def _visit_statement(self, node: ast.stmt) -> List[ir.Statement]:
        """Convert a Python statement to IR Statement(s).

        Returns a list because some transformations (like try block hoisting)
        may expand a single Python statement into multiple IR statements.

        Raises UnsupportedPatternError for unsupported statement types.
        """
        language_call = self._visit_language_call(node)
        if language_call is not None:
            return language_call
        lowered = SYNTAX_HANDLERS.lower_statement(node, self._compiler_context)
        if lowered is not None:
            return lowered
        if isinstance(node, ast.Assign):
            expanded = self._expand_list_comprehension_assignment(node)
            if expanded is not None:
                return expanded
            result = self._visit_assign(node)
            return [result] if result else []
        elif isinstance(node, ast.AnnAssign):
            result = self._visit_ann_assign(node)
            return [result] if result else []
        elif isinstance(node, ast.Expr):
            result = self._visit_expr_stmt(node)
            return [result] if result else []
        elif isinstance(node, ast.If):
            return self._visit_if(node)
        elif isinstance(node, ast.Return):
            return self._visit_return(node)
        elif isinstance(node, ast.AugAssign):
            return self._visit_aug_assign(node)
        elif isinstance(node, ast.Pass):
            # Pass statements are fine, they just don't produce IR
            return []
        elif isinstance(node, ast.Break):
            return self._visit_break(node)
        elif isinstance(node, ast.Continue):
            return self._visit_continue(node)

        # Check for unsupported statement types - this MUST raise for any
        # unhandled statement to avoid silently dropping code
        self._check_unsupported_statement(node)

    def _check_unsupported_statement(self, node: ast.stmt) -> NoReturn:
        """Check for unsupported statement types and raise descriptive errors.

        This function ALWAYS raises an exception - it never returns normally.
        Any statement type that reaches this function is either explicitly
        unsupported (with a specific error message) or unhandled (with a
        generic catch-all error). This ensures we never silently drop code.
        """
        line = getattr(node, "lineno", None)
        col = getattr(node, "col_offset", None)

        if isinstance(node, (ast.With, ast.AsyncWith)):
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
        else:
            # Catch-all for any unhandled statement types.
            # This is critical to avoid silently dropping code.
            stmt_type = type(node).__name__
            raise UnsupportedPatternError(
                f"Unhandled statement type: {stmt_type}",
                RECOMMENDATIONS["unsupported_statement"],
                line=line,
                col=col,
            )

    def _expand_list_comprehension_assignment(
        self, node: ast.Assign
    ) -> Optional[List[ir.Statement]]:
        """Expand a list comprehension assignment into loop-based statements.

        Example:
            active_users = [user for user in users if user.active]

        Becomes:
            source = users
            result = []
            for item in source:
                if item.active:
                    result = result + [item]
            active_users = result
        """
        if not isinstance(node.value, ast.ListComp):
            return None

        if len(node.targets) != 1 or not isinstance(node.targets[0], ast.Name):
            line = getattr(node, "lineno", None)
            col = getattr(node, "col_offset", None)
            raise UnsupportedPatternError(
                "List comprehension assignments must target a single variable",
                "Assign the comprehension to a simple variable like `results = [x for x in items]`",
                line=line,
                col=col,
            )

        listcomp = copy.deepcopy(node.value)
        if len(listcomp.generators) != 1:
            line = getattr(listcomp, "lineno", None)
            col = getattr(listcomp, "col_offset", None)
            raise UnsupportedPatternError(
                "List comprehensions with multiple generators are not supported",
                "Use nested for loops instead of combining multiple generators in one comprehension",
                line=line,
                col=col,
            )

        gen = listcomp.generators[0]
        if gen.is_async:
            line = getattr(listcomp, "lineno", None)
            col = getattr(listcomp, "col_offset", None)
            raise UnsupportedPatternError(
                "Async list comprehensions are not supported",
                "Rewrite using an explicit async for loop",
                line=line,
                col=col,
            )

        replacements = {
            name: ast.Name(id=self._ctx.next_implicit_fn_name("comprehension_item"), ctx=ast.Load())
            for name in assignment_targets([gen.target])
        }
        for target in ast.walk(gen.target):
            if isinstance(target, ast.Name):
                replacement = replacements[target.id]
                assert isinstance(replacement, ast.Name)
                target.id = replacement.id
        rewritten_element = rewrite_load_names(listcomp.elt, replacements)
        assert isinstance(rewritten_element, ast.expr)
        listcomp.elt = rewritten_element
        rewritten_filters = [rewrite_load_names(condition, replacements) for condition in gen.ifs]
        assert all(isinstance(condition, ast.expr) for condition in rewritten_filters)
        gen.ifs = [condition for condition in rewritten_filters if isinstance(condition, ast.expr)]
        iterable_name = self._ctx.next_implicit_fn_name("comprehension_input")
        iterable_assignment = ast.fix_missing_locations(
            ast.copy_location(
                ast.Assign(targets=[ast.Name(id=iterable_name, ctx=ast.Store())], value=gen.iter),
                node,
            )
        )
        gen.iter = ast.copy_location(ast.Name(id=iterable_name, ctx=ast.Load()), gen.iter)

        target_name = self._ctx.next_implicit_fn_name("comprehension_result")

        # Publish only after the entire comprehension succeeds.
        init_assign_ast = ast.Assign(
            targets=[ast.Name(id=target_name, ctx=ast.Store())],
            value=ast.List(elts=[], ctx=ast.Load()),
            type_comment=None,
        )
        ast.copy_location(init_assign_ast, node)
        ast.fix_missing_locations(init_assign_ast)

        def _make_append_assignment(value_expr: ast.expr) -> ast.Assign:
            append_assign = ast.Assign(
                targets=[ast.Name(id=target_name, ctx=ast.Store())],
                value=ast.BinOp(
                    left=ast.Name(id=target_name, ctx=ast.Load()),
                    op=ast.Add(),
                    right=ast.List(elts=[copy.deepcopy(value_expr)], ctx=ast.Load()),
                ),
                type_comment=None,
            )
            ast.copy_location(append_assign, node.value)
            ast.fix_missing_locations(append_assign)
            return append_assign

        append_statements: List[ast.stmt] = []
        if isinstance(listcomp.elt, ast.IfExp):
            then_assign = _make_append_assignment(listcomp.elt.body)
            else_assign = _make_append_assignment(listcomp.elt.orelse)
            branch_if = ast.If(
                test=copy.deepcopy(listcomp.elt.test),
                body=[then_assign],
                orelse=[else_assign],
            )
            ast.copy_location(branch_if, listcomp.elt)
            ast.fix_missing_locations(branch_if)
            append_statements.append(branch_if)
        else:
            append_statements.append(_make_append_assignment(listcomp.elt))

        loop_body: List[ast.stmt] = append_statements
        for condition in reversed(gen.ifs):
            if_stmt = ast.If(test=copy.deepcopy(condition), body=loop_body, orelse=[])
            ast.copy_location(if_stmt, condition)
            ast.fix_missing_locations(if_stmt)
            loop_body = [if_stmt]

        loop_ast = ast.For(
            target=copy.deepcopy(gen.target),
            iter=copy.deepcopy(gen.iter),
            body=loop_body,
            orelse=[],
            type_comment=None,
        )
        ast.copy_location(loop_ast, node)
        ast.fix_missing_locations(loop_ast)

        statements = self._visit_statement(iterable_assignment)
        init_stmt = self._visit_assign(init_assign_ast)
        if init_stmt:
            statements.append(init_stmt)
        statements.extend(self._visit_statement(loop_ast))
        final_assignment = ast.fix_missing_locations(
            ast.copy_location(
                ast.Assign(
                    targets=copy.deepcopy(node.targets),
                    value=ast.Name(id=target_name, ctx=ast.Load()),
                ),
                node,
            )
        )
        statements.extend(self._visit_statement(final_assignment))

        return statements

    def _visit_language_call(self, node: ast.stmt) -> list[ir.Statement] | None:
        """Dispatch registered awaited calls; handlers own recognition and lowering."""
        match node:
            case ast.Assign(value=ast.Await(value=ast.Call() as call)):
                targets = assignment_targets(node.targets)
            case ast.AnnAssign(value=ast.Await(value=ast.Call() as call)):
                targets = assignment_targets([node.target])
            case ast.Expr(value=ast.Await(value=ast.Call() as call)):
                targets = []
            case ast.Return(value=ast.Await(value=ast.Call() as call)):
                targets = []
            case ast.Expr(value=ast.Call() as call):
                handler = self._language_calls.resolve(call.func, self._language_call_namespace)
                if handler is None or handler.requires_await:
                    return None
                lowered = handler.compile(call, self._compiler_context)
                return [
                    *lowered.statements,
                    ir.Statement(expr_stmt=ir.ExprStmt(expr=lowered.value)),
                ]
            case _:
                return None

        handler = self._language_calls.resolve(call.func, self._language_call_namespace)
        if handler is None:
            return None
        if isinstance(node, ast.Return):
            targets = [self._ctx.next_implicit_fn_name(prefix="return_tmp")]
        if not handler.requires_await:
            raise handler.error(call, "this call cannot be awaited")
        lowered = handler.compile(call, self._compiler_context)
        if (
            isinstance(node, (ast.Assign, ast.AnnAssign))
            and isinstance(
                node.targets[0] if isinstance(node, ast.Assign) else node.target,
                (ast.Tuple, ast.List),
            )
            and lowered.value.HasField("parallel_expr")
            and len(targets) != len(lowered.value.parallel_expr.calls)
        ):
            raise handler.error(call, "unpacking target count does not match result count")
        lowered.value.span.CopyFrom(make_span(call))
        statement = ir.Statement(span=make_span(node))
        if isinstance(node, ast.Expr) and lowered.value.HasField("parallel_expr"):
            statement.parallel_block.calls.extend(lowered.value.parallel_expr.calls)
        elif isinstance(node, ast.Expr) and not lowered.value.HasField("spread_expr"):
            if lowered.value.HasField("action_call"):
                statement.action_call.CopyFrom(lowered.value.action_call)
            else:
                statement.expr_stmt.CopyFrom(ir.ExprStmt(expr=lowered.value))
        else:
            statement.assignment.CopyFrom(ir.Assignment(targets=targets, value=lowered.value))
        statements = [*lowered.statements, statement]
        if isinstance(node, ast.Return):
            statements.append(
                ir.Statement(
                    return_stmt=ir.ReturnStmt(value=ir.Expr(variable=ir.Variable(name=targets[0]))),
                    span=make_span(node),
                )
            )
        return statements

    def _visit_ann_assign(self, node: ast.AnnAssign) -> Optional[ir.Statement]:
        """Convert annotated assignment to IR when a value is present."""
        if node.value is None:
            return None

        assign = ast.Assign(targets=[node.target], value=node.value, type_comment=None)
        ast.copy_location(assign, node)
        ast.fix_missing_locations(assign)
        return self._visit_assign(assign)

    def _visit_assign(self, node: ast.Assign) -> Optional[ir.Statement]:
        """Convert assignment to IR.

        All assignments with targets use the Assignment statement type.
        This provides uniform unpacking support for:
        - Action calls: a, b = @get_pair()
        - Parallel blocks: a, b = parallel: @x() @y()
        - Regular expressions: a, b = some_list

        Raises UnsupportedPatternError for:
        - Constructor calls: x = MyClass(...)
        - Non-action await: x = await some_func()
        """
        stmt = ir.Statement(span=make_span(node))
        targets = assignment_targets(node.targets)

        # Check if this is an action call - wrap in Assignment for uniform unpacking
        action_call = self._extract_action_call(node.value)
        if action_call:
            value = ir.Expr(action_call=action_call, span=make_span(node))
            assign = ir.Assignment(targets=targets, value=value)
            stmt.assignment.CopyFrom(assign)
            return stmt

        # Regular assignment (variables, literals, expressions)
        value_expr = self._compile_expression(node.value)
        if value_expr:
            assign = ir.Assignment(targets=targets, value=value_expr)
            stmt.assignment.CopyFrom(assign)
            return stmt

        return None

    def _visit_expr_stmt(self, node: ast.Expr) -> Optional[ir.Statement]:
        """Convert expression statement to IR (side effect only, no assignment)."""
        stmt = ir.Statement(span=make_span(node))

        # Check if this is an action call (side effect only)
        action_call = self._extract_action_call(node.value)
        if action_call:
            stmt.action_call.CopyFrom(action_call)
            return stmt

        # Regular expression
        expr = self._compile_expression(node.value)
        if expr:
            stmt.expr_stmt.CopyFrom(ir.ExprStmt(expr=expr))
            return stmt

        return None

    def _collect_variables_from_expr(self, expr: ir.Expr) -> set:
        """Recursively collect all variable names used in an expression."""
        vars_found: set = set()

        if expr.HasField("variable"):
            vars_found.add(expr.variable.name)
        elif expr.HasField("binary_op"):
            vars_found.update(self._collect_variables_from_expr(expr.binary_op.left))
            vars_found.update(self._collect_variables_from_expr(expr.binary_op.right))
        elif expr.HasField("unary_op"):
            vars_found.update(self._collect_variables_from_expr(expr.unary_op.operand))
        elif expr.HasField("list"):
            for elem in expr.list.elements:
                vars_found.update(self._collect_variables_from_expr(elem))
        elif expr.HasField("dict"):
            for entry in expr.dict.entries:
                vars_found.update(self._collect_variables_from_expr(entry.key))
                vars_found.update(self._collect_variables_from_expr(entry.value))
        elif expr.HasField("index"):
            vars_found.update(self._collect_variables_from_expr(expr.index.value))
            vars_found.update(self._collect_variables_from_expr(expr.index.index))
        elif expr.HasField("dot"):
            vars_found.update(self._collect_variables_from_expr(expr.dot.object))
        elif expr.HasField("function_call"):
            for kwarg in expr.function_call.kwargs:
                vars_found.update(self._collect_variables_from_expr(kwarg.value))
        elif expr.HasField("action_call"):
            for kwarg in expr.action_call.kwargs:
                vars_found.update(self._collect_variables_from_expr(kwarg.value))

        return vars_found

    def _visit_if(self, node: ast.If) -> List[ir.Statement]:
        """Convert if statement to IR.

        Normalizes patterns like:
            if await some_action(...):
                ...
        into:
            __if_cond_n__ = await some_action(...)
            if __if_cond_n__:
                ...
        """

        def normalize_condition(test: ast.expr) -> tuple[List[ir.Statement], Optional[ir.Expr]]:
            action_call = self._extract_action_call(test)
            if action_call is None:
                return ([], self._compile_expression(test))

            if not isinstance(test, ast.Await):
                line = getattr(test, "lineno", None)
                col = getattr(test, "col_offset", None)
                raise UnsupportedPatternError(
                    "Action calls inside boolean expressions are not supported in if conditions",
                    "Assign the awaited action result to a variable, then use the variable in the if condition.",
                    line=line,
                    col=col,
                )

            cond_var = self._ctx.next_implicit_fn_name(prefix="if_cond")
            assign_stmt = ir.Statement(span=make_span(test))
            assign_stmt.assignment.CopyFrom(
                ir.Assignment(
                    targets=[cond_var],
                    value=ir.Expr(action_call=action_call, span=make_span(test)),
                )
            )
            cond_expr = ir.Expr(variable=ir.Variable(name=cond_var), span=make_span(test))
            return ([assign_stmt], cond_expr)

        def visit_body(nodes: list[ast.stmt]) -> List[ir.Statement]:
            stmts: List[ir.Statement] = []
            for body_node in nodes:
                stmts.extend(self._visit_statement(body_node))
            return stmts

        # Collect if/elif branches as (test_expr, body_nodes)
        branches: list[tuple[ast.expr, list[ast.stmt], ast.AST]] = [(node.test, node.body, node)]
        current = node
        while current.orelse and len(current.orelse) == 1 and isinstance(current.orelse[0], ast.If):
            elif_node = current.orelse[0]
            branches.append((elif_node.test, elif_node.body, elif_node))
            current = elif_node

        else_nodes = current.orelse

        normalized: list[
            tuple[List[ir.Statement], Optional[ir.Expr], List[ir.Statement], ast.AST]
        ] = []
        for test_expr, body_nodes, span_node in branches:
            prefix, cond = normalize_condition(test_expr)
            normalized.append((prefix, cond, visit_body(body_nodes), span_node))

        else_body = visit_body(else_nodes) if else_nodes else []

        # If any non-first branch needs normalization, preserve Python semantics by nesting.
        requires_nested = any(prefix for prefix, _, _, _ in normalized[1:])

        def build_conditional_stmt(
            condition: ir.Expr,
            then_body: List[ir.Statement],
            else_body_statements: List[ir.Statement],
            span_node: ast.AST,
        ) -> ir.Statement:
            conditional_stmt = ir.Statement(span=make_span(span_node))
            if_branch = ir.IfBranch(
                condition=condition,
                block_body=ir.Block(statements=then_body, span=make_span(span_node)),
                span=make_span(span_node),
            )
            conditional = ir.Conditional(if_branch=if_branch)
            if else_body_statements:
                else_branch = ir.ElseBranch(
                    block_body=ir.Block(
                        statements=else_body_statements,
                        span=make_span(span_node),
                    ),
                    span=make_span(span_node),
                )
                conditional.else_branch.CopyFrom(else_branch)
            conditional_stmt.conditional.CopyFrom(conditional)
            return conditional_stmt

        if requires_nested:
            nested_else: List[ir.Statement] = else_body
            for prefix, cond, then_body, span_node in reversed(normalized):
                if cond is None:
                    continue
                nested_if_stmt = build_conditional_stmt(
                    condition=cond,
                    then_body=then_body,
                    else_body_statements=nested_else,
                    span_node=span_node,
                )
                nested_else = [*prefix, nested_if_stmt]
            return nested_else

        # Flat conditional with elif/else (original behavior), plus optional prefix for the if guard.
        if_prefix, if_condition, if_body, if_span_node = normalized[0]
        if if_condition is None:
            return []

        conditional_stmt = ir.Statement(span=make_span(if_span_node))
        if_branch = ir.IfBranch(
            condition=if_condition,
            block_body=ir.Block(statements=if_body, span=make_span(if_span_node)),
            span=make_span(if_span_node),
        )
        conditional = ir.Conditional(if_branch=if_branch)

        for _, elif_condition, elif_body, elif_span_node in normalized[1:]:
            if elif_condition is None:
                continue
            elif_branch = ir.ElifBranch(
                condition=elif_condition,
                block_body=ir.Block(statements=elif_body, span=make_span(elif_span_node)),
                span=make_span(elif_span_node),
            )
            conditional.elif_branches.append(elif_branch)

        if else_body:
            else_branch = ir.ElseBranch(
                block_body=ir.Block(statements=else_body, span=make_span(if_span_node)),
                span=make_span(if_span_node),
            )
            conditional.else_branch.CopyFrom(else_branch)

        conditional_stmt.conditional.CopyFrom(conditional)
        return [*if_prefix, conditional_stmt]

    def _collect_assigned_vars(self, stmts: List[ir.Statement]) -> set:
        """Collect all variable names assigned in a list of statements."""
        assigned = set()
        for stmt in stmts:
            if stmt.HasField("assignment"):
                assigned.update(stmt.assignment.targets)
        return assigned

    def _collect_assigned_vars_in_order(self, stmts: List[ir.Statement]) -> list[str]:
        """Collect assigned variable names in statement order (deduplicated)."""
        assigned: list[str] = []
        seen: set[str] = set()

        for stmt in stmts:
            if stmt.HasField("assignment"):
                for target in stmt.assignment.targets:
                    if target not in seen:
                        seen.add(target)
                        assigned.append(target)

            if stmt.HasField("conditional"):
                cond = stmt.conditional
                if cond.HasField("if_branch") and cond.if_branch.HasField("block_body"):
                    for target in self._collect_assigned_vars_in_order(
                        list(cond.if_branch.block_body.statements)
                    ):
                        if target not in seen:
                            seen.add(target)
                            assigned.append(target)
                for elif_branch in cond.elif_branches:
                    if elif_branch.HasField("block_body"):
                        for target in self._collect_assigned_vars_in_order(
                            list(elif_branch.block_body.statements)
                        ):
                            if target not in seen:
                                seen.add(target)
                                assigned.append(target)
                if cond.HasField("else_branch") and cond.else_branch.HasField("block_body"):
                    for target in self._collect_assigned_vars_in_order(
                        list(cond.else_branch.block_body.statements)
                    ):
                        if target not in seen:
                            seen.add(target)
                            assigned.append(target)

            if stmt.HasField("for_loop") and stmt.for_loop.HasField("block_body"):
                for target in self._collect_assigned_vars_in_order(
                    list(stmt.for_loop.block_body.statements)
                ):
                    if target not in seen:
                        seen.add(target)
                        assigned.append(target)

            if stmt.HasField("while_loop") and stmt.while_loop.HasField("block_body"):
                for target in self._collect_assigned_vars_in_order(
                    list(stmt.while_loop.block_body.statements)
                ):
                    if target not in seen:
                        seen.add(target)
                        assigned.append(target)

            if stmt.HasField("try_except"):
                try_block = stmt.try_except.try_block
                if try_block.HasField("span"):
                    for target in self._collect_assigned_vars_in_order(list(try_block.statements)):
                        if target not in seen:
                            seen.add(target)
                            assigned.append(target)
                for handler in stmt.try_except.handlers:
                    if handler.HasField("block_body"):
                        for target in self._collect_assigned_vars_in_order(
                            list(handler.block_body.statements)
                        ):
                            if target not in seen:
                                seen.add(target)
                                assigned.append(target)

        return assigned

    def _collect_variables_from_block(self, block: ir.Block) -> list[str]:
        return self._collect_variables_from_statements(list(block.statements))

    def _collect_variables_from_statements(self, stmts: List[ir.Statement]) -> list[str]:
        """Collect variable references from statements in encounter order."""
        vars_found: list[str] = []
        seen: set[str] = set()

        for stmt in stmts:
            if stmt.HasField("assignment") and stmt.assignment.HasField("value"):
                for var in self._collect_variables_from_expr(stmt.assignment.value):
                    if var not in seen:
                        seen.add(var)
                        vars_found.append(var)

            if stmt.HasField("return_stmt") and stmt.return_stmt.HasField("value"):
                for var in self._collect_variables_from_expr(stmt.return_stmt.value):
                    if var not in seen:
                        seen.add(var)
                        vars_found.append(var)

            if stmt.HasField("action_call"):
                expr = ir.Expr(action_call=stmt.action_call, span=stmt.span)
                for var in self._collect_variables_from_expr(expr):
                    if var not in seen:
                        seen.add(var)
                        vars_found.append(var)

            if stmt.HasField("expr_stmt"):
                for var in self._collect_variables_from_expr(stmt.expr_stmt.expr):
                    if var not in seen:
                        seen.add(var)
                        vars_found.append(var)

            if stmt.HasField("conditional"):
                cond = stmt.conditional
                if cond.HasField("if_branch"):
                    if cond.if_branch.HasField("condition"):
                        for var in self._collect_variables_from_expr(cond.if_branch.condition):
                            if var not in seen:
                                seen.add(var)
                                vars_found.append(var)
                    if cond.if_branch.HasField("block_body"):
                        for var in self._collect_variables_from_block(cond.if_branch.block_body):
                            if var not in seen:
                                seen.add(var)
                                vars_found.append(var)
                for elif_branch in cond.elif_branches:
                    if elif_branch.HasField("condition"):
                        for var in self._collect_variables_from_expr(elif_branch.condition):
                            if var not in seen:
                                seen.add(var)
                                vars_found.append(var)
                    if elif_branch.HasField("block_body"):
                        for var in self._collect_variables_from_block(elif_branch.block_body):
                            if var not in seen:
                                seen.add(var)
                                vars_found.append(var)
                if cond.HasField("else_branch") and cond.else_branch.HasField("block_body"):
                    for var in self._collect_variables_from_block(cond.else_branch.block_body):
                        if var not in seen:
                            seen.add(var)
                            vars_found.append(var)

            if stmt.HasField("for_loop"):
                fl = stmt.for_loop
                if fl.HasField("iterable"):
                    for var in self._collect_variables_from_expr(fl.iterable):
                        if var not in seen:
                            seen.add(var)
                            vars_found.append(var)
                if fl.HasField("block_body"):
                    for var in self._collect_variables_from_block(fl.block_body):
                        if var not in seen:
                            seen.add(var)
                            vars_found.append(var)

            if stmt.HasField("while_loop"):
                wl = stmt.while_loop
                if wl.HasField("condition"):
                    for var in self._collect_variables_from_expr(wl.condition):
                        if var not in seen:
                            seen.add(var)
                            vars_found.append(var)
                if wl.HasField("block_body"):
                    for var in self._collect_variables_from_block(wl.block_body):
                        if var not in seen:
                            seen.add(var)
                            vars_found.append(var)

            if stmt.HasField("try_except"):
                te = stmt.try_except
                if te.HasField("try_block"):
                    for var in self._collect_variables_from_block(te.try_block):
                        if var not in seen:
                            seen.add(var)
                            vars_found.append(var)
                for handler in te.handlers:
                    if handler.HasField("block_body"):
                        for var in self._collect_variables_from_block(handler.block_body):
                            if var not in seen:
                                seen.add(var)
                                vars_found.append(var)

            if stmt.HasField("parallel_block"):
                for call in stmt.parallel_block.calls:
                    if call.HasField("action"):
                        for kwarg in call.action.kwargs:
                            for var in self._collect_variables_from_expr(kwarg.value):
                                if var not in seen:
                                    seen.add(var)
                                    vars_found.append(var)
                    elif call.HasField("function"):
                        for kwarg in call.function.kwargs:
                            for var in self._collect_variables_from_expr(kwarg.value):
                                if var not in seen:
                                    seen.add(var)
                                    vars_found.append(var)

            if stmt.HasField("spread_action"):
                spread = stmt.spread_action
                if spread.HasField("collection"):
                    for var in self._collect_variables_from_expr(spread.collection):
                        if var not in seen:
                            seen.add(var)
                            vars_found.append(var)
                if spread.HasField("action"):
                    for kwarg in spread.action.kwargs:
                        for var in self._collect_variables_from_expr(kwarg.value):
                            if var not in seen:
                                seen.add(var)
                                vars_found.append(var)

        return vars_found

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

    def _wrap_body_as_function(
        self,
        body: List[ir.Statement],
        prefix: str,
        node: ast.AST,
        inputs: Optional[List[str]] = None,
        modified_vars: Optional[List[str]] = None,
    ) -> List[ir.Statement]:
        """Wrap a body with multiple calls into a synthetic function.

        Args:
            body: The statements to wrap
            prefix: Name prefix for the synthetic function
            node: AST node for span information
            inputs: Variables to pass as inputs (e.g., loop variables)
            modified_vars: Out-of-scope variables modified in the body.
                          These are added as inputs AND returned as outputs,
                          enabling functional transformation of external state.

        Returns a list containing a single function call statement (or assignment
        if modified_vars are present).
        """
        fn_name = self._ctx.next_implicit_fn_name(prefix)
        fn_inputs = list(inputs or [])

        # Add modified variables as inputs (they need to be passed in)
        modified_vars = modified_vars or []
        for var in modified_vars:
            if var not in fn_inputs:
                fn_inputs.append(var)

        # If there are modified variables, add a return statement for them
        wrapped_body = list(body)
        if modified_vars:
            # Create return statement: return (var1, var2, ...) or return var1
            if len(modified_vars) == 1:
                return_expr = ir.Expr(
                    variable=ir.Variable(name=modified_vars[0]),
                    span=make_span(node),
                )
            else:
                # Return as list (tuples are represented as lists in IR)
                return_expr = ir.Expr(
                    list=ir.ListExpr(
                        elements=[ir.Expr(variable=ir.Variable(name=var)) for var in modified_vars]
                    ),
                    span=make_span(node),
                )
            return_stmt = ir.Statement(span=make_span(node))
            return_stmt.return_stmt.CopyFrom(ir.ReturnStmt(value=return_expr))
            wrapped_body.append(return_stmt)

        # Create the synthetic function
        implicit_fn = ir.FunctionDef(
            name=fn_name,
            io=ir.IoDecl(inputs=fn_inputs, outputs=modified_vars),
            body=ir.Block(statements=wrapped_body),
            span=make_span(node),
        )
        self._ctx.implicit_functions.append(implicit_fn)

        # Create a function call expression
        kwargs = [
            ir.Kwarg(name=var, value=ir.Expr(variable=ir.Variable(name=var))) for var in fn_inputs
        ]
        fn_call_expr = ir.Expr(
            function_call=ir.FunctionCall(name=fn_name, kwargs=kwargs),
            span=make_span(node),
        )

        # If there are modified variables, create an assignment statement
        # so the returned values are assigned back to the variables
        call_stmt = ir.Statement(span=make_span(node))
        if modified_vars:
            # Create assignment: var1, var2 = fn(...) or var1 = fn(...)
            assign = ir.Assignment(value=fn_call_expr)
            assign.targets.extend(modified_vars)
            call_stmt.assignment.CopyFrom(assign)
        else:
            call_stmt.expr_stmt.CopyFrom(ir.ExprStmt(expr=fn_call_expr))

        return [call_stmt]

    def _visit_return(self, node: ast.Return) -> List[ir.Statement]:
        """Convert return statement to IR.

        Return statements should only contain variables or literals, not action calls.
        If the return contains an action call, we normalize it:
            return await action()
        becomes:
            _return_tmp = await action()
            return _return_tmp

        Constructor calls (like return MyModel(...)) are not supported and will
        raise an error with a recommendation to use an @action instead.
        """
        if node.value:
            # Check if returning an action call - normalize to assignment + return
            action_call = self._extract_action_call(node.value)
            if action_call:
                # Create a temporary variable for the action result
                tmp_var = "_return_tmp"

                # Create assignment: _return_tmp = await action()
                assign_stmt = ir.Statement(span=make_span(node))
                value = ir.Expr(action_call=action_call, span=make_span(node))
                assign = ir.Assignment(targets=[tmp_var], value=value)
                assign_stmt.assignment.CopyFrom(assign)

                # Create return: return _return_tmp
                return_stmt = ir.Statement(span=make_span(node))
                var_expr = ir.Expr(variable=ir.Variable(name=tmp_var), span=make_span(node))
                ret = ir.ReturnStmt(value=var_expr)
                return_stmt.return_stmt.CopyFrom(ret)

                return [assign_stmt, return_stmt]

            # Normalize return of function calls into assignment + return
            expr = self._compile_expression(node.value)
            if expr and expr.HasField("function_call"):
                tmp_var = self._ctx.next_implicit_fn_name(prefix="return_tmp")

                assign_stmt = ir.Statement(span=make_span(node))
                assign_stmt.assignment.CopyFrom(ir.Assignment(targets=[tmp_var], value=expr))

                return_stmt = ir.Statement(span=make_span(node))
                var_expr = ir.Expr(variable=ir.Variable(name=tmp_var), span=make_span(node))
                return_stmt.return_stmt.CopyFrom(ir.ReturnStmt(value=var_expr))
                return [assign_stmt, return_stmt]

            # Regular return with expression (variable, literal, etc.)
            if expr:
                stmt = ir.Statement(span=make_span(node))
                return_stmt = ir.ReturnStmt(value=expr)
                stmt.return_stmt.CopyFrom(return_stmt)
                return [stmt]

        # Return with no value
        stmt = ir.Statement(span=make_span(node))
        stmt.return_stmt.CopyFrom(ir.ReturnStmt())
        return [stmt]

    def _visit_break(self, node: ast.Break) -> List[ir.Statement]:
        """Convert break statement to IR."""
        stmt = ir.Statement(span=make_span(node))
        stmt.break_stmt.CopyFrom(ir.BreakStmt())
        return [stmt]

    def _visit_continue(self, node: ast.Continue) -> List[ir.Statement]:
        """Convert continue statement to IR."""
        stmt = ir.Statement(span=make_span(node))
        stmt.continue_stmt.CopyFrom(ir.ContinueStmt())
        return [stmt]

    def _visit_aug_assign(self, node: ast.AugAssign) -> List[ir.Statement]:
        """Convert augmented assignment (+=, -=, etc.) to IR."""
        # For now, we can represent this as a regular assignment with binary op
        # target op= value  ->  target = target op value
        stmt = ir.Statement(span=make_span(node))

        targets: List[str] = []
        if isinstance(node.target, ast.Name):
            targets.append(node.target.id)

        left = self._compile_expression(node.target)
        right = self._compile_expression(node.value)
        if right and right.HasField("function_call"):
            tmp_var = self._ctx.next_implicit_fn_name(prefix="aug_tmp")

            assign_tmp = ir.Statement(span=make_span(node))
            assign_tmp.assignment.CopyFrom(
                ir.Assignment(
                    targets=[tmp_var],
                    value=ir.Expr(function_call=right.function_call, span=make_span(node)),
                )
            )

            if left:
                op = binary_operator(node.op)
                if op:
                    binary = ir.BinaryOp(
                        left=left,
                        op=op,
                        right=ir.Expr(variable=ir.Variable(name=tmp_var)),
                    )
                    value = ir.Expr(binary_op=binary)
                    assign = ir.Assignment(targets=targets, value=value)
                    stmt.assignment.CopyFrom(assign)
                    return [assign_tmp, stmt]
            return [assign_tmp]

        if left and right:
            op = binary_operator(node.op)
            if op:
                binary = ir.BinaryOp(left=left, op=op, right=right)
                value = ir.Expr(binary_op=binary)
                assign = ir.Assignment(targets=targets, value=value)
                stmt.assignment.CopyFrom(assign)
                return [stmt]

        return []

    def _collect_function_inputs(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> List[str]:
        """Collect workflow inputs from function parameters, including kw-only args."""
        if node.args.vararg is not None or node.args.kwarg is not None:
            raise UnsupportedPatternError(
                "Variadic workflow/helper parameters are unsupported",
                "Declare each workflow argument explicitly.",
                line=node.lineno,
                col=node.col_offset,
            )
        args: List[str] = []
        seen: set[str] = set()

        ordered_args = list(node.args.posonlyargs) + list(node.args.args)
        if ordered_args and ordered_args[0].arg == "self":
            ordered_args = ordered_args[1:]

        for arg in ordered_args:
            if arg.arg not in seen:
                args.append(arg.arg)
                seen.add(arg.arg)

        if node.args.vararg and node.args.vararg.arg not in seen:
            args.append(node.args.vararg.arg)
            seen.add(node.args.vararg.arg)

        for arg in node.args.kwonlyargs:
            if arg.arg not in seen:
                args.append(arg.arg)
                seen.add(arg.arg)

        if node.args.kwarg and node.args.kwarg.arg not in seen:
            args.append(node.args.kwarg.arg)
            seen.add(node.args.kwarg.arg)

        return args

    def _extract_action_call(self, node: ast.expr) -> Optional[ir.ActionCall]:
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            handler = self._language_calls.resolve(node.value.func, self._language_call_namespace)
            if handler is not None:
                if not handler.requires_await:
                    raise handler.error(node.value, "this call cannot be awaited")
                lowered = handler.compile(node.value, self._compiler_context)
                if lowered.value.HasField("action_call") and not lowered.statements:
                    return lowered.value.action_call
        return None

    def _convert_to_function_call(self, node: ast.Call) -> Optional[ir.FunctionCall]:
        """Convert an AST Call to IR FunctionCall."""
        func_name = self._get_func_name(node.func)
        if not func_name:
            return None

        information = self._ctx.function_signatures.get(func_name)
        if information is None:
            return None
        bound = bind_call(node, information, self._language_call_namespace)
        fn_call = ir.FunctionCall(name=func_name)
        for argument in node.args:
            value = self._compile_expression(argument)
            assert value is not None
            fn_call.args.append(value)
        for keyword in node.keywords:
            assert keyword.arg is not None
            value = self._compile_expression(keyword.value)
            assert value is not None
            fn_call.kwargs.append(ir.Kwarg(name=keyword.arg, value=value))
        for parameter in information.parameters.values():
            if (
                parameter.name not in bound.arguments
                and parameter.default is not inspect.Parameter.empty
            ):
                value = self._compile_expression(literal_expression(parameter.default, node))
                assert value is not None
                fn_call.kwargs.append(ir.Kwarg(name=parameter.name, value=value))

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
            name = ".".join(reversed(parts))
            if name.startswith("self."):
                return name[5:]
            return name
        return None

    def _convert_registered_call(self, node: ast.Call) -> Optional[ir.Expr]:
        handler = self._language_calls.resolve(node.func, self._language_call_namespace)
        if handler is not None:
            handler.parse_arguments(node)
            if handler.requires_await:
                raise handler.error(
                    node, "await this call in its own assignment, return, or expression statement"
                )
            lowered = handler.compile(node, self._compiler_context)
            if lowered.statements:
                raise handler.error(node, "use this call in its own expression statement")
            return lowered.value
        if _is_self_method_call(node):
            function = self._convert_to_function_call(node)
            if function is not None:
                return ir.Expr(function_call=function)
        return None

    def _resolve_enum_attribute(self, node: ast.Attribute) -> Optional[ir.Expr]:
        instance = self._resolve_instance_attribute_chain(node)
        if instance is not None:
            return instance
        value = _resolve_enum_attribute_value(node, self._module_globals)
        if value is None:
            return None
        literal = constant_to_literal(value)
        if literal is None:
            line = getattr(node, "lineno", None)
            col = getattr(node, "col_offset", None)
            raise UnsupportedPatternError(
                "Enum value must be a primitive literal",
                RECOMMENDATIONS["unsupported_literal"],
                line=line,
                col=col,
            )
        expr = ir.Expr(span=make_span(node))
        expr.literal.CopyFrom(literal)
        return expr

    def _compile_expression(self, node: ast.expr) -> Optional[ir.Expr]:
        """Dispatch syntax lowering and recurse through shared compiler operations."""
        lowered = SYNTAX_HANDLERS.lower_expression(node, self._compiler_context)
        if lowered is not None:
            return lowered
        if isinstance(node, ast.Attribute):
            resolved = self._resolve_enum_attribute(node)
            if resolved is not None:
                return resolved
        return _expr_to_ir(node, self._compiler_context)

    def _resolve_instance_attribute_chain(self, node: ast.expr) -> Optional[ir.Expr]:
        """Resolve self.attr.subattr chains at build time.

        For expressions like self.config.max_retries where self.config is
        defined in __init__, evaluate the chain at build time and return
        the value as an IR literal.
        """
        chain = self._extract_self_attribute_chain(node)
        if chain is None:
            return None

        # chain[0] is the top-level attribute (e.g., "config")
        # chain[1:] are nested attributes (e.g., ["max_retries"])
        top_attr = chain[0]
        if top_attr not in self._instance_attrs or top_attr in self._ambiguous_instance_attrs:
            raise UnsupportedPatternError(
                "Instance attribute is not assigned a unique literal in __init__",
                "Pass runtime values through run() arguments.",
                line=node.lineno,
                col=node.col_offset,
            )

        # Get the AST expression for the top-level attribute
        attr_ast = self._instance_attrs[top_attr]

        # Try to resolve nested attributes
        resolved_ast = self._resolve_nested_attributes(attr_ast, chain[1:])
        if resolved_ast is None:
            raise UnsupportedPatternError(
                "Instance attribute cannot be resolved at compile time",
                "Pass runtime values through run() arguments.",
                line=node.lineno,
                col=node.col_offset,
            )

        # Convert the resolved AST to IR
        return self._compile_expression(resolved_ast)

    def _extract_self_attribute_chain(self, node: ast.expr) -> Optional[List[str]]:
        """Extract attribute chain from self.a.b.c pattern.

        Returns ["a", "b", "c"] for self.a.b.c, or None if not a self chain.
        """
        chain: List[str] = []
        current = node

        while isinstance(current, ast.Attribute):
            chain.append(current.attr)
            current = current.value

        # Check if the base is "self"
        if isinstance(current, ast.Name) and current.id == "self":
            chain.reverse()
            return chain

        return None

    def _resolve_nested_attributes(
        self, ast_node: ast.expr, attrs: List[str]
    ) -> Optional[ast.expr]:
        """Resolve nested attributes on an AST expression.

        For a Call like ClassName(a=1, b=2), resolving ["a"] returns the
        AST node for 1.
        """
        if not attrs:
            return ast_node

        attr_name = attrs[0]
        remaining = attrs[1:]

        # Handle Call expressions (constructor calls)
        if isinstance(ast_node, ast.Call):
            handler = self._language_calls.resolve(ast_node.func, self._language_call_namespace)
            if handler is None or handler.requires_await:
                return None
            handler.compile(ast_node, self._compiler_context)
            # Look for keyword argument with matching name
            for kw in ast_node.keywords:
                if kw.arg == attr_name:
                    return self._resolve_nested_attributes(kw.value, remaining)
            return None

        # Handle Dict expressions
        if isinstance(ast_node, ast.Dict):
            for key, value in zip(ast_node.keys, ast_node.values, strict=True):
                if isinstance(key, ast.Constant) and key.value == attr_name:
                    return self._resolve_nested_attributes(value, remaining)
            return None

        return None


def _attribute_chain(node: ast.Attribute) -> Optional[List[str]]:
    parts: List[str] = []
    current: ast.AST = node
    while isinstance(current, ast.Attribute):
        parts.append(current.attr)
        current = current.value
    if isinstance(current, ast.Name):
        parts.append(current.id)
        return list(reversed(parts))
    return None


def _resolve_enum_attribute_value(
    node: ast.Attribute,
    module_globals: Mapping[str, Any],
) -> Optional[Any]:
    chain = _attribute_chain(node)
    if not chain or len(chain) < 2:
        return None

    current = module_globals.get(chain[0])
    if current is None:
        return None

    for part in chain[1:-1]:
        try:
            current_dict = current.__dict__
        except AttributeError:
            return None
        current = current_dict.get(part)
        if current is None:
            return None

    member_name = chain[-1]
    if isinstance(current, EnumMeta):
        member = current.__members__.get(member_name)
        if member is None:
            return None
        return member.value

    return None


def _expr_to_ir(
    expr: ast.expr,
    context: CompilerContext,
) -> Optional[ir.Expr]:
    """Convert Python AST expression to IR Expr.

    Args:
        expr: The AST expression to convert.
        context: Shared recursive expression and call compilation.
    """
    result = ir.Expr(span=make_span(expr))

    if isinstance(expr, ast.Call):
        converted = context.synchronous_call(expr)
        if converted:
            return converted

    if isinstance(expr, ast.Name):
        result.variable.CopyFrom(ir.Variable(name=expr.id))
        return result

    if isinstance(expr, ast.Constant):
        literal = constant_to_literal(expr.value)
        if literal:
            result.literal.CopyFrom(literal)
            return result

    if isinstance(expr, ast.Compare):
        left = context.expression(expr.left)
        if not left:
            return None
        # For simplicity, handle single comparison
        if expr.ops and expr.comparators:
            op = _cmp_op_to_ir(expr.ops[0])
            right = context.expression(expr.comparators[0])
            if op and right:
                result.binary_op.CopyFrom(ir.BinaryOp(left=left, op=op, right=right))
                return result

    if isinstance(expr, ast.List):
        if any(isinstance(element, ast.Starred) for element in expr.elts):
            segments: list[ir.Expr] = []
            buffered_elements: list[ir.Expr] = []

            def flush_buffered_elements() -> None:
                if not buffered_elements:
                    return
                segments.append(ir.Expr(list=ir.ListExpr(elements=list(buffered_elements))))
                buffered_elements.clear()

            for element in expr.elts:
                if isinstance(element, ast.Starred):
                    flush_buffered_elements()
                    starred_value = context.expression(element.value)
                    if not starred_value:
                        return None
                    segments.append(starred_value)
                    continue

                value = context.expression(element)
                if not value:
                    return None
                buffered_elements.append(value)

            flush_buffered_elements()
            if not segments:
                result.list.CopyFrom(ir.ListExpr(elements=[]))
                return result

            combined = segments[0]
            for segment in segments[1:]:
                combined = ir.Expr(
                    binary_op=ir.BinaryOp(
                        left=combined,
                        op=ir.BinaryOperator.BINARY_OP_ADD,
                        right=segment,
                    )
                )
            return combined

        elements = [context.expression(e) for e in expr.elts]
        if all(e for e in elements):
            list_expr = ir.ListExpr(elements=[e for e in elements if e])
            result.list.CopyFrom(list_expr)
            return result

    if isinstance(expr, ast.Dict):
        entries: List[ir.DictEntry] = []
        for k, v in zip(expr.keys, expr.values, strict=False):
            if k:
                key_expr = context.expression(k)
                value_expr = context.expression(v)
                if key_expr and value_expr:
                    entries.append(ir.DictEntry(key=key_expr, value=value_expr))
        result.dict.CopyFrom(ir.DictExpr(entries=entries))
        return result

    if isinstance(expr, ast.Subscript):
        obj = context.expression(expr.value)
        index = context.expression(expr.slice) if isinstance(expr.slice, ast.AST) else None
        if obj and index:
            result.index.CopyFrom(ir.IndexAccess(object=obj, index=index))
            return result

    if isinstance(expr, ast.Attribute):
        obj = context.expression(expr.value)
        if obj:
            result.dot.CopyFrom(ir.DotAccess(object=obj, attribute=expr.attr))
            return result

    if isinstance(expr, ast.Await) and isinstance(expr.value, ast.Call):
        converted = context.synchronous_call(expr.value)
        if converted is not None:
            return converted

    if isinstance(expr, ast.Call):
        raise UnsupportedPatternError(
            f"Calling '{ast.unparse(expr.func)}()' is not supported in workflow code",
            RECOMMENDATIONS["sync_function_call"],
            line=expr.lineno,
            col=expr.col_offset,
        )

    if isinstance(expr, ast.Tuple):
        # Handle tuple as list for now
        elements = [context.expression(e) for e in expr.elts]
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

    if isinstance(expr, ast.Constant):
        if constant_to_literal(expr.value) is None:
            raise UnsupportedPatternError(
                f"Unsupported literal type '{type(expr.value).__name__}'",
                RECOMMENDATIONS["unsupported_literal"],
                line=line,
                col=col,
            )

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
            "Dict comprehensions are not supported in this context",
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
    elif isinstance(expr, ast.expr):
        raise UnsupportedPatternError(
            f"Unsupported expression type '{type(expr).__name__}'",
            RECOMMENDATIONS["unsupported_expression"],
            line=line,
            col=col,
        )


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
        # Identity comparisons are treated as equality in waymark since we deal
        # with serialized values where object identity doesn't apply
        ast.Is: ir.BinaryOperator.BINARY_OP_EQ,
        ast.IsNot: ir.BinaryOperator.BINARY_OP_NE,
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
        name = ".".join(reversed(parts))
        if name.startswith("self."):
            return name[5:]
        return name
    return None


def _is_self_method_call(node: ast.Call) -> bool:
    """Return True if the call is a direct self.method(...) invocation."""
    func = node.func
    return (
        isinstance(func, ast.Attribute)
        and isinstance(func.value, ast.Name)
        and func.value.id == "self"
    )
