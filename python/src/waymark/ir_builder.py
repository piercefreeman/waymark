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

import ast
import builtins
import inspect
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, NoReturn, Optional

from waymark.call_binding import bind_call
from waymark.compiler.analysis import SourceAnalysis
from waymark.compiler.ast import make_span
from waymark.compiler.context import CompilerContext
from waymark.compiler.errors import UnsupportedPatternError
from waymark.compiler.literals import literal_expression
from waymark.compiler.recommendations import RECOMMENDATIONS
from waymark.compiler.source import InstanceAttrs, extract_instance_attrs, load_workflow_source
from waymark.compiler.targets import assignment_targets
from waymark.compiler.types import has_open_type_parameters
from waymark.compiler.validation import SourceValidator
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
    # Function signatures for workflow methods (used to fill in default arguments)
    function_signatures: Dict[str, inspect.Signature] = field(default_factory=dict)

    def next_implicit_fn_name(self, prefix: str = "implicit") -> str:
        """Generate a unique implicit function name."""
        self.implicit_fn_counter += 1
        return f"__{prefix}_{self.implicit_fn_counter}__"


def build_workflow_ir(workflow_cls: type["Workflow"]) -> ir.Program:
    """Load source, compile each reachable function, and assemble the workflow program."""
    functions = load_workflow_source(workflow_cls)
    context = TransformContext(
        function_signatures={function.name: function.signature for function in functions}
    )
    instance_attrs = extract_instance_attrs(workflow_cls)
    program = ir.Program()
    for function in functions:
        builder = IRBuilder(
            context,
            function.namespace,
            instance_attrs,
            function_locals=function.local_names,
            receiver_type=workflow_cls,
        )
        try:
            builder.visit(function.node)
        except UnsupportedPatternError as error:
            raise function.locate_error(error) from error
        assert builder.function_def is not None
        builder.function_def.name = function.name
        program.functions.append(builder.function_def)
    return program


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
            instance_namespace=instance_attrs.namespace if instance_attrs else self._module_globals,
            ambiguous_instance_attrs=self._ambiguous_instance_attrs,
        )
        self.function_def: Optional[ir.FunctionDef] = None

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
        statements: list[ir.Statement] = []
        for stmt in node.body:
            ir_stmts = self._visit_statement(stmt)
            statements.extend(ir_stmts)

        # Set the body
        self.function_def.body.CopyFrom(ir.Block(statements=statements))

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
        if isinstance(node, ast.Expr):
            result = self._visit_expr_stmt(node)
            return [result] if result else []
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
        line = node.lineno
        col = node.col_offset

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

    def _compile_expression(self, node: ast.expr) -> Optional[ir.Expr]:
        """Dispatch syntax lowering and recurse through shared compiler operations."""
        lowered = SYNTAX_HANDLERS.lower_expression(node, self._compiler_context)
        if lowered is not None:
            return lowered
        return _expr_to_ir(node, self._compiler_context)


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

    if isinstance(expr, ast.Call):
        raise UnsupportedPatternError(
            f"Calling '{ast.unparse(expr.func)}()' is not supported in workflow code",
            RECOMMENDATIONS["sync_function_call"],
            line=expr.lineno,
            col=expr.col_offset,
        )

    # Check for unsupported expression types
    _check_unsupported_expression(expr)

    return None


def _check_unsupported_expression(expr: ast.expr) -> None:
    """Check for unsupported expression types and raise descriptive errors."""
    line = expr.lineno
    col = expr.col_offset

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


def _is_self_method_call(node: ast.Call) -> bool:
    """Return True if the call is a direct self.method(...) invocation."""
    func = node.func
    return (
        isinstance(func, ast.Attribute)
        and isinstance(func.value, ast.Name)
        and func.value.id == "self"
    )
