"""Compile workflow source through shared analysis and registered lowering handlers."""

import ast
import builtins
import inspect
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NoReturn

from waymark.compiler.analysis import SourceAnalysis
from waymark.compiler.ast import make_span
from waymark.compiler.context import CompilerContext
from waymark.compiler.errors import UnsupportedPatternError
from waymark.compiler.recommendations import RECOMMENDATIONS
from waymark.compiler.source import InstanceAttrs, extract_instance_attrs, load_workflow_source
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

    # Counter for generating unique temporary names
    temporary_counter: int = 0
    # Function signatures for workflow methods (used to fill in default arguments)
    function_signatures: dict[str, inspect.Signature] = field(default_factory=dict)

    def next_name(self, prefix: str = "temporary") -> str:
        """Generate a unique temporary name across the workflow program."""
        self.temporary_counter += 1
        return f"__{prefix}_{self.temporary_counter}__"


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
        module_globals: Mapping[str, Any] | None = None,
        instance_attrs: InstanceAttrs | None = None,
        function_locals: tuple[str, ...] = (),
        receiver_type: type | None = None,
    ):
        module_namespace = module_globals or {}
        self._language_call_namespace = {
            name: value
            for name, value in {**vars(builtins), **module_namespace}.items()
            if name not in function_locals
        }
        if receiver_type is not None:
            self._language_call_namespace["self"] = receiver_type
        self._language_calls = GLOBAL_HANDLERS
        self._compiler_context = CompilerContext(
            expression=self._compile_expression,
            statement=self._visit_statement,
            next_name=ctx.next_name,
            function_signatures=ctx.function_signatures,
            resolve=lambda function: self._language_calls.resolve(
                function, self._language_call_namespace
            ),
            namespace=self._language_call_namespace,
            validate_syntax=lambda node: SourceValidator(
                self._compiler_context, SYNTAX_HANDLERS
            ).visit(node),
            instance_attrs=instance_attrs.literals if instance_attrs else {},
            instance_namespace=instance_attrs.namespace if instance_attrs else module_namespace,
            ambiguous_instance_attrs=instance_attrs.ambiguous if instance_attrs else set(),
        )
        self.function_def: ir.FunctionDef | None = None

    def visit_FunctionDef(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> Any:
        """Validate and lower one workflow method."""
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
        """Apply the same lowering rules to asynchronous methods."""
        self.visit_FunctionDef(node)

    def _visit_statement(self, node: ast.stmt) -> list[ir.Statement]:
        """Convert a Python statement to IR Statement(s).

        Handlers may emit setup before the statement, so lowering returns an ordered list.

        Raises UnsupportedPatternError for unsupported statement types.
        """
        lowered = SYNTAX_HANDLERS.lower_statement(node, self._compiler_context)
        if lowered is not None:
            return lowered
        if isinstance(node, ast.Expr):
            return self._visit_expr_stmt(node)
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
        elif isinstance(node, ast.Match):
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

    def _visit_expr_stmt(self, node: ast.Expr) -> list[ir.Statement]:
        """Execute a value for its effects, retaining any setup emitted by its handler."""
        lowered = self._compiler_context.lower_value(node.value)
        value = lowered.value
        statement = ir.Statement(span=make_span(node))
        if value.HasField("parallel_expr"):
            statement.parallel_block.calls.extend(value.parallel_expr.calls)
        elif value.HasField("action_call"):
            statement.action_call.CopyFrom(value.action_call)
        elif value.HasField("spread_expr"):
            statement.assignment.CopyFrom(ir.Assignment(value=value))
        else:
            statement.expr_stmt.CopyFrom(ir.ExprStmt(expr=value))
        return [*lowered.statements, statement]

    def _visit_break(self, node: ast.Break) -> list[ir.Statement]:
        """Convert break statement to IR."""
        stmt = ir.Statement(span=make_span(node))
        stmt.break_stmt.CopyFrom(ir.BreakStmt())
        return [stmt]

    def _visit_continue(self, node: ast.Continue) -> list[ir.Statement]:
        """Convert continue statement to IR."""
        stmt = ir.Statement(span=make_span(node))
        stmt.continue_stmt.CopyFrom(ir.ContinueStmt())
        return [stmt]

    def _collect_function_inputs(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
        """Collect workflow inputs from function parameters, including kw-only args."""
        if node.args.vararg is not None or node.args.kwarg is not None:
            raise UnsupportedPatternError(
                "Variadic workflow/helper parameters are unsupported",
                "Declare each workflow argument explicitly.",
                line=node.lineno,
                col=node.col_offset,
            )
        args: list[str] = []
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

    def _compile_expression(self, node: ast.expr) -> ir.Expr:
        """Dispatch syntax lowering and recurse through shared compiler operations."""
        lowered = SYNTAX_HANDLERS.lower_expression(node, self._compiler_context)
        if lowered is not None:
            return lowered
        return _expr_to_ir(node, self._compiler_context)


def _expr_to_ir(expr: ast.expr, context: CompilerContext) -> ir.Expr:
    """Compile leaf values and calls that do not require surrounding setup statements."""
    if isinstance(expr, ast.Name):
        return ir.Expr(variable=ir.Variable(name=expr.id), span=make_span(expr))
    if isinstance(expr, ast.Call):
        lowered = context.compile_call(expr, awaited=False)
        if lowered.statements:
            raise UnsupportedPatternError(
                "This call requires setup statements; use it in its own statement",
                RECOMMENDATIONS["sync_function_call"],
                line=expr.lineno,
                col=expr.col_offset,
            )
        return lowered.value
    _check_unsupported_expression(expr)


def _check_unsupported_expression(expr: ast.expr) -> NoReturn:
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
    else:
        raise UnsupportedPatternError(
            f"Unsupported expression type '{type(expr).__name__}'",
            RECOMMENDATIONS["unsupported_expression"],
            line=line,
            col=col,
        )
