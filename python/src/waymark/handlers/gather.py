"""The argument contract and lowering for workflow asyncio.gather calls."""

import ast
import asyncio
import copy
from dataclasses import dataclass, field
from typing import Literal

from waymark.compiler.ast import rewrite_load_names
from waymark.compiler.context import CompilerContext
from waymark.compiler.errors import UnsupportedPatternError
from waymark.language_calls import LanguageCallHandler, LoweredCall
from waymark.proto import ast_pb2 as ir


@dataclass(frozen=True)
class GatherArguments:
    """Supported gather operands and the only implemented exception mode."""

    awaitables: tuple[ast.Call | ast.Starred, ...]
    return_exceptions: Literal[False] = field(default=False, kw_only=True)

    @property
    def comprehension(self) -> ast.ListComp | ast.GeneratorExp | None:
        """Expose a sole comprehension spread for statement-level setup and lowering."""
        if len(self.awaitables) != 1:
            return None
        argument: ast.expr = self.awaitables[0]
        if isinstance(argument, ast.Starred):
            argument = argument.value
        if isinstance(argument, (ast.ListComp, ast.GeneratorExp)):
            return argument
        return None


class GatherHandler(LanguageCallHandler[GatherArguments]):
    """Validate and lower gather calls using shared compiler operations."""

    target = asyncio.gather
    name = "asyncio.gather"
    arguments_type = GatherArguments
    variadic_argument = "awaitables"
    recommendation = (
        "Pass action or workflow-helper calls directly, or use a single list comprehension "
        "spread: asyncio.gather(*[action(item) for item in items]). "
        "Omit return_exceptions or pass False; collecting exceptions with "
        "return_exceptions=True is not supported by the runtime."
    )

    def parse_arguments(self, node: ast.Call) -> GatherArguments:
        """Reject unsupported operand shapes before either gather lowering path runs."""
        arguments = super().parse_arguments(node)
        if not arguments.awaitables:
            raise self.error(node, "empty gathers are not supported")
        comprehension = arguments.comprehension
        if comprehension is not None:
            if len(comprehension.generators) != 1 or comprehension.generators[0].is_async:
                raise self.error(comprehension, "use a single synchronous comprehension loop")
            if not isinstance(comprehension.elt, ast.Call):
                raise self.error(comprehension.elt, "the comprehension element must be a call")
        else:
            for argument in arguments.awaitables:
                if not isinstance(argument, ast.Call):
                    raise self.error(
                        argument,
                        f"unsupported awaitable {ast.unparse(argument)!r}; "
                        "a comprehension spread must be the only positional argument",
                    )
        return arguments

    def lower(
        self, arguments: GatherArguments, node: ast.Call, context: CompilerContext
    ) -> LoweredCall:
        """Lower validated calls into static parallel calls or a comprehension spread."""
        comprehension = arguments.comprehension
        if comprehension is not None:
            return self._lower_comprehension(comprehension, context)
        return LoweredCall(
            value=ir.Expr(
                parallel_expr=ir.ParallelExpr(
                    calls=[self._lower_awaitable(call, context) for call in arguments.awaitables]
                )
            )
        )

    def _lower_awaitable(self, node: ast.expr, context: CompilerContext) -> ir.Call:
        if not isinstance(node, ast.Call):
            raise self.error(node, "each awaitable must be a direct call")
        call = context.call(node)
        if call is None:
            raise self.error(
                node,
                f"unsupported awaitable {ast.unparse(node.func)!r}; use an action or workflow helper",
            )
        return call

    def _lower_comprehension(
        self, comprehension: ast.ListComp | ast.GeneratorExp, context: CompilerContext
    ) -> LoweredCall:
        generator = comprehension.generators[0]
        prologue: list[ir.Statement] = []
        collection_node = copy.deepcopy(generator.iter)
        if generator.ifs:
            temporary = context.next_name("spread_items")
            prologue = self._build_filtered_spread_collection_statements(
                comprehension, temporary, context
            )
            collection_node = ast.copy_location(
                ast.Name(id=temporary, ctx=ast.Load()), comprehension
            )

        element_node = copy.deepcopy(comprehension.elt)
        if isinstance(generator.target, ast.Name):
            loop_var = generator.target.id
        else:
            loop_var = context.next_name("spread_item")
            replacements = self._build_comprehension_target_access_map(generator.target, loop_var)
            rewritten = rewrite_load_names(element_node, replacements)
            if not isinstance(rewritten, ast.expr):
                raise self.error(
                    comprehension, "spread pattern rewrite produced an invalid expression"
                )
            element_node = rewritten

        collection = context.expression(collection_node)
        if collection is None:
            raise self.error(
                collection_node, "could not convert collection expression in spread pattern"
            )
        return LoweredCall(
            value=ir.Expr(
                spread_expr=ir.SpreadExpr(
                    collection=collection,
                    loop_var=loop_var,
                    call=self._lower_awaitable(element_node, context),
                )
            ),
            statements=prologue,
        )

    def _build_filtered_spread_collection_statements(
        self,
        comprehension: ast.ListComp | ast.GeneratorExp,
        temp_collection_var: str,
        context: CompilerContext,
    ) -> list[ir.Statement]:
        """Build IR that materializes a filtered spread collection."""
        generator = comprehension.generators[0]
        loop_var = context.next_name("spread_filter_item")
        replacements = self._build_comprehension_target_access_map(generator.target, loop_var)

        init_assign_ast = ast.Assign(
            targets=[ast.Name(id=temp_collection_var, ctx=ast.Store())],
            value=ast.List(elts=[], ctx=ast.Load()),
            type_comment=None,
        )
        ast.copy_location(init_assign_ast, comprehension)
        ast.fix_missing_locations(init_assign_ast)

        append_value = ast.copy_location(ast.Name(id=loop_var, ctx=ast.Load()), generator.target)
        append_assign_ast = ast.Assign(
            targets=[ast.Name(id=temp_collection_var, ctx=ast.Store())],
            value=ast.BinOp(
                left=ast.Name(id=temp_collection_var, ctx=ast.Load()),
                op=ast.Add(),
                right=ast.List(elts=[append_value], ctx=ast.Load()),
            ),
            type_comment=None,
        )
        ast.copy_location(append_assign_ast, comprehension)
        ast.fix_missing_locations(append_assign_ast)

        loop_body: list[ast.stmt] = [append_assign_ast]
        for condition in reversed(generator.ifs):
            condition = copy.deepcopy(condition)
            if replacements:
                rewritten = rewrite_load_names(condition, replacements)
                assert isinstance(rewritten, ast.expr)
                condition = rewritten
            if_stmt = ast.If(test=condition, body=loop_body, orelse=[])
            ast.copy_location(if_stmt, condition)
            ast.fix_missing_locations(if_stmt)
            loop_body = [if_stmt]

        loop_ast = ast.For(
            target=ast.copy_location(ast.Name(id=loop_var, ctx=ast.Store()), generator.target),
            iter=copy.deepcopy(generator.iter),
            body=loop_body,
            orelse=[],
            type_comment=None,
        )
        ast.copy_location(loop_ast, comprehension)
        ast.fix_missing_locations(loop_ast)

        return [*context.statement(init_assign_ast), *context.statement(loop_ast)]

    def _build_comprehension_target_access_map(
        self, target: ast.expr, loop_var: str
    ) -> dict[str, ast.expr]:
        """Map destructured comprehension variables to accesses on one spread item."""
        replacements: dict[str, ast.expr] = {}

        def visit_target(current_target: ast.expr, current_expr: ast.expr) -> None:
            if isinstance(current_target, ast.Name):
                replacement = copy.deepcopy(current_expr)
                ast.copy_location(replacement, current_target)
                ast.fix_missing_locations(replacement)
                replacements[current_target.id] = replacement
                return

            if isinstance(current_target, (ast.Tuple, ast.List)):
                for index, elt in enumerate(current_target.elts):
                    next_expr = ast.Subscript(
                        value=copy.deepcopy(current_expr),
                        slice=ast.Constant(value=index),
                        ctx=ast.Load(),
                    )
                    ast.copy_location(next_expr, elt)
                    ast.fix_missing_locations(next_expr)
                    visit_target(elt, next_expr)
                return

            line = current_target.lineno if hasattr(current_target, "lineno") else None
            col = current_target.col_offset if hasattr(current_target, "col_offset") else None
            raise UnsupportedPatternError(
                "Spread pattern supports loop targets that are simple names or tuple/list unpacking",
                "Use a simple variable like `for item in items` or tuple/list unpacking of names.",
                line=line,
                col=col,
            )

        root_expr = ast.Name(id=loop_var, ctx=ast.Load())
        ast.copy_location(root_expr, target)
        ast.fix_missing_locations(root_expr)
        visit_target(target, root_expr)
        return replacements
