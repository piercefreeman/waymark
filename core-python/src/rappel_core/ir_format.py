"""Pretty-printer for IR AST structures."""

from __future__ import annotations

import json
from typing import Optional

from proto import ast_pb2 as ir

from .dag import assert_never

_INDENT = "    "


class IRFormatter:
    """Render IR AST nodes into a source-like representation."""

    def __init__(self, indent: str = _INDENT) -> None:
        self._indent = indent

    def format_program(self, program: ir.Program) -> str:
        lines: list[str] = []
        for idx, fn_def in enumerate(program.functions):
            if idx:
                lines.append("")
            lines.extend(self._format_function(fn_def))
        return "\n".join(lines)

    def _format_function(self, fn_def: ir.FunctionDef) -> list[str]:
        io_decl = fn_def.io if fn_def.HasField("io") else None
        inputs = ", ".join(io_decl.inputs) if io_decl else ""
        outputs = ", ".join(io_decl.outputs) if io_decl else ""
        header = f"fn {fn_def.name}(input: [{inputs}], output: [{outputs}]):"
        lines = [header]
        if fn_def.HasField("body"):
            lines.extend(self._format_block(fn_def.body, 1))
        else:
            lines.append(self._indent_line(1, "pass"))
        return lines

    def _format_block(self, block: ir.Block, level: int) -> list[str]:
        if not block.statements:
            return [self._indent_line(level, "pass")]

        lines: list[str] = []
        for stmt in block.statements:
            lines.extend(self._format_statement(stmt, level))
        return lines

    def _format_statement(self, stmt: ir.Statement, level: int) -> list[str]:
        indent = self._indent_line(level, "")
        kind = stmt.WhichOneof("kind")
        match kind:
            case "assignment":
                targets = ", ".join(stmt.assignment.targets) or "_"
                value_kind = stmt.assignment.value.WhichOneof("kind")
                match value_kind:
                    case "parallel_expr":
                        return self._format_parallel_block(
                            stmt.assignment.value.parallel_expr, level, targets=targets
                        )
                    case "spread_expr":
                        expr = self._format_spread_expr(stmt.assignment.value.spread_expr)
                        return [f"{indent}{targets} = {expr}"]
                    case _:
                        expr = self._format_expr(stmt.assignment.value)
                        return [f"{indent}{targets} = {expr}"]
            case "action_call":
                return [f"{indent}{self._format_action_call(stmt.action_call)}"]
            case "spread_action":
                return [f"{indent}{self._format_spread_action(stmt.spread_action)}"]
            case "parallel_block":
                return self._format_parallel_block(stmt.parallel_block, level, targets=None)
            case "for_loop":
                loop_vars = ", ".join(stmt.for_loop.loop_vars) or "_"
                iterable = self._format_expr(stmt.for_loop.iterable)
                lines = [f"{indent}for {loop_vars} in {iterable}:"]
                lines.extend(self._format_block(stmt.for_loop.block_body, level + 1))
                return lines
            case "while_loop":
                condition = self._format_expr(stmt.while_loop.condition)
                lines = [f"{indent}while {condition}:"]
                lines.extend(self._format_block(stmt.while_loop.block_body, level + 1))
                return lines
            case "conditional":
                return self._format_conditional(stmt.conditional, level)
            case "try_except":
                return self._format_try_except(stmt.try_except, level)
            case "return_stmt":
                if stmt.return_stmt.HasField("value"):
                    expr = self._format_expr(stmt.return_stmt.value)
                    return [f"{indent}return {expr}"]
                return [f"{indent}return"]
            case "break_stmt":
                return [f"{indent}break"]
            case "continue_stmt":
                return [f"{indent}continue"]
            case "expr_stmt":
                expr_kind = stmt.expr_stmt.expr.WhichOneof("kind")
                match expr_kind:
                    case "parallel_expr":
                        return self._format_parallel_block(
                            stmt.expr_stmt.expr.parallel_expr, level, targets=None
                        )
                    case "spread_expr":
                        expr = self._format_spread_expr(stmt.expr_stmt.expr.spread_expr)
                        return [f"{indent}{expr}"]
                    case _:
                        expr = self._format_expr(stmt.expr_stmt.expr)
                        return [f"{indent}{expr}"]
            case None:
                return [f"{indent}pass"]
            case _:
                assert_never(kind)

    def _format_conditional(self, conditional: ir.Conditional, level: int) -> list[str]:
        indent = self._indent_line(level, "")
        lines: list[str] = []

        if conditional.HasField("if_branch"):
            condition = self._format_expr(conditional.if_branch.condition)
            lines.append(f"{indent}if {condition}:")
            lines.extend(self._format_block(conditional.if_branch.block_body, level + 1))

        for branch in conditional.elif_branches:
            condition = self._format_expr(branch.condition)
            lines.append(f"{indent}elif {condition}:")
            lines.extend(self._format_block(branch.block_body, level + 1))

        if conditional.HasField("else_branch"):
            lines.append(f"{indent}else:")
            lines.extend(self._format_block(conditional.else_branch.block_body, level + 1))

        return lines

    def _format_try_except(self, try_except: ir.TryExcept, level: int) -> list[str]:
        indent = self._indent_line(level, "")
        lines = [f"{indent}try:"]
        lines.extend(self._format_block(try_except.try_block, level + 1))

        for handler in try_except.handlers:
            exc_types = ", ".join(handler.exception_types)
            if exc_types:
                header = f"except {exc_types}"
            else:
                header = "except"
            if handler.exception_var:
                header = f"{header} as {handler.exception_var}"
            lines.append(f"{indent}{header}:")
            lines.extend(self._format_block(handler.block_body, level + 1))
        return lines

    def _format_parallel_block(
        self,
        parallel: ir.ParallelBlock | ir.ParallelExpr,
        level: int,
        targets: Optional[str],
    ) -> list[str]:
        indent = self._indent_line(level, "")
        header = "parallel:"
        if targets:
            header = f"{targets} = {header}"
        lines = [f"{indent}{header}"]
        if not parallel.calls:
            lines.append(self._indent_line(level + 1, "pass"))
            return lines
        for call in parallel.calls:
            lines.append(self._indent_line(level + 1, self._format_call(call)))
        return lines

    def _format_spread_action(self, spread: ir.SpreadAction) -> str:
        return self._format_spread(spread.collection, spread.loop_var, spread.action)

    def _format_spread_expr(self, spread: ir.SpreadExpr) -> str:
        return self._format_spread(spread.collection, spread.loop_var, spread.action)

    def _format_spread(self, collection: ir.Expr, loop_var: str, action: ir.ActionCall) -> str:
        collection_str = self._format_expr(collection)
        action_str = self._format_action_call(action)
        return f"spread {collection_str}:{loop_var} -> {action_str}"

    def _format_call(self, call: ir.Call) -> str:
        kind = call.WhichOneof("kind")
        match kind:
            case "action":
                return self._format_action_call(call.action)
            case "function":
                return self._format_function_call(call.function)
            case None:
                return "pass"
            case _:
                assert_never(kind)

    def _format_action_call(self, action: ir.ActionCall) -> str:
        prefix = action.action_name
        if action.module_name:
            prefix = f"{action.module_name}.{prefix}"
        args = []
        for kw in action.kwargs:
            if kw.HasField("value"):
                args.append(f"{kw.name}={self._format_expr(kw.value)}")
        rendered = f"@{prefix}({', '.join(args)})"
        for policy in action.policies:
            rendered = f"{rendered} {self._format_policy(policy)}"
        return rendered

    def _format_function_call(self, call: ir.FunctionCall) -> str:
        name = self._resolve_function_name(call)
        args = [self._format_expr(arg) for arg in call.args]
        for kw in call.kwargs:
            if kw.HasField("value"):
                args.append(f"{kw.name}={self._format_expr(kw.value)}")
        return f"{name}({', '.join(args)})"

    def _resolve_function_name(self, call: ir.FunctionCall) -> str:
        if call.name:
            return call.name
        match call.global_function:
            case ir.GlobalFunction.GLOBAL_FUNCTION_RANGE:
                return "range"
            case ir.GlobalFunction.GLOBAL_FUNCTION_LEN:
                return "len"
            case ir.GlobalFunction.GLOBAL_FUNCTION_ENUMERATE:
                return "enumerate"
            case ir.GlobalFunction.GLOBAL_FUNCTION_ISEXCEPTION:
                return "isexception"
            case ir.GlobalFunction.GLOBAL_FUNCTION_UNSPECIFIED:
                return "fn"
            case _:
                assert_never(call.global_function)

    def _format_policy(self, policy: ir.PolicyBracket) -> str:
        kind = policy.WhichOneof("kind")
        match kind:
            case "retry":
                retry = policy.retry
                exc_types = ", ".join(retry.exception_types)
                header = f"{exc_types} -> " if exc_types else ""
                parts = [f"retry: {retry.max_retries}"]
                if retry.HasField("backoff"):
                    parts.append(f"backoff: {self._format_duration(retry.backoff)}")
                return f"[{header}{', '.join(parts)}]"
            case "timeout":
                timeout = policy.timeout
                return f"[timeout: {self._format_duration(timeout.timeout)}]"
            case None:
                return "[]"
            case _:
                assert_never(kind)

    def _format_duration(self, duration: ir.Duration) -> str:
        seconds = duration.seconds
        if seconds and seconds % 3600 == 0:
            return f"{seconds // 3600}h"
        if seconds and seconds % 60 == 0:
            return f"{seconds // 60}m"
        return f"{seconds}s"

    def _format_expr(self, expr: ir.Expr, parent_prec: int = 0) -> str:
        kind = expr.WhichOneof("kind")
        match kind:
            case "literal":
                return self._format_literal(expr.literal)
            case "variable":
                return expr.variable.name
            case "binary_op":
                return self._format_binary_op(expr.binary_op, parent_prec)
            case "unary_op":
                return self._format_unary_op(expr.unary_op, parent_prec)
            case "list":
                items = [self._format_expr(item) for item in expr.list.elements]
                return f"[{', '.join(items)}]"
            case "dict":
                entries = []
                for entry in expr.dict.entries:
                    key = self._format_expr(entry.key) if entry.HasField("key") else ""
                    value = self._format_expr(entry.value) if entry.HasField("value") else ""
                    entries.append(f"{key}: {value}")
                return f"{{{', '.join(entries)}}}"
            case "index":
                obj = self._format_expr(expr.index.object, self._precedence("index"))
                idx = self._format_expr(expr.index.index)
                return f"{obj}[{idx}]"
            case "dot":
                obj = self._format_expr(expr.dot.object, self._precedence("dot"))
                return f"{obj}.{expr.dot.attribute}"
            case "function_call":
                return self._format_function_call(expr.function_call)
            case "action_call":
                return self._format_action_call(expr.action_call)
            case "parallel_expr":
                calls = ", ".join(self._format_call(call) for call in expr.parallel_expr.calls)
                return f"parallel({calls})"
            case "spread_expr":
                return self._format_spread_expr(expr.spread_expr)
            case None:
                return "None"
            case _:
                assert_never(kind)

    def _format_binary_op(self, op: ir.BinaryOp, parent_prec: int) -> str:
        op_str, prec = self._binary_operator(op.op)
        left = self._format_expr(op.left, prec)
        right = self._format_expr(op.right, prec + 1)
        rendered = f"{left} {op_str} {right}"
        if prec < parent_prec:
            return f"({rendered})"
        return rendered

    def _format_unary_op(self, op: ir.UnaryOp, parent_prec: int) -> str:
        op_str, prec = self._unary_operator(op.op)
        operand = self._format_expr(op.operand, prec)
        rendered = f"{op_str}{operand}"
        if prec < parent_prec:
            return f"({rendered})"
        return rendered

    def _binary_operator(self, op: int) -> tuple[str, int]:
        match op:
            case ir.BinaryOperator.BINARY_OP_OR:
                return "or", 10
            case ir.BinaryOperator.BINARY_OP_AND:
                return "and", 20
            case ir.BinaryOperator.BINARY_OP_EQ:
                return "==", 30
            case ir.BinaryOperator.BINARY_OP_NE:
                return "!=", 30
            case ir.BinaryOperator.BINARY_OP_LT:
                return "<", 30
            case ir.BinaryOperator.BINARY_OP_LE:
                return "<=", 30
            case ir.BinaryOperator.BINARY_OP_GT:
                return ">", 30
            case ir.BinaryOperator.BINARY_OP_GE:
                return ">=", 30
            case ir.BinaryOperator.BINARY_OP_IN:
                return "in", 30
            case ir.BinaryOperator.BINARY_OP_NOT_IN:
                return "not in", 30
            case ir.BinaryOperator.BINARY_OP_ADD:
                return "+", 40
            case ir.BinaryOperator.BINARY_OP_SUB:
                return "-", 40
            case ir.BinaryOperator.BINARY_OP_MUL:
                return "*", 50
            case ir.BinaryOperator.BINARY_OP_DIV:
                return "/", 50
            case ir.BinaryOperator.BINARY_OP_FLOOR_DIV:
                return "//", 50
            case ir.BinaryOperator.BINARY_OP_MOD:
                return "%", 50
            case ir.BinaryOperator.BINARY_OP_UNSPECIFIED:
                return "?", 0
            case _:
                assert_never(op)

    def _unary_operator(self, op: int) -> tuple[str, int]:
        match op:
            case ir.UnaryOperator.UNARY_OP_NEG:
                return "-", 60
            case ir.UnaryOperator.UNARY_OP_NOT:
                return "not ", 60
            case ir.UnaryOperator.UNARY_OP_UNSPECIFIED:
                return "?", 0
            case _:
                assert_never(op)

    def _precedence(self, kind: str) -> int:
        match kind:
            case "index" | "dot":
                return 80
            case _:
                return 0

    def _format_literal(self, lit: ir.Literal) -> str:
        kind = lit.WhichOneof("value")
        match kind:
            case "int_value":
                return str(lit.int_value)
            case "float_value":
                return repr(lit.float_value)
            case "string_value":
                return json.dumps(lit.string_value)
            case "bool_value":
                return "True" if lit.bool_value else "False"
            case "is_none":
                return "None"
            case None:
                return "None"
            case _:
                assert_never(kind)

    def _indent_line(self, level: int, text: str) -> str:
        return f"{self._indent * level}{text}"


def format_program(program: ir.Program) -> str:
    """Convenience wrapper to format a program."""
    return IRFormatter().format_program(program)
