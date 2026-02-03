"""Assignment and expression conversion helpers."""

from __future__ import annotations

import copy
from typing import Dict, Iterable, Sequence

from proto import ast_pb2 as ir

from ..models import assert_never
from ..nodes import ActionCallNode, AssignmentNode, ExpressionNode, FnCallNode


class AssignmentConversionMixin:
    """Convert assignments and expression statements into DAG nodes."""

    def convert_assignment(self, assign: ir.Assignment) -> list[str]:
        """Convert an assignment into one or more DAG nodes.

        Example IR:
        - x = @action()
        Produces a call node with target x and tracks x as defined at that node.
        """
        value = assign.value
        if value is None:
            return []

        targets = list(assign.targets)
        kind = value.WhichOneof("kind")
        match kind:
            case "function_call":
                target = targets[0] if targets else "_"
                return self.convert_fn_call_assignment(target, targets, value.function_call)
            case "action_call":
                return self.convert_action_call_with_targets(value.action_call, targets)
            case "parallel_expr":
                return self.convert_parallel_expr(value.parallel_expr, targets)
            case "spread_expr":
                return self.convert_spread_expr(value.spread_expr, targets)
            case (
                "literal"
                | "variable"
                | "binary_op"
                | "unary_op"
                | "list"
                | "dict"
                | "index"
                | "dot"
            ):
                node_id = self.next_id("assign")
                node = AssignmentNode(id=node_id, targets=targets, assign_expr=value)
                if self.current_function:
                    node.function_name = self.current_function
                self.dag.add_node(node)
                for t in targets:
                    self.track_var_definition(t, node_id)
                return [node_id]
            case None:
                return []
            case _:
                assert_never(kind)

    def convert_fn_call_assignment(
        self,
        target: str,
        targets: Sequence[str],
        call: ir.FunctionCall,
    ) -> list[str]:
        """Convert a function call assignment into a FnCallNode."""
        node_id = self.next_id("fn_call")
        kwargs, kwarg_exprs = self.extract_fn_call_args(call)
        call_expr = ir.Expr(function_call=copy.deepcopy(call))
        node = FnCallNode(
            id=node_id,
            called_function=call.name,
            kwargs=kwargs,
            kwarg_exprs=kwarg_exprs,
            targets=list(targets),
            assign_expr=call_expr,
            function_name=self.current_function,
        )
        self.dag.add_node(node)

        for t in targets:
            self.track_var_definition(t, node_id)

        return [node_id]

    def convert_action_call_with_targets(
        self,
        action: ir.ActionCall,
        targets: Sequence[str],
    ) -> list[str]:
        """Convert an action call into an ActionCallNode and track target bindings."""
        node_id = self.next_id("action")
        kwargs = self.extract_kwargs(action.kwargs)
        kwarg_exprs = self.extract_kwarg_exprs(action.kwargs)
        module_name = action.module_name if action.HasField("module_name") else None
        node = ActionCallNode(
            id=node_id,
            action_name=action.action_name,
            module_name=module_name,
            kwargs=kwargs,
            kwarg_exprs=kwarg_exprs,
            policies=list(action.policies),
            targets=list(targets) if targets else None,
            function_name=self.current_function,
        )
        self.dag.add_node(node)

        for t in targets:
            self.track_var_definition(t, node_id)

        return [node_id]

    def extract_kwargs(self, kwargs: Iterable[ir.Kwarg]) -> Dict[str, str]:
        """Convert kwarg expressions into their string forms for labels/guards."""
        result: Dict[str, str] = {}
        for kwarg in kwargs:
            if kwarg.HasField("value"):
                result[kwarg.name] = self.expr_to_string(kwarg.value)
        return result

    def extract_kwarg_exprs(self, kwargs: Iterable[ir.Kwarg]) -> Dict[str, ir.Expr]:
        """Copy kwarg expressions so nodes can inspect or rewrite them later."""
        result: Dict[str, ir.Expr] = {}
        for kwarg in kwargs:
            if kwarg.HasField("value"):
                result[kwarg.name] = copy.deepcopy(kwarg.value)
        return result

    def extract_fn_call_args(
        self, call: ir.FunctionCall
    ) -> tuple[Dict[str, str], Dict[str, ir.Expr]]:
        """Build kwargs/kwarg_exprs by merging positional args with known inputs."""
        kwargs = self.extract_kwargs(call.kwargs)
        kwarg_exprs = self.extract_kwarg_exprs(call.kwargs)

        input_names: Sequence[str] | None = None
        fn_def = self.function_defs.get(call.name)
        if fn_def is not None:
            input_names = fn_def.io.inputs

        if input_names is not None:
            for idx, arg in enumerate(call.args):
                if idx >= len(input_names):
                    break
                param_name = input_names[idx]
                if param_name not in kwargs:
                    kwargs[param_name] = self.expr_to_string(arg)
                if param_name not in kwarg_exprs:
                    kwarg_exprs[param_name] = copy.deepcopy(arg)

        return kwargs, kwarg_exprs

    def expr_to_string(self, expr: ir.Expr) -> str:
        """Render a best-effort string for UI labels and quick debugging."""
        kind = expr.WhichOneof("kind")
        match kind:
            case "variable":
                return f"${expr.variable.name}"
            case "literal":
                return self.literal_to_string(expr.literal)
            case "list":
                items = [self.expr_to_string(item) for item in expr.list.elements]
                return f"[{', '.join(items)}]"
            case "dict":
                entries = []
                for entry in expr.dict.entries:
                    key = self.expr_to_string(entry.key) if entry.HasField("key") else ""
                    val = self.expr_to_string(entry.value) if entry.HasField("value") else ""
                    entries.append(f"{key}: {val}")
                return f"{{{', '.join(entries)}}}"
            case "function_call":
                parts = [self.expr_to_string(arg) for arg in expr.function_call.args]
                for kw in expr.function_call.kwargs:
                    if kw.HasField("value"):
                        parts.append(f"{kw.name}={self.expr_to_string(kw.value)}")
                return f"{expr.function_call.name}({', '.join(parts)})"
            case (
                "binary_op"
                | "unary_op"
                | "index"
                | "dot"
                | "action_call"
                | "parallel_expr"
                | "spread_expr"
            ):
                return "null"
            case None:
                return "null"
            case _:
                assert_never(kind)

    def literal_to_string(self, lit: ir.Literal) -> str:
        """Render a literal to a stable string for labels."""
        kind = lit.WhichOneof("value")
        match kind:
            case "int_value":
                return str(lit.int_value)
            case "float_value":
                return str(lit.float_value)
            case "string_value":
                return f'"{lit.string_value}"'
            case "bool_value":
                return str(lit.bool_value).lower()
            case "is_none":
                return "null" if lit.is_none else "null"
            case None:
                return "null"
            case _:
                assert_never(kind)

    def convert_expr_statement(self, expr_stmt: ir.ExprStmt) -> list[str]:
        """Convert an expression statement into a node (or a no-op)."""
        if not expr_stmt.HasField("expr"):
            return []

        expr = expr_stmt.expr
        kind = expr.WhichOneof("kind")
        match kind:
            case "action_call":
                return self.convert_action_call_with_targets(expr.action_call, [])
            case "function_call":
                node_id = self.next_id("fn_call")
                kwargs, kwarg_exprs = self.extract_fn_call_args(expr.function_call)
                node = FnCallNode(
                    id=node_id,
                    called_function=expr.function_call.name,
                    kwargs=kwargs,
                    kwarg_exprs=kwarg_exprs,
                    function_name=self.current_function,
                )
                self.dag.add_node(node)
                return [node_id]
            case (
                "literal"
                | "variable"
                | "binary_op"
                | "unary_op"
                | "list"
                | "dict"
                | "index"
                | "dot"
                | "parallel_expr"
                | "spread_expr"
            ):
                node_id = self.next_id("expr")
                node = ExpressionNode(id=node_id, function_name=self.current_function)
                self.dag.add_node(node)
                return [node_id]
            case None:
                return []
            case _:
                assert_never(kind)
