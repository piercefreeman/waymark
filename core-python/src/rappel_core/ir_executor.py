"""Async IR statement executor for Rappel workflows."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Iterable, Mapping, Optional, Sequence

from proto import ast_pb2 as ir

from .dag import EXCEPTION_SCOPE_VAR, assert_never

ActionHandler = Callable[[ir.ActionCall, dict[str, Any]], Awaitable[Any]]


class ExecutionError(Exception):
    """Base error raised during IR execution."""


class FunctionNotFoundError(ExecutionError):
    """Raised when a function call references an unknown function."""


class VariableNotFoundError(ExecutionError):
    """Raised when a variable lookup fails."""


class ParallelExecutionError(ExecutionError):
    """Raised when parallel execution yields exceptions."""

    def __init__(self, errors: Sequence[BaseException]) -> None:
        super().__init__("parallel execution failed")
        self.errors = list(errors)


@dataclass
class ExecutionLimits:
    max_call_depth: int = 64
    max_loop_iterations: Optional[int] = None


class ControlFlow(Enum):
    NONE = "none"
    RETURN = "return"
    BREAK = "break"
    CONTINUE = "continue"


@dataclass
class StatementResult:
    control: ControlFlow = ControlFlow.NONE
    value: Any = None

    @classmethod
    def none(cls) -> "StatementResult":
        return cls()

    @classmethod
    def returned(cls, value: Any) -> "StatementResult":
        return cls(control=ControlFlow.RETURN, value=value)

    @classmethod
    def broke(cls) -> "StatementResult":
        return cls(control=ControlFlow.BREAK)

    @classmethod
    def continued(cls) -> "StatementResult":
        return cls(control=ControlFlow.CONTINUE)


@dataclass
class ExecutionFrame:
    variables: dict[str, Any]

    def get(self, name: str) -> Any:
        if name in self.variables:
            return self.variables[name]
        raise VariableNotFoundError(f"variable not found: {name}")

    def set(self, name: str, value: Any) -> None:
        self.variables[name] = value

    def snapshot(self) -> "ExecutionFrame":
        return ExecutionFrame(variables=dict(self.variables))


class StatementExecutor:
    """Executes IR statements and expressions with async action support."""

    def __init__(
        self,
        program: ir.Program,
        action_handler: ActionHandler,
        limits: Optional[ExecutionLimits] = None,
    ) -> None:
        self._program = program
        self._action_handler = action_handler
        self._limits = limits or ExecutionLimits()
        self._functions: dict[str, ir.FunctionDef] = {func.name: func for func in program.functions}

    async def execute_program(
        self,
        entry: str = "main",
        inputs: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        return await self.execute_function(entry, inputs=inputs)

    async def execute_function(
        self,
        name: str,
        inputs: Optional[Mapping[str, Any]] = None,
        args: Optional[Sequence[Any]] = None,
        depth: int = 0,
    ) -> Any:
        if depth >= self._limits.max_call_depth:
            raise ExecutionError("maximum call depth exceeded")

        fn_def = self._functions.get(name)
        if fn_def is None:
            raise FunctionNotFoundError(f"function not found: {name}")

        frame = ExecutionFrame(variables={})
        bound_inputs = self._bind_inputs(fn_def, args or [], inputs or {})
        frame.variables.update(bound_inputs)

        result = await self.execute_block(fn_def.body, frame, depth=depth)
        if result.control == ControlFlow.RETURN:
            return result.value

        if fn_def.io.outputs:
            return self._collect_output_values(fn_def, frame)
        return None

    async def execute_block(
        self,
        block: ir.Block,
        frame: ExecutionFrame,
        depth: int,
    ) -> StatementResult:
        for stmt in block.statements:
            result = await self.execute_statement(stmt, frame, depth=depth)
            if result.control != ControlFlow.NONE:
                return result
        return StatementResult.none()

    async def execute_statement(
        self,
        stmt: ir.Statement,
        frame: ExecutionFrame,
        depth: int,
    ) -> StatementResult:
        kind = stmt.WhichOneof("kind")
        match kind:
            case "assignment":
                value = await self._eval_expr(stmt.assignment.value, frame, depth=depth)
                self._assign_targets(stmt.assignment.targets, value, frame)
                return StatementResult.none()
            case "action_call":
                await self._execute_action(stmt.action_call, frame, depth=depth)
                return StatementResult.none()
            case "spread_action":
                await self._execute_spread_action(stmt.spread_action, frame, depth=depth)
                return StatementResult.none()
            case "parallel_block":
                await self._execute_parallel_block(stmt.parallel_block, frame, depth=depth)
                return StatementResult.none()
            case "for_loop":
                return await self._execute_for_loop(stmt.for_loop, frame, depth=depth)
            case "while_loop":
                return await self._execute_while_loop(stmt.while_loop, frame, depth=depth)
            case "conditional":
                return await self._execute_conditional(stmt.conditional, frame, depth=depth)
            case "try_except":
                return await self._execute_try_except(stmt.try_except, frame, depth=depth)
            case "return_stmt":
                if stmt.return_stmt.HasField("value"):
                    value = await self._eval_expr(stmt.return_stmt.value, frame, depth=depth)
                else:
                    value = None
                return StatementResult.returned(value)
            case "break_stmt":
                return StatementResult.broke()
            case "continue_stmt":
                return StatementResult.continued()
            case "expr_stmt":
                await self._execute_expr_stmt(stmt.expr_stmt, frame, depth=depth)
                return StatementResult.none()
            case None:
                return StatementResult.none()
            case _:
                assert_never(kind)

    def _bind_inputs(
        self,
        fn_def: ir.FunctionDef,
        args: Sequence[Any],
        kwargs: Mapping[str, Any],
    ) -> dict[str, Any]:
        bound: dict[str, Any] = {}
        inputs = list(fn_def.io.inputs)

        for idx, arg in enumerate(args):
            if idx >= len(inputs):
                raise ExecutionError("too many positional arguments")
            bound[inputs[idx]] = arg

        for key, value in kwargs.items():
            if key not in inputs:
                raise ExecutionError(f"unknown argument: {key}")
            bound[key] = value

        missing = [name for name in inputs if name not in bound]
        if missing:
            raise ExecutionError(f"missing arguments: {', '.join(missing)}")

        return bound

    def _collect_output_values(self, fn_def: ir.FunctionDef, frame: ExecutionFrame) -> Any:
        outputs = list(fn_def.io.outputs)
        values = [frame.variables.get(name) for name in outputs]
        if len(values) == 1:
            return values[0]
        return tuple(values)

    def _assign_targets(
        self,
        targets: Iterable[str],
        value: Any,
        frame: ExecutionFrame,
    ) -> None:
        targets_list = list(targets)
        if not targets_list:
            return

        if len(targets_list) == 1:
            frame.set(targets_list[0], value)
            return

        if isinstance(value, (list, tuple)):
            if len(value) != len(targets_list):
                raise ExecutionError("tuple unpacking mismatch")
            for target, item in zip(targets_list, value, strict=True):
                frame.set(target, item)
        else:
            for target in targets_list:
                frame.set(target, value)

    async def _eval_expr(
        self,
        expr: ir.Expr,
        frame: ExecutionFrame,
        depth: int,
    ) -> Any:
        kind = expr.WhichOneof("kind")
        match kind:
            case "literal":
                return self._eval_literal(expr.literal)
            case "variable":
                return frame.get(expr.variable.name)
            case "binary_op":
                return await self._eval_binary_op(expr.binary_op, frame, depth=depth)
            case "unary_op":
                return await self._eval_unary_op(expr.unary_op, frame, depth=depth)
            case "list":
                return [
                    await self._eval_expr(item, frame, depth=depth) for item in expr.list.elements
                ]
            case "dict":
                return await self._eval_dict(expr.dict, frame, depth=depth)
            case "index":
                return await self._eval_index(expr.index, frame, depth=depth)
            case "dot":
                return await self._eval_dot(expr.dot, frame, depth=depth)
            case "function_call":
                return await self._eval_function_call(expr.function_call, frame, depth=depth)
            case "action_call":
                return await self._execute_action(expr.action_call, frame, depth=depth)
            case "parallel_expr":
                return await self._eval_parallel_expr(expr.parallel_expr, frame, depth=depth)
            case "spread_expr":
                return await self._eval_spread_expr(expr.spread_expr, frame, depth=depth)
            case None:
                return None
            case _:
                assert_never(kind)

    def _eval_literal(self, literal: ir.Literal) -> Any:
        kind = literal.WhichOneof("value")
        match kind:
            case "int_value":
                return literal.int_value
            case "float_value":
                return literal.float_value
            case "string_value":
                return literal.string_value
            case "bool_value":
                return literal.bool_value
            case "is_none":
                return None
            case None:
                return None
            case _:
                assert_never(kind)

    async def _eval_binary_op(
        self,
        op: ir.BinaryOp,
        frame: ExecutionFrame,
        depth: int,
    ) -> Any:
        if not op.HasField("left") or not op.HasField("right"):
            raise ExecutionError("binary operator missing operands")

        operator = op.op
        if operator == ir.BinaryOperator.BINARY_OP_AND:
            left = await self._eval_expr(op.left, frame, depth=depth)
            if not bool(left):
                return left
            return await self._eval_expr(op.right, frame, depth=depth)
        if operator == ir.BinaryOperator.BINARY_OP_OR:
            left = await self._eval_expr(op.left, frame, depth=depth)
            if bool(left):
                return left
            return await self._eval_expr(op.right, frame, depth=depth)

        left = await self._eval_expr(op.left, frame, depth=depth)
        right = await self._eval_expr(op.right, frame, depth=depth)

        match operator:
            case ir.BinaryOperator.BINARY_OP_ADD:
                return left + right
            case ir.BinaryOperator.BINARY_OP_SUB:
                return left - right
            case ir.BinaryOperator.BINARY_OP_MUL:
                return left * right
            case ir.BinaryOperator.BINARY_OP_DIV:
                return left / right
            case ir.BinaryOperator.BINARY_OP_FLOOR_DIV:
                return left // right
            case ir.BinaryOperator.BINARY_OP_MOD:
                return left % right
            case ir.BinaryOperator.BINARY_OP_EQ:
                return left == right
            case ir.BinaryOperator.BINARY_OP_NE:
                return left != right
            case ir.BinaryOperator.BINARY_OP_LT:
                return left < right
            case ir.BinaryOperator.BINARY_OP_LE:
                return left <= right
            case ir.BinaryOperator.BINARY_OP_GT:
                return left > right
            case ir.BinaryOperator.BINARY_OP_GE:
                return left >= right
            case ir.BinaryOperator.BINARY_OP_IN:
                return left in right
            case ir.BinaryOperator.BINARY_OP_NOT_IN:
                return left not in right
            case ir.BinaryOperator.BINARY_OP_AND | ir.BinaryOperator.BINARY_OP_OR:
                raise ExecutionError("unexpected short-circuit operator")
            case _:
                raise ExecutionError("unknown binary operator")

    async def _eval_unary_op(
        self,
        op: ir.UnaryOp,
        frame: ExecutionFrame,
        depth: int,
    ) -> Any:
        if not op.HasField("operand"):
            raise ExecutionError("unary operator missing operand")

        operand = await self._eval_expr(op.operand, frame, depth=depth)
        operator = op.op
        match operator:
            case ir.UnaryOperator.UNARY_OP_NEG:
                return -operand
            case ir.UnaryOperator.UNARY_OP_NOT:
                return not bool(operand)
            case _:
                raise ExecutionError("unknown unary operator")

    async def _eval_dict(
        self,
        dict_expr: ir.DictExpr,
        frame: ExecutionFrame,
        depth: int,
    ) -> dict[Any, Any]:
        result: dict[Any, Any] = {}
        for entry in dict_expr.entries:
            if not entry.HasField("key") or not entry.HasField("value"):
                raise ExecutionError("dict entry missing key or value")
            key = await self._eval_expr(entry.key, frame, depth=depth)
            value = await self._eval_expr(entry.value, frame, depth=depth)
            result[key] = value
        return result

    async def _eval_index(
        self,
        index: ir.IndexAccess,
        frame: ExecutionFrame,
        depth: int,
    ) -> Any:
        if not index.HasField("object") or not index.HasField("index"):
            raise ExecutionError("index access missing object or index")
        obj = await self._eval_expr(index.object, frame, depth=depth)
        idx = await self._eval_expr(index.index, frame, depth=depth)
        return obj[idx]

    async def _eval_dot(
        self,
        dot: ir.DotAccess,
        frame: ExecutionFrame,
        depth: int,
    ) -> Any:
        if not dot.HasField("object"):
            raise ExecutionError("dot access missing object")
        obj = await self._eval_expr(dot.object, frame, depth=depth)
        if isinstance(obj, dict):
            if dot.attribute in obj:
                return obj[dot.attribute]
            raise ExecutionError(f"dict has no key '{dot.attribute}'")

        try:
            return object.__getattribute__(obj, dot.attribute)
        except AttributeError as exc:
            raise ExecutionError(f"attribute '{dot.attribute}' not found") from exc

    async def _eval_function_call(
        self,
        call: ir.FunctionCall,
        frame: ExecutionFrame,
        depth: int,
    ) -> Any:
        if call.global_function != ir.GlobalFunction.GLOBAL_FUNCTION_UNSPECIFIED:
            return await self._eval_global_function(call, frame, depth=depth)

        args = [await self._eval_expr(arg, frame, depth=depth) for arg in call.args]
        kwargs = {
            kw.name: await self._eval_expr(kw.value, frame, depth=depth)
            for kw in call.kwargs
            if kw.HasField("value")
        }
        return await self.execute_function(
            call.name,
            inputs=kwargs,
            args=args,
            depth=depth + 1,
        )

    async def _eval_global_function(
        self,
        call: ir.FunctionCall,
        frame: ExecutionFrame,
        depth: int,
    ) -> Any:
        args = [await self._eval_expr(arg, frame, depth=depth) for arg in call.args]
        kwargs = {
            kw.name: await self._eval_expr(kw.value, frame, depth=depth)
            for kw in call.kwargs
            if kw.HasField("value")
        }

        match call.global_function:
            case ir.GlobalFunction.GLOBAL_FUNCTION_RANGE:
                return list(range(*args))
            case ir.GlobalFunction.GLOBAL_FUNCTION_LEN:
                if args:
                    return len(args[0])
                if "items" in kwargs:
                    return len(kwargs["items"])
                raise ExecutionError("len() missing argument")
            case ir.GlobalFunction.GLOBAL_FUNCTION_ENUMERATE:
                if args:
                    return list(enumerate(args[0]))
                if "items" in kwargs:
                    return list(enumerate(kwargs["items"]))
                raise ExecutionError("enumerate() missing argument")
            case ir.GlobalFunction.GLOBAL_FUNCTION_ISEXCEPTION:
                if args:
                    return self._is_exception_value(args[0])
                if "value" in kwargs:
                    return self._is_exception_value(kwargs["value"])
                raise ExecutionError("isexception() missing argument")
            case ir.GlobalFunction.GLOBAL_FUNCTION_UNSPECIFIED:
                raise ExecutionError("global function unspecified")
            case _:
                assert_never(call.global_function)

    async def _execute_action(
        self,
        action: ir.ActionCall,
        frame: ExecutionFrame,
        depth: int,
    ) -> Any:
        kwargs = {
            kw.name: await self._eval_expr(kw.value, frame, depth=depth)
            for kw in action.kwargs
            if kw.HasField("value")
        }
        return await self._action_handler(action, kwargs)

    async def _execute_spread_action(
        self,
        spread: ir.SpreadAction,
        frame: ExecutionFrame,
        depth: int,
    ) -> None:
        results = await self._execute_spread(spread, frame, depth=depth)
        errors = [value for value in results if isinstance(value, BaseException)]
        if errors:
            raise ParallelExecutionError(errors)

    async def _eval_spread_expr(
        self,
        spread: ir.SpreadExpr,
        frame: ExecutionFrame,
        depth: int,
    ) -> list[Any]:
        return await self._execute_spread(spread, frame, depth=depth)

    async def _execute_spread(
        self,
        spread: ir.SpreadExpr | ir.SpreadAction,
        frame: ExecutionFrame,
        depth: int,
    ) -> list[Any]:
        collection = await self._eval_expr(spread.collection, frame, depth=depth)
        tasks = []
        for item in collection:
            child_frame = frame.snapshot()
            child_frame.set(spread.loop_var, item)
            tasks.append(self._execute_action(spread.action, child_frame, depth=depth))
        if not tasks:
            return []
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def _execute_parallel_block(
        self,
        parallel: ir.ParallelBlock,
        frame: ExecutionFrame,
        depth: int,
    ) -> None:
        results = await self._execute_parallel_calls(parallel.calls, frame, depth=depth)
        errors = [value for value in results if isinstance(value, BaseException)]
        if errors:
            raise ParallelExecutionError(errors)

    async def _eval_parallel_expr(
        self,
        parallel: ir.ParallelExpr,
        frame: ExecutionFrame,
        depth: int,
    ) -> list[Any]:
        return await self._execute_parallel_calls(parallel.calls, frame, depth=depth)

    async def _execute_parallel_calls(
        self,
        calls: Iterable[ir.Call],
        frame: ExecutionFrame,
        depth: int,
    ) -> list[Any]:
        tasks: list[Awaitable[Any]] = []
        for call in calls:
            tasks.append(self._execute_call(call, frame, depth=depth))
        if not tasks:
            return []
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def _execute_call(
        self,
        call: ir.Call,
        frame: ExecutionFrame,
        depth: int,
    ) -> Any:
        kind = call.WhichOneof("kind")
        match kind:
            case "action":
                return await self._execute_action(call.action, frame.snapshot(), depth=depth)
            case "function":
                return await self._eval_function_call(call.function, frame.snapshot(), depth=depth)
            case None:
                return None
            case _:
                assert_never(kind)

    async def _execute_for_loop(
        self,
        for_loop: ir.ForLoop,
        frame: ExecutionFrame,
        depth: int,
    ) -> StatementResult:
        if not for_loop.HasField("iterable"):
            raise ExecutionError("for loop missing iterable")

        iterable = await self._eval_expr(for_loop.iterable, frame, depth=depth)
        iteration_count = 0
        for item in iterable:
            iteration_count += 1
            if (
                self._limits.max_loop_iterations is not None
                and iteration_count > self._limits.max_loop_iterations
            ):
                raise ExecutionError("loop iteration limit exceeded")

            self._assign_loop_vars(for_loop.loop_vars, item, frame)

            if for_loop.HasField("block_body"):
                result = await self.execute_block(for_loop.block_body, frame, depth=depth)
                if result.control == ControlFlow.BREAK:
                    return StatementResult.none()
                if result.control == ControlFlow.CONTINUE:
                    continue
                if result.control == ControlFlow.RETURN:
                    return result

        return StatementResult.none()

    async def _execute_while_loop(
        self,
        while_loop: ir.WhileLoop,
        frame: ExecutionFrame,
        depth: int,
    ) -> StatementResult:
        if not while_loop.HasField("condition"):
            raise ExecutionError("while loop missing condition")

        iteration_count = 0
        while True:
            condition = await self._eval_expr(while_loop.condition, frame, depth=depth)
            if not bool(condition):
                return StatementResult.none()

            iteration_count += 1
            if (
                self._limits.max_loop_iterations is not None
                and iteration_count > self._limits.max_loop_iterations
            ):
                raise ExecutionError("loop iteration limit exceeded")

            if while_loop.HasField("block_body"):
                result = await self.execute_block(while_loop.block_body, frame, depth=depth)
                if result.control == ControlFlow.BREAK:
                    return StatementResult.none()
                if result.control == ControlFlow.CONTINUE:
                    continue
                if result.control == ControlFlow.RETURN:
                    return result

    async def _execute_conditional(
        self,
        cond: ir.Conditional,
        frame: ExecutionFrame,
        depth: int,
    ) -> StatementResult:
        if not cond.HasField("if_branch"):
            raise ExecutionError("conditional missing if branch")
        if_branch = cond.if_branch
        if not if_branch.HasField("condition"):
            raise ExecutionError("if branch missing condition")

        if_condition = await self._eval_expr(if_branch.condition, frame, depth=depth)
        if bool(if_condition):
            return await self.execute_block(if_branch.block_body, frame, depth=depth)

        for elif_branch in cond.elif_branches:
            if not elif_branch.HasField("condition"):
                raise ExecutionError("elif branch missing condition")
            condition = await self._eval_expr(elif_branch.condition, frame, depth=depth)
            if bool(condition):
                return await self.execute_block(elif_branch.block_body, frame, depth=depth)

        if cond.HasField("else_branch") and cond.else_branch.HasField("block_body"):
            return await self.execute_block(cond.else_branch.block_body, frame, depth=depth)

        return StatementResult.none()

    async def _execute_try_except(
        self,
        try_except: ir.TryExcept,
        frame: ExecutionFrame,
        depth: int,
    ) -> StatementResult:
        if not try_except.HasField("try_block"):
            return StatementResult.none()

        try:
            result = await self.execute_block(try_except.try_block, frame, depth=depth)
            if result.control != ControlFlow.NONE:
                return result
            return StatementResult.none()
        except BaseException as exc:  # noqa: BLE001 - interpreter catches user errors
            for handler in try_except.handlers:
                if self._exception_matches(handler.exception_types, exc):
                    if handler.exception_var:
                        frame.set(handler.exception_var, exc)
                    elif handler.HasField("exception_var"):
                        frame.set(EXCEPTION_SCOPE_VAR, exc)
                    if handler.HasField("block_body"):
                        return await self.execute_block(handler.block_body, frame, depth=depth)
                    return StatementResult.none()
            raise

    async def _execute_expr_stmt(
        self,
        expr_stmt: ir.ExprStmt,
        frame: ExecutionFrame,
        depth: int,
    ) -> None:
        if not expr_stmt.HasField("expr"):
            return
        await self._eval_expr(expr_stmt.expr, frame, depth=depth)

    def _exception_matches(self, exception_types: Sequence[str], exc: BaseException) -> bool:
        if not exception_types:
            return True
        if len(exception_types) == 1 and exception_types[0] == "Exception":
            return True
        names = [cls.__name__ for cls in exc.__class__.__mro__]
        return any(name in exception_types for name in names)

    def _is_exception_value(self, value: Any) -> bool:
        if isinstance(value, BaseException):
            return True
        if isinstance(value, dict) and "type" in value and "message" in value:
            return True
        return False

    def _assign_loop_vars(self, loop_vars: Sequence[str], item: Any, frame: ExecutionFrame) -> None:
        if not loop_vars:
            return
        if len(loop_vars) == 1:
            frame.set(loop_vars[0], item)
            return
        if not isinstance(item, (list, tuple)):
            raise ExecutionError("loop unpacking requires tuple/list value")
        if len(item) != len(loop_vars):
            raise ExecutionError("loop unpacking mismatch")
        for var, value in zip(loop_vars, item, strict=True):
            frame.set(var, value)
