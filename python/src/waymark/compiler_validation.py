"""Reject source constructs whose meaning the current IR cannot preserve."""

import ast
import math
from collections.abc import Mapping
from typing import Any, NoReturn

from waymark.compiler_errors import UnsupportedPatternError
from waymark.compiler_types import VALUE_KINDS, ValueKind
from waymark.language_calls import LanguageCallRegistry


def reject(node: ast.expr | ast.stmt | ast.ExceptHandler, message: str) -> NoReturn:
    """Report an unsupported source construct at its original location."""
    raise UnsupportedPatternError(
        message,
        "Move this operation into an action, or rewrite it using supported explicit statements.",
        line=node.lineno,
        col=node.col_offset,
    )


def validate_literal(value: Any) -> None:
    """Wire literals must fit the VM's finite, signed 64-bit value representation."""
    if type(value) is int and not -(2**63) <= value < 2**63:
        raise UnsupportedPatternError(
            "Integer literal is outside the signed 64-bit range",
            "Use a string for larger integers, and compute inside an action.",
        )
    if type(value) is float and not math.isfinite(value):
        raise UnsupportedPatternError(
            "Non-finite float literals are unsupported", "Use finite numbers."
        )


class SourceValidator(ast.NodeVisitor):
    """Conservative local facts; unknown dynamic values still need VM validation."""

    def __init__(
        self,
        namespace: Mapping[str, Any],
        registry: LanguageCallRegistry,
        function_names: set[str],
    ):
        self.namespace = namespace
        self.registry = registry
        self.function_names = function_names
        self.parents: dict[ast.AST, ast.AST] = {}
        self.types: dict[str, ValueKind] = {}
        self.fresh_lists: set[str] = set()
        self.aliased: set[str] = set()
        self.mutations: dict[str, list[ast.expr | ast.stmt]] = {}

    def validate(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> dict[str, type]:
        """Reject lossy source forms and return receivers proven safe for local list updates."""
        self.parents = {
            child: parent for parent in ast.walk(node) for child in ast.iter_child_nodes(parent)
        }
        for child in ast.walk(node):
            match child:
                case ast.Call(func=ast.Attribute(value=ast.Name(id=name))):
                    self.mutations.setdefault(name, []).append(child)
                case ast.AugAssign(target=ast.Name(id=name)):
                    self.mutations.setdefault(name, []).append(child)
        for argument in (*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs):
            if argument.annotation is not None:
                self.types[argument.arg] = self._annotation_kind(argument.annotation)
        for child in ast.walk(node):
            if isinstance(child, ast.ExceptHandler) and child.name:
                self.types[child.name] = "exception"
        # Iterate to include alias chains regardless of source ordering across branches.
        assignments = [
            child for child in ast.walk(node) if isinstance(child, (ast.Assign, ast.AnnAssign))
        ]
        for _ in range(len(assignments) + 1):
            previous = dict(self.types)
            for assignment in assignments:
                targets = (
                    assignment.targets
                    if isinstance(assignment, ast.Assign)
                    else [assignment.target]
                )
                kind = self._kind(assignment.value)
                for target in targets:
                    if isinstance(target, ast.Name):
                        if kind != "unknown":
                            old = self.types.get(target.id)
                            self.types[target.id] = (
                                kind if old in (None, "unknown", kind) else "mixed"
                            )
                        if isinstance(assignment.value, (ast.List, ast.ListComp)):
                            self.fresh_lists.add(target.id)
                if assignment.value is not None:
                    for value in ast.walk(assignment.value):
                        if isinstance(value, ast.Name) and self.types.get(value.id) == "list":
                            parent = self.parents.get(value)
                            if parent is assignment or isinstance(
                                parent, (ast.List, ast.Tuple, ast.Dict)
                            ):
                                self._record_alias(value)
                            ancestor = parent
                            while ancestor is not None and ancestor is not assignment:
                                if isinstance(ancestor, ast.Call):
                                    self._record_alias(value)
                                    break
                                ancestor = self.parents.get(ancestor)
            if previous == self.types:
                break
        for expression in ast.walk(node):
            if not isinstance(expression, ast.Call):
                continue
            for argument in [*expression.args, *(keyword.value for keyword in expression.keywords)]:
                for value in ast.walk(argument):
                    if isinstance(value, ast.Name) and self.types.get(value.id) == "list":
                        self._record_alias(value)
        for expression in ast.walk(node):
            if isinstance(expression, ast.ListComp):
                for value in ast.walk(expression.elt):
                    if isinstance(value, ast.Name) and self.types.get(value.id) == "list":
                        self._record_alias(value)
            elif isinstance(expression, ast.AugAssign):
                for value in ast.walk(expression.value):
                    if isinstance(value, ast.Name) and self.types.get(value.id) == "list":
                        self._record_alias(value)
        # A fresh initializer does not make other assignments to the same name safe.
        for assignment in assignments:
            targets = (
                assignment.targets if isinstance(assignment, ast.Assign) else [assignment.target]
            )
            for target in targets:
                if isinstance(target, ast.Name) and not (
                    isinstance(assignment.value, (ast.List, ast.ListComp))
                    or isinstance(assignment.value, ast.BinOp)
                    and isinstance(assignment.value.op, ast.Add)
                    and self._kind(assignment.value) == "list"
                ):
                    self.fresh_lists.discard(target.id)
        for name in list(self.fresh_lists):
            writes = [
                assignment
                for assignment in assignments
                if any(
                    isinstance(target, ast.Name) and target.id == name
                    for target in (
                        assignment.targets
                        if isinstance(assignment, ast.Assign)
                        else [assignment.target]
                    )
                )
            ]
            first = min(writes, key=lambda assignment: assignment.lineno)
            if self.parents.get(first) is not node:
                self.fresh_lists.discard(name)
            for usage in ast.walk(node):
                if isinstance(usage, ast.Name) and usage.id == name:
                    parent = self.parents.get(usage)
                    if isinstance(usage.ctx, ast.Store) and not isinstance(
                        parent, (ast.Assign, ast.AnnAssign, ast.AugAssign)
                    ):
                        self.fresh_lists.discard(name)
                    if isinstance(usage.ctx, ast.Load) and usage.lineno < first.lineno:
                        self.fresh_lists.discard(name)
        for statement in node.body:
            self.visit(statement)
        return {
            name: list for name in self.fresh_lists - self.aliased if self.types.get(name) == "list"
        }

    def visit_Try(self, node: ast.Try) -> None:
        if node.finalbody or node.orelse:
            reject(node, "try/finally and try/else are unsupported")
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:
        if node.orelse:
            reject(node, "for/else is unsupported")
        self._check_target(node.target)
        if self._kind(node.iter) in {"dict", "str", "exception"}:
            reject(node.iter, "Iteration over dictionaries, strings, or exceptions is unsupported")
        self.generic_visit(node)

    def visit_While(self, node: ast.While) -> None:
        if node.orelse:
            reject(node, "while/else is unsupported")
        self.generic_visit(node)

    def visit_comprehension(self, node: ast.comprehension) -> None:
        if self._kind(node.iter) in {"dict", "str", "exception"}:
            reject(node.iter, "Iteration over dictionaries, strings, or exceptions is unsupported")
        self.generic_visit(node)

    def visit_Compare(self, node: ast.Compare) -> None:
        if len(node.ops) != 1:
            reject(node, "Chained comparisons are unsupported")
        if self._kind(node.left) in {"list", "tuple", "dict"} or self._kind(
            node.comparators[0]
        ) in {
            "list",
            "tuple",
            "dict",
        }:
            reject(node, "Collection comparisons are unsupported")
        left, right = self._kind(node.left), self._kind(node.comparators[0])
        if left in {"int", "float", "bool"} and right in {"int", "float", "bool"} and left != right:
            reject(node, "Mixed numeric comparisons require VM support")
        self.generic_visit(node)

    def visit_BoolOp(self, node: ast.BoolOp) -> None:
        reject(node, "Short-circuit and/or expressions are unsupported; use nested if statements")

    def visit_Assign(self, node: ast.Assign) -> None:
        if len(node.targets) != 1:
            reject(node, "Chained assignments are unsupported")
        self._check_target(node.targets[0])
        if (
            isinstance(node.targets[0], (ast.Tuple, ast.List))
            and isinstance(node.value, (ast.Tuple, ast.List))
            and len(node.targets[0].elts) != len(node.value.elts)
        ):
            reject(node, "Unpacking target count does not match result count")
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self._check_target(node.target)
        if node.value is not None:
            self.visit(node.value)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        if not isinstance(node.target, ast.Name):
            reject(
                node.target,
                "Augmented assignment requires a name; subscript and attribute writes are unsupported",
            )
        assert isinstance(node.target, ast.Name)
        kind = self._kind(node.target)
        if (
            kind not in {"int", "float", "str"}
            and self._kind(node.value) not in {"int", "float"}
            and not (kind == "list" and node.target.id in self.fresh_lists - self.aliased)
        ):
            reject(
                node, "Augmented assignment may mutate an aliased value; use explicit assignment"
            )
        self.visit_BinOp(
            ast.copy_location(ast.BinOp(left=node.target, op=node.op, right=node.value), node)
        )

    def visit_DictComp(self, node: ast.DictComp) -> None:
        reject(node, "Dict comprehensions require unsupported subscript writes")

    def visit_Dict(self, node: ast.Dict) -> None:
        for key in node.keys:
            if key is None:
                return reject(node, "Dictionary unpacking (**mapping) is unsupported")
            if self._kind(key) != "str":
                reject(key, "Dictionary keys must be known strings")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if self._kind(node.value) == "exception":
            reject(node, "Caught exception inspection is unsupported")
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if self._kind(node.value) == "exception":
            reject(node, "Caught exception inspection is unsupported")
        self.generic_visit(node)

    def visit_Await(self, node: ast.Await) -> None:
        if isinstance(node.value, ast.Call):
            handler = self.registry.resolve(node.value.func, self.namespace)
            if handler is not None:
                handler.parse_arguments(node.value)
                if not handler.requires_await:
                    raise handler.error(node.value, "this call cannot be awaited")
        parent = self.parents.get(node)
        if not isinstance(parent, (ast.Assign, ast.AnnAssign, ast.Expr, ast.Return, ast.If)):
            reject(
                node, "Nested await expressions are unsupported; assign the awaited result first"
            )
        if not isinstance(node.value, ast.Call):
            return reject(node, "Await requires a direct action or workflow-helper call")
        call = node.value
        function = call.func
        name = (
            function.id
            if isinstance(function, ast.Name)
            else function.attr
            if isinstance(function, ast.Attribute)
            else "unknown"
        )
        helper = (
            isinstance(function, ast.Attribute)
            and isinstance(function.value, ast.Name)
            and function.value.id == "self"
            and name in self.function_names
        )
        if self.registry.resolve(function, self.namespace) is None and not helper:
            reject(node, f"Awaiting non-action function '{name}()' is not supported")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        handler = self.registry.resolve(node.func, self.namespace)
        if handler is not None:
            handler.validate_usage(node, self.parents.get(node), self._kind)
        self.generic_visit(node)

    def visit_BinOp(self, node: ast.BinOp) -> None:
        if not isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod)):
            reject(node, "Unsupported binary or augmented operator")
        left, right = self._kind(node.left), self._kind(node.right)
        numeric = {"int", "float", "bool"}
        if left in numeric and right in numeric and (left != right or "bool" in (left, right)):
            reject(node, "Mixed numeric types and boolean arithmetic require VM support")
        if isinstance(node.op, ast.Div) and (left == "int" or right == "int"):
            reject(node, "Integer true division requires VM support")
        if (
            isinstance(node.op, (ast.Div, ast.FloorDiv, ast.Mod))
            and right == "float"
            and isinstance(node.right, ast.Constant)
            and node.right.value == 0
        ):
            reject(node, "Float division by zero requires VM exception support")
        if isinstance(node.op, ast.Mult) and ({left, right} & {"str", "list", "tuple"}):
            reject(node, "Sequence repetition requires VM support")
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        parent = self.parents.get(node)
        value = node.value
        if (
            isinstance(parent, ast.UnaryOp)
            and isinstance(parent.op, ast.USub)
            and type(value) in (int, float)
        ):
            value = -value
        try:
            validate_literal(value)
        except UnsupportedPatternError as error:
            reject(node, error.message)

    def _record_alias(self, value: ast.Name) -> None:
        """Only escapes that can precede a mutation invalidate local list updates."""
        ancestors: set[ast.AST] = set()
        ancestor = self.parents.get(value)
        while ancestor is not None:
            ancestors.add(ancestor)
            ancestor = self.parents.get(ancestor)
        loops = {ancestor for ancestor in ancestors if isinstance(ancestor, (ast.For, ast.While))}
        for mutation in self.mutations.get(value.id, []):
            if mutation in ancestors or (mutation.lineno, mutation.col_offset) > (
                value.lineno,
                value.col_offset,
            ):
                self.aliased.add(value.id)
                return
            # An escape after an update can still precede the next loop iteration's update.
            ancestor = self.parents.get(mutation)
            while ancestor is not None:
                if ancestor in loops:
                    self.aliased.add(value.id)
                    return
                ancestor = self.parents.get(ancestor)

    def _annotation_kind(self, annotation: ast.expr) -> ValueKind:
        if isinstance(annotation, ast.Subscript):
            return self._annotation_kind(annotation.value)
        if isinstance(annotation, ast.Name):
            return VALUE_KINDS.get(annotation.id, "unknown")
        return "unknown"

    def _kind(self, node: ast.expr | None) -> ValueKind:
        match node:
            case ast.Name(id=name):
                return self.types.get(name, "unknown")
            case ast.Constant(value=value):
                return VALUE_KINDS.get(type(value).__name__, "unknown")
            case ast.List() | ast.ListComp():
                return "list"
            case ast.Tuple():
                return "tuple"
            case ast.Dict() | ast.DictComp():
                return "dict"
            case ast.UnaryOp(operand=operand):
                return self._kind(operand)
            case ast.BinOp(left=left, right=right):
                return self._kind(left) if self._kind(left) == self._kind(right) else "unknown"
            case _:
                return "unknown"

    def _check_target(self, target: ast.expr) -> None:
        if isinstance(target, ast.Name):
            return
        if isinstance(target, (ast.Tuple, ast.List)) and all(
            isinstance(element, ast.Name) for element in target.elts
        ):
            return
        reject(
            target,
            "Assignment requires a name or flat unpacking target; attribute and subscript writes are unsupported",
        )
