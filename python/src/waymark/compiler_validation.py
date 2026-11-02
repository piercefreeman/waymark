"""Validate registered calls with conservative local type information."""

import ast
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


class SourceValidator(ast.NodeVisitor):
    """Infer local value kinds for handler argument and placement checks."""

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
        self.generic_visit(node)

    def visit_Await(self, node: ast.Await) -> None:
        if isinstance(node.value, ast.Call):
            handler = self.registry.resolve(node.value.func, self.namespace)
            if handler is not None:
                handler.parse_arguments(node.value)
                if not handler.requires_await:
                    raise handler.error(node.value, "this call cannot be awaited")
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
