"""Shared source facts used by call and syntax handlers."""

import ast
from dataclasses import dataclass, field

from waymark.compiler_types import VALUE_KINDS, ValueKind


@dataclass
class SourceAnalysis:
    """Collect conservative function-wide facts without rejecting source constructs."""

    parents: dict[ast.AST, ast.AST] = field(default_factory=dict)
    types: dict[str, ValueKind] = field(default_factory=dict)
    fresh_lists: set[str] = field(default_factory=set)
    aliased: set[str] = field(default_factory=set)
    mutations: dict[str, list[ast.expr | ast.stmt]] = field(default_factory=dict)

    def analyze(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """Collect local type, parent, mutation, and alias facts without deciding language support."""
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
                kind = self.kind(assignment.value)
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
                    and self.kind(assignment.value) == "list"
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

    def kind(self, node: ast.expr | None) -> ValueKind:
        """Return a locally known value kind, retaining unknown runtime values as unknown."""
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
                return self.kind(operand)
            case ast.BinOp(left=left, right=right):
                return self.kind(left) if self.kind(left) == self.kind(right) else "unknown"
            case _:
                return "unknown"

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
