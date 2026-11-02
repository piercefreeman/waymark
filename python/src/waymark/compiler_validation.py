"""Traverse source once, dispatching construct checks to their registered handlers."""

import ast

from waymark.compiler_context import CompilerContext
from waymark.compiler_diagnostics import reject
from waymark.compiler_errors import UnsupportedPatternError
from waymark.compiler_literals import validate_literal
from waymark.language_syntax import SyntaxRegistry


class SourceValidator(ast.NodeVisitor):
    """Route source nodes to validation using the function's shared analysis."""

    def __init__(self, context: CompilerContext, registry: SyntaxRegistry):
        self.context = context
        self.registry = registry

    def validate(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """Validate executable bodies; annotations are checked by the function compiler."""
        for statement in node.body:
            self.visit(statement)

    def visit(self, node: ast.AST) -> None:
        """Dispatch registered syntax handlers before walking their children."""
        handler = self.registry.resolve(node)
        if handler is not None:
            handler.validate(node, self.context)
            for child in handler.children(node):
                self.visit(child)
        else:
            super().visit(node)

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
        kind = self.context.analysis.kind(node.target)
        if (
            kind not in {"int", "float", "str"}
            and self.context.analysis.kind(node.value) not in {"int", "float"}
            and not (
                kind == "list"
                and node.target.id
                in self.context.analysis.fresh_lists - self.context.analysis.aliased
            )
        ):
            reject(
                node, "Augmented assignment may mutate an aliased value; use explicit assignment"
            )
        self.context.validate_syntax(
            ast.copy_location(ast.BinOp(left=node.target, op=node.op, right=node.value), node)
        )

    def visit_DictComp(self, node: ast.DictComp) -> None:
        reject(node, "Dict comprehensions require unsupported subscript writes")

    def visit_Dict(self, node: ast.Dict) -> None:
        for key in node.keys:
            if key is None:
                return reject(node, "Dictionary unpacking (**mapping) is unsupported")
            if self.context.analysis.kind(key) != "str":
                reject(key, "Dictionary keys must be known strings")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if self.context.analysis.kind(node.value) == "exception":
            reject(node, "Caught exception inspection is unsupported")
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if self.context.analysis.kind(node.value) == "exception":
            reject(node, "Caught exception inspection is unsupported")
        self.generic_visit(node)

    def visit_Await(self, node: ast.Await) -> None:
        if isinstance(node.value, ast.Call):
            handler = self.context.resolve(node.value.func)
            if handler is not None:
                handler.parse_arguments(node.value)
                if not handler.requires_await:
                    raise handler.error(node.value, "this call cannot be awaited")
        parent = self.context.analysis.parents.get(node)
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
            and name in self.context.function_names
        )
        if self.context.resolve(function) is None and not helper:
            reject(node, f"Awaiting non-action function '{name}()' is not supported")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        handler = self.context.resolve(node.func)
        if handler is not None:
            handler.validate_usage(
                node, self.context.analysis.parents.get(node), self.context.analysis.kind
            )
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        parent = self.context.analysis.parents.get(node)
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
