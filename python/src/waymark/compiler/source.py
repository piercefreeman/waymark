"""Load workflow source, lexical bindings, signatures, and diagnostic locations."""

import ast
import builtins
import inspect
import textwrap
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import FunctionType
from typing import TYPE_CHECKING, Any

from waymark.compiler.errors import UnsupportedPatternError
from waymark.global_handlers import GLOBAL_HANDLERS

if TYPE_CHECKING:
    from waymark.workflow import Workflow


@dataclass(frozen=True)
class ParsedFunction:
    """A discovered method with the bindings and source coordinates needed to compile it."""

    name: str
    node: ast.FunctionDef | ast.AsyncFunctionDef
    namespace: Mapping[str, Any]
    signature: inspect.Signature
    local_names: tuple[str, ...]
    filename: str | None
    start_line: int

    def locate_error(self, error: UnsupportedPatternError) -> UnsupportedPatternError:
        """Translate parser-relative coordinates back to the original Python file."""
        return UnsupportedPatternError(
            error.message,
            error.recommendation,
            line=self.start_line + error.line - 1 if error.line is not None else None,
            col=error.col + 1 if error.col is not None else None,
            filename=self.filename,
        )


@dataclass(frozen=True)
class InstanceAttrs:
    """What `__init__` assigns to `self`, as far as the compiler can read it."""

    literals: dict[str, ast.expr]
    """Attributes assigned exactly once, at the top level of `__init__`, with the value."""

    ambiguous: set[str]
    """Attributes assigned more than once, or under a branch or loop: unreadable."""

    namespace: Mapping[str, Any] = field(default_factory=dict)
    """Globals and closure bindings where the initializer expressions were defined."""


def load_workflow_source(workflow_cls: type["Workflow"]) -> list[ParsedFunction]:
    """Discover reachable methods in source order before any method is lowered."""
    original_run = _find_workflow_method(workflow_cls, "__workflow_run_impl__")
    if original_run is None:
        original_run = workflow_cls.__dict__.get("run")
    if not inspect.isfunction(original_run):
        raise ValueError(f"workflow {workflow_cls!r} missing run implementation")
    run = parse_function(original_run, "main")
    methods = [run]
    pending = _collect_self_method_calls(run.node)
    visited: set[str] = set()
    pending_index = 0
    while pending_index < len(pending):
        name = pending[pending_index]
        pending_index += 1
        if name in visited or name == "run":
            continue
        if (
            GLOBAL_HANDLERS.resolve(
                ast.Attribute(value=ast.Name(id="self"), attr=name), {"self": workflow_cls}
            )
            is not None
        ):
            continue
        visited.add(name)
        method = _find_workflow_method(workflow_cls, name)
        if method is None:
            continue
        parsed = parse_function(method)
        methods.append(parsed)
        pending.extend(_collect_self_method_calls(parsed.node))
    return methods


def parse_function(function: FunctionType, name: str | None = None) -> ParsedFunction:
    """Parse a method once while retaining its lexical scope and original indentation."""
    source_lines, start_line = inspect.getsourcelines(function)
    source = textwrap.dedent("".join(source_lines))
    filename = inspect.getsourcefile(function) or inspect.getfile(function)
    tree = ast.parse(source, filename=filename or "<unknown>")
    parsed_lines = source.splitlines(keepends=True)
    for node in ast.walk(tree):
        if isinstance(node, (ast.stmt, ast.expr, ast.arg, ast.keyword, ast.ExceptHandler)):
            node.col_offset += len(source_lines[node.lineno - 1]) - len(
                parsed_lines[node.lineno - 1]
            )
            if node.end_lineno is not None and node.end_col_offset is not None:
                node.end_col_offset += len(source_lines[node.end_lineno - 1]) - len(
                    parsed_lines[node.end_lineno - 1]
                )
    function_node = tree.body[0]
    assert isinstance(function_node, (ast.FunctionDef, ast.AsyncFunctionDef))
    signature = inspect.signature(function)
    return ParsedFunction(
        name=name if name is not None else function.__name__,
        node=function_node,
        namespace={**function.__globals__, **inspect.getclosurevars(function).nonlocals},
        signature=signature.replace(
            parameters=[
                parameter for key, parameter in signature.parameters.items() if key != "self"
            ]
        ),
        local_names=function.__code__.co_varnames + function.__code__.co_cellvars,
        filename=filename,
        start_line=start_line,
    )


def extract_instance_attrs(workflow_cls: type["Workflow"]) -> InstanceAttrs:
    """Read the `self.attr = value` assignments of the workflow's `__init__`.

    Only an attribute assigned exactly once, at the top level of the method,
    has a value the compiler can read, such as:
        self.retry_policy = RetryPolicy(attempts=3)
        self.timeout = 30

    One assigned twice, or inside an `if`, a loop or a `try`, is recorded as
    ambiguous: the source order would otherwise decide which value wins, so
    a policy that names it is rejected instead.
    """
    init_method = _find_workflow_method(workflow_cls, "__init__")
    if init_method is None:
        return InstanceAttrs({}, set())

    try:
        source_lines, _ = inspect.getsourcelines(init_method)
        source = textwrap.dedent("".join(source_lines))
        tree = ast.parse(source)
    except (OSError, TypeError, SyntaxError):
        return InstanceAttrs({}, set())

    function_def = tree.body[0]
    if not isinstance(function_def, ast.FunctionDef):
        return InstanceAttrs({}, set())

    literals: dict[str, ast.expr] = {}
    ambiguous: set[str] = set()
    for statement in function_def.body:
        top_level = _self_attribute_assignment(statement)
        if top_level is not None:
            name, value = top_level
            if name in literals or name in ambiguous:
                literals.pop(name, None)
                ambiguous.add(name)
            else:
                literals[name] = value
            continue
        for nested in ast.walk(statement):
            nested_assignment = _self_attribute_assignment(nested)
            if nested_assignment is not None:
                literals.pop(nested_assignment[0], None)
                ambiguous.add(nested_assignment[0])

    return InstanceAttrs(
        literals,
        ambiguous,
        {
            **vars(builtins),
            **init_method.__globals__,
            **inspect.getclosurevars(init_method).nonlocals,
        },
    )


def _find_workflow_method(workflow_cls: type["Workflow"], name: str) -> FunctionType | None:
    """Find a workflow method by name across the class MRO."""
    for base in workflow_cls.__mro__:
        if name not in base.__dict__:
            continue
        value = base.__dict__[name]
        if isinstance(value, staticmethod) or isinstance(value, classmethod):
            return value.__func__
        if inspect.isfunction(value):
            return value
    return None


def _collect_self_method_calls(tree: ast.AST) -> list[str]:
    """Collect self.method(...) call names from a parsed function AST in source order."""

    class SelfMethodCallCollector(ast.NodeVisitor):
        def __init__(self) -> None:
            self.calls: list[str] = []
            self._seen: set[str] = set()

        def visit_Call(self, node: ast.Call) -> None:
            func = node.func
            if (
                isinstance(func, ast.Attribute)
                and isinstance(func.value, ast.Name)
                and func.value.id == "self"
                and func.attr not in self._seen
            ):
                self._seen.add(func.attr)
                self.calls.append(func.attr)

            self.generic_visit(node)

    collector = SelfMethodCallCollector()
    collector.visit(tree)
    return collector.calls


def _self_attribute_assignment(node: ast.AST) -> tuple[str, ast.expr] | None:
    """The name and value of a `self.name = value` assignment, annotated or not; else None."""
    if isinstance(node, ast.Assign) and len(node.targets) == 1:
        target, value = node.targets[0], node.value
    elif isinstance(node, ast.AnnAssign) and node.value is not None:
        target, value = node.target, node.value
    else:
        return None
    if (
        isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id == "self"
    ):
        return target.attr, value
    return None
