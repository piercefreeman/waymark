import ast
import hashlib
import inspect
import os
import sys
from dataclasses import dataclass
from datetime import timedelta
from functools import wraps
from threading import RLock
from typing import Any, Awaitable, ClassVar, Dict, Optional, Sequence, TextIO, TypeVar

from proto import messages_pb2 as pb2

from . import bridge
from .actions import deserialize_result_payload
from .formatter import Formatter, supports_color
from .logger import configure as configure_logger
from .serialization import build_arguments_from_kwargs
from .workflow_dag import (
    DagNode,
    EdgeType,
    LoopHeadMeta,
    LoopSpec,
    MultiActionLoopSpec,
    NodeType,
    WorkflowDag,
    build_workflow_dag,
)
from .workflow_runtime import WorkflowNodeResult

logger = configure_logger("rappel.workflow")

TWorkflow = TypeVar("TWorkflow", bound="Workflow")
TResult = TypeVar("TResult")


@dataclass(frozen=True)
class RetryPolicy:
    attempts: Optional[int] = None


class BackoffPolicy:
    """Base class for backoff policies. Use LinearBackoff or ExponentialBackoff."""

    def to_proto(self) -> pb2.BackoffPolicy:
        raise NotImplementedError("Subclasses must implement to_proto")


@dataclass(frozen=True)
class LinearBackoff(BackoffPolicy):
    """Linear backoff: delay = base_delay_ms * attempt_number"""

    base_delay_ms: int = 1000

    def to_proto(self) -> pb2.BackoffPolicy:
        return pb2.BackoffPolicy(linear=pb2.LinearBackoff(base_delay_ms=self.base_delay_ms))


@dataclass(frozen=True)
class ExponentialBackoff(BackoffPolicy):
    """Exponential backoff: delay = base_delay_ms * multiplier^(attempt_number - 1)"""

    base_delay_ms: int = 1000
    multiplier: float = 2.0

    def to_proto(self) -> pb2.BackoffPolicy:
        return pb2.BackoffPolicy(
            exponential=pb2.ExponentialBackoff(
                base_delay_ms=self.base_delay_ms, multiplier=self.multiplier
            )
        )


class Workflow:
    """Base class for workflow definitions."""

    name: ClassVar[Optional[str]] = None
    """Human-friendly identifier. Override to pin the registry key; defaults to lowercase class name."""

    concurrent: ClassVar[bool] = False
    """When True, downstream engines may respect DAG-parallel execution; False preserves sequential semantics which
    is what you would see if you were to run the code directly in a Python process."""

    _workflow_dag: ClassVar[Optional[WorkflowDag]] = None
    _dag_lock: ClassVar[RLock] = RLock()
    _workflow_version_id: ClassVar[Optional[str]] = None

    async def run(self) -> Any:
        raise NotImplementedError

    async def run_action(
        self,
        awaitable: Awaitable[TResult],
        *,
        retry: Optional[RetryPolicy] = None,
        backoff: Optional[BackoffPolicy] = None,
        timeout: Optional[float | int | timedelta] = None,
    ) -> TResult:
        """Helper that simply awaits the provided action coroutine.

        The retry, backoff, and timeout arguments are consumed by the workflow
        compiler rather than the runtime execution path.
        """

        # Parameters are intentionally unused at runtime; the workflow compiler
        # inspects the AST to record them.
        del retry, backoff, timeout
        return await awaitable

    @classmethod
    def short_name(cls) -> str:
        if cls.name:
            return cls.name
        return cls.__name__.lower()

    @classmethod
    def workflow_dag(cls) -> WorkflowDag:
        if cls._workflow_dag is None:
            with cls._dag_lock:
                if cls._workflow_dag is None:
                    cls._workflow_dag = build_workflow_dag(cls)
        return cls._workflow_dag

    @classmethod
    def _build_registration_payload(
        cls, initial_context: Optional[pb2.WorkflowArguments] = None
    ) -> pb2.WorkflowRegistration:
        dag = cls.workflow_dag()
        dag_definition = pb2.WorkflowDagDefinition(concurrent=cls.concurrent)
        for node in dag.nodes:
            proto_node = pb2.WorkflowDagNode(
                id=node.id,
                action=node.action,
                module=node.module or "",
                depends_on=list(node.depends_on),
                wait_for_sync=list(node.wait_for_sync),
                produces=list(node.produces),
            )
            proto_node.kwargs.update(node.kwargs)
            if node.loop:
                proto_node.loop.iterable_expr = node.loop.iterable_expr
                proto_node.loop.loop_var = node.loop.loop_var
                proto_node.loop.accumulator = node.loop.accumulator
                proto_node.loop.preamble = node.loop.preamble or ""
                proto_node.loop.body_action = node.loop.body_action
                if node.loop.body_module:
                    proto_node.loop.body_module = node.loop.body_module
                proto_node.loop.body_kwargs.update(node.loop.body_kwargs)
            if node.guard:
                proto_node.guard = node.guard
            _populate_node_ast(proto_node, node)
            if node.timeout_seconds is not None:
                proto_node.timeout_seconds = node.timeout_seconds
            if node.max_retries is not None:
                proto_node.max_retries = node.max_retries
            if node.backoff is not None:
                proto_node.backoff.CopyFrom(node.backoff.to_proto())
            for edge in node.exception_edges:
                proto_edge = proto_node.exception_edges.add()
                proto_edge.source_node_id = edge.source_node_id
                if edge.exception_type:
                    proto_edge.exception_type = edge.exception_type
                if edge.exception_module:
                    proto_edge.exception_module = edge.exception_module
            # Cycle-based loop support
            proto_node.node_type = _node_type_to_proto(node.node_type)
            if node.loop_head_meta:
                proto_node.loop_head_meta.CopyFrom(_loop_head_meta_to_proto(node.loop_head_meta))
            if node.loop_id:
                proto_node.loop_id = node.loop_id
            dag_definition.nodes.append(proto_node)
        # Add explicit edges
        for edge in dag.edges:
            proto_edge = pb2.DagEdge(
                from_node=edge.from_node,
                to_node=edge.to_node,
                edge_type=_edge_type_to_proto(edge.edge_type),
            )
            dag_definition.edges.append(proto_edge)
        if dag.return_variable:
            dag_definition.return_variable = dag.return_variable
        dag_bytes = dag_definition.SerializeToString()
        dag_hash = hashlib.sha256(dag_bytes).hexdigest()
        message = pb2.WorkflowRegistration(
            workflow_name=cls.short_name(),
            dag=dag_definition,
            dag_hash=dag_hash,
        )
        if initial_context:
            message.initial_context.CopyFrom(initial_context)
        return message

    @classmethod
    def visualize(cls, *, stream: Optional[TextIO] = None) -> str:
        """Render the cached DAG into a readable ASCII summary."""

        dag = cls.workflow_dag()
        target = stream or sys.stdout
        enable_colors = supports_color(target)
        formatter = Formatter(enable_colors=enable_colors)
        renderer = WorkflowDagVisualizer(cls, dag, formatter)
        return renderer.write(target)


class WorkflowRegistry:
    """Registry of workflow definitions keyed by workflow name."""

    def __init__(self) -> None:
        self._workflows: Dict[str, type[Workflow]] = {}
        self._lock = RLock()

    def register(self, name: str, workflow_cls: type[Workflow]) -> None:
        with self._lock:
            if name in self._workflows:
                raise ValueError(f"workflow '{name}' already registered")
            self._workflows[name] = workflow_cls

    def get(self, name: str) -> Optional[type[Workflow]]:
        with self._lock:
            return self._workflows.get(name)

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._workflows.keys())

    def reset(self) -> None:
        with self._lock:
            self._workflows.clear()


workflow_registry = WorkflowRegistry()


class WorkflowDagVisualizer:
    """Pretty-printer for workflow DAG metadata."""

    def __init__(
        self,
        workflow_cls: type["Workflow"],
        dag: WorkflowDag,
        formatter: Formatter,
    ) -> None:
        self._workflow_cls = workflow_cls
        self._dag = dag
        self._formatter = formatter
        self._meta_label_width = 12
        self._detail_label_width = 12
        self._nodes_by_id = {node.id: node for node in dag.nodes}
        self._adjacency, self._parents = self._build_adjacency()
        self._style_map: Dict[str, Sequence[str]] = {
            "meta_heading": ("bold",),
            "section_heading": ("bold",),
            "border": ("cyan",),
            "id": ("bold",),
            "action": ("green",),
            "module": ("magenta",),
            "connector": ("yellow",),
            "details_header": ("bold",),
            "placeholder": ("dim",),
        }

    def render(self) -> str:
        lines = self._build_lines()
        return "\n".join(lines).rstrip()

    def write(self, stream: TextIO) -> str:
        output = self.render()
        if output:
            stream.write(f"{output}\n")
        else:
            stream.write("")
        return output

    def _build_lines(self) -> list[str]:
        workflow_cls = self._workflow_cls
        dag = self._dag
        meta_lines = [
            (f"Workflow: {workflow_cls.__module__}.{workflow_cls.__name__}", "meta_heading"),
            (f"{'Short name':<{self._meta_label_width}}: {workflow_cls.short_name()}", None),
            (
                f"{'Concurrent':<{self._meta_label_width}}: {'yes' if workflow_cls.concurrent else 'no'}",
                None,
            ),
            (f"{'Return var':<{self._meta_label_width}}: {dag.return_variable or '<none>'}", None),
            (f"{'Total nodes':<{self._meta_label_width}}: {len(dag.nodes)}", None),
        ]
        lines: list[str] = [self._styled_line(text, style) for text, style in meta_lines]
        lines.append("")
        lines.append(self._styled_line("Graph:", "section_heading"))
        if not dag.nodes:
            lines.append(self._styled_line("  <none>"))
            return lines
        lines.append("")
        visited: set[str] = set()
        start_nodes = [node.id for node in dag.nodes if not self._parents[node.id]]
        if not start_nodes:
            start_nodes = [dag.nodes[0].id]
        for idx, node_id in enumerate(start_nodes):
            if idx:
                lines.append("")
            lines.extend(self._render_graph_node(node_id, prefix="", visited=visited))
        for node in dag.nodes:
            if node.id not in visited:
                lines.append("")
                lines.extend(self._render_graph_node(node.id, prefix="", visited=visited))
        lines.append("")
        lines.append(self._styled_line("Details:", "section_heading"))
        lines.append("")
        for idx, node in enumerate(dag.nodes):
            lines.extend(self._format_node_details(node))
            if idx < len(dag.nodes) - 1:
                lines.append("")
        return lines

    def _render_graph_node(self, node_id: str, prefix: str, visited: set[str]) -> list[str]:
        node = self._nodes_by_id[node_id]
        lines: list[str] = []
        box = self._build_box_lines(node)
        already_rendered = node_id in visited
        for kind, line in box:
            lines.append(self._styled_line(f"{prefix}{line}", kind))
        if already_rendered:
            lines.append(self._styled_line(f"{prefix}(see above)", "module"))
            return lines
        visited.add(node_id)
        children = self._adjacency.get(node_id, [])
        for idx, child_id in enumerate(children):
            branch_prefix = prefix + "    "
            connector = "└──▶" if idx == len(children) - 1 else "├──▶"
            lines.append(self._styled_line(f"{branch_prefix}{connector}", "connector"))
            child_prefix = prefix + ("    " if idx == len(children) - 1 else "│   ")
            lines.extend(self._render_graph_node(child_id, child_prefix, visited))
        return lines

    def _format_node_details(self, node: DagNode) -> list[str]:
        header = f"{node.id}: {node.action}"
        if node.module:
            header = f"{header} [{node.module}]"
        lines = [self._styled_line(header, "details_header")]
        indent = "    "
        detail_specs = [
            ("depends_on", self._format_sequence(node.depends_on)),
            ("wait_for", self._format_sequence(node.wait_for_sync)),
            ("produces", self._format_sequence(node.produces)),
            ("guard", node.guard or "-"),
        ]
        for label, value in detail_specs:
            detail_text, is_placeholder = self._format_detail(indent, label, value)
            lines.append(self._styled_line(detail_text, "placeholder" if is_placeholder else None))
        for detail_line, placeholder in self._format_kwargs(indent, node.kwargs):
            lines.append(self._styled_line(detail_line, "placeholder" if placeholder else None))
        return lines

    def _format_detail(self, indent: str, label: str, value: str) -> tuple[str, bool]:
        placeholder = value.strip() == "-"
        return f"{indent}{label:<{self._detail_label_width}}: {value}", placeholder

    def _format_kwargs(self, indent: str, kwargs: Dict[str, Any]) -> list[tuple[str, bool]]:
        if not kwargs:
            text, placeholder = self._format_detail(indent, "kwargs", "-")
            return [(text, placeholder)]
        lines: list[tuple[str, bool]] = [
            (f"{indent}{'kwargs':<{self._detail_label_width}}:", False)
        ]
        for key in sorted(kwargs):
            rendered = self._stringify_value(kwargs[key])
            parts = rendered.splitlines() or ["-"]
            first = parts[0] or "-"
            placeholder = first.strip() == "-"
            lines.append((f"{indent}  - {key}: {first}", placeholder))
            continuation = f"{indent}    "
            for part in parts[1:]:
                cont_placeholder = part.strip() == "-"
                lines.append((continuation + part, cont_placeholder))
        return lines

    def _build_box_lines(self, node: DagNode) -> list[tuple[str, str]]:
        label_entries: list[tuple[str, str]] = [
            ("id", node.id),
            ("action", node.action),
        ]
        if node.module:
            label_entries.append(("module", f"[{node.module}]"))
        width = max(len(entry[1]) for entry in label_entries)
        horizontal = "+" + "-" * (width + 2) + "+"
        lines: list[tuple[str, str]] = [("border", horizontal)]
        for kind, label in label_entries:
            lines.append((kind, f"| {label.ljust(width)} |"))
        lines.append(("border", horizontal))
        return lines

    def _styled_line(self, text: str, style_key: Optional[str] = None) -> str:
        styles = self._style_map.get(style_key or "", ())
        if not styles:
            return self._formatter.format(text)
        styled_text = self._formatter.apply_styles(text, styles)
        return self._formatter.format(styled_text)

    def _build_adjacency(self) -> tuple[Dict[str, list[str]], Dict[str, set[str]]]:
        adjacency: Dict[str, list[str]] = {node.id: [] for node in self._dag.nodes}
        parents: Dict[str, set[str]] = {node.id: set() for node in self._dag.nodes}
        for node in self._dag.nodes:
            for dep in list(dict.fromkeys(node.depends_on + node.wait_for_sync)):
                if dep not in adjacency:
                    continue
                if node.id not in adjacency[dep]:
                    adjacency[dep].append(node.id)
                parents[node.id].add(dep)
        return adjacency, parents

    @staticmethod
    def _format_sequence(values: list[str]) -> str:
        return ", ".join(values) if values else "-"

    @staticmethod
    def _stringify_value(value: Any) -> str:
        if isinstance(value, (list, tuple, set)):
            items = [str(item) for item in value]
            return ", ".join(items) if items else "-"
        if value is None:
            return "-"
        text = str(value)
        return text if text else "-"


def workflow(cls: type[TWorkflow]) -> type[TWorkflow]:
    """Decorator that registers workflow classes and caches their DAGs."""

    if not issubclass(cls, Workflow):
        raise TypeError("workflow decorator requires Workflow subclasses")
    run_impl = cls.run
    if not inspect.iscoroutinefunction(run_impl):
        raise TypeError("workflow run() must be defined with 'async def'")

    @wraps(run_impl)
    async def run_public(self: Workflow, *args: Any, **kwargs: Any) -> Any:
        if _running_under_pytest():
            cls.workflow_dag()
            return await run_impl(self, *args, **kwargs)

        # Get the signature of run() to map positional args to parameter names
        sig = inspect.signature(run_impl)
        params = list(sig.parameters.keys())[1:]  # Skip 'self'

        # Convert positional args to kwargs
        for i, arg in enumerate(args):
            if i < len(params):
                kwargs[params[i]] = arg

        # Serialize kwargs using common logic
        initial_context = build_arguments_from_kwargs(kwargs)

        payload = cls._build_registration_payload(initial_context)
        run_result = await bridge.run_instance(payload.SerializeToString())
        cls._workflow_version_id = run_result.workflow_version_id
        if _skip_wait_for_instance():
            logger.info(
                "Skipping wait_for_instance for workflow %s due to CARABINER_SKIP_WAIT_FOR_INSTANCE",
                cls.short_name(),
            )
            return None
        result_bytes = await bridge.wait_for_instance(
            instance_id=run_result.workflow_instance_id,
            poll_interval_secs=1.0,
        )
        if result_bytes is None:
            raise TimeoutError(
                f"workflow instance {run_result.workflow_instance_id} did not complete"
            )
        arguments = pb2.WorkflowArguments()
        arguments.ParseFromString(result_bytes)
        result = deserialize_result_payload(arguments)
        if result.error:
            raise RuntimeError(f"workflow failed: {result.error}")

        # Unwrap WorkflowNodeResult if present (internal worker representation)
        if isinstance(result.result, WorkflowNodeResult):
            # Extract the actual result from the variables dict
            variables = result.result.variables
            dag = cls.workflow_dag()
            if dag.return_variable and dag.return_variable in variables:
                return variables[dag.return_variable]
            # If no return variable or key missing, this might be an empty workflow
            return None

        return result.result

    cls.__workflow_run_impl__ = run_impl
    cls.run = run_public  # type: ignore[assignment]
    workflow_registry.register(cls.short_name(), cls)
    return cls


def _populate_node_ast(proto_node: pb2.WorkflowDagNode, node: DagNode) -> None:
    ast_payload = pb2.WorkflowNodeAst()
    # Python block kwargs contain full statements; skip kwargs AST emission for those nodes.
    # However, we still need to emit guards and other control-flow AST fields.
    if node.action != "python_block":
        for key, expr_text in node.kwargs.items():
            expr_proto = _parse_expr(expr_text)
            if expr_proto is None:
                continue
            ast_payload.kwargs[key].CopyFrom(expr_proto)
    if node.guard:
        guard_expr = _parse_expr(node.guard)
        if guard_expr is not None:
            ast_payload.guard.CopyFrom(guard_expr)
    if node.loop:
        loop_ast = pb2.LoopAst(
            loop_var=node.loop.loop_var,
            accumulator=node.loop.accumulator,
        )
        iterable_expr = _parse_expr(node.loop.iterable_expr)
        if iterable_expr is not None:
            loop_ast.iterable.CopyFrom(iterable_expr)
        if node.loop.preamble:
            for stmt in _parse_preamble(node.loop.preamble):
                loop_ast.preamble.append(stmt)
        action_ast = _build_action_ast(node.loop)
        if action_ast is not None:
            loop_ast.body_action.CopyFrom(action_ast)
        ast_payload.loop.CopyFrom(loop_ast)
    if node.multi_action_loop:
        loop_ast = _build_multi_action_loop_ast(node.multi_action_loop)
        ast_payload.loop.CopyFrom(loop_ast)
    if node.sleep_duration_expr:
        sleep_expr = _parse_expr(node.sleep_duration_expr)
        if sleep_expr is not None:
            ast_payload.sleep_duration.CopyFrom(sleep_expr)
    has_ast_content = (
        ast_payload.kwargs
        or ast_payload.HasField("guard")
        or ast_payload.HasField("loop")
        or ast_payload.HasField("sleep_duration")
    )
    if has_ast_content:
        proto_node.ast.CopyFrom(ast_payload)


def _build_action_ast(loop_spec: "LoopSpec") -> Optional[pb2.ActionAst]:
    action_name = loop_spec.body_action
    module_name = loop_spec.body_module or ""
    if not action_name:
        return None
    action_ast = pb2.ActionAst(action_name=action_name, module_name=module_name)
    for key, expr_text in loop_spec.body_kwargs.items():
        expr_proto = _parse_expr(expr_text)
        if expr_proto is None:
            continue
        keyword = action_ast.keywords.add()
        keyword.arg = key
        keyword.value.CopyFrom(expr_proto)
    return action_ast


def _build_multi_action_loop_ast(loop_spec: "MultiActionLoopSpec") -> pb2.LoopAst:
    """Build a LoopAst proto from a MultiActionLoopSpec with body_graph."""
    loop_ast = pb2.LoopAst(
        loop_var=loop_spec.loop_var,
        accumulator=loop_spec.accumulator,
    )

    # Parse iterable expression
    iterable_expr = _parse_expr(loop_spec.iterable_expr)
    if iterable_expr is not None:
        loop_ast.iterable.CopyFrom(iterable_expr)

    # Parse preamble if present
    if loop_spec.preamble:
        for stmt in _parse_preamble(loop_spec.preamble):
            loop_ast.preamble.append(stmt)

    # Build body graph
    body_graph = pb2.LoopBodyGraph(
        result_variable=loop_spec.body_graph.result_variable,
    )

    for body_node in loop_spec.body_graph.nodes:
        proto_node = pb2.LoopBodyNode(
            id=body_node.id,
            action=body_node.action,
            module=body_node.module or "",
            output_var=body_node.output_var,
        )

        # Add dependencies
        for dep in body_node.depends_on:
            proto_node.depends_on.append(dep)

        # Add kwargs as Keyword protos
        for key, expr_text in body_node.kwargs.items():
            expr_proto = _parse_expr(expr_text)
            if expr_proto is None:
                continue
            keyword = proto_node.kwargs.add()
            keyword.arg = key
            keyword.value.CopyFrom(expr_proto)

        # Add timeout/retry config if present
        if body_node.timeout_seconds is not None:
            proto_node.timeout_seconds = body_node.timeout_seconds
        if body_node.max_retries is not None:
            proto_node.max_retries = body_node.max_retries

        body_graph.nodes.append(proto_node)

    loop_ast.body_graph.CopyFrom(body_graph)
    return loop_ast


def _parse_preamble(snippet: str) -> list[pb2.Stmt]:
    try:
        tree = ast.parse(snippet)
    except SyntaxError:
        return []
    statements: list[pb2.Stmt] = []
    for stmt in tree.body:
        stmt_proto = _stmt_to_proto(stmt)
        if stmt_proto is not None:
            statements.append(stmt_proto)
    return statements


def _parse_expr(expr_text: str) -> Optional[pb2.Expr]:
    text = expr_text.strip()
    if not text:
        return None
    try:
        parsed = ast.parse(text, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"unsupported expression syntax: {expr_text!r}") from exc
    expr_proto = _expr_to_proto(parsed.body)
    if expr_proto is None:
        raise ValueError(f"unsupported expression structure: {expr_text!r}")
    return expr_proto


def _stmt_to_proto(stmt: ast.stmt) -> Optional[pb2.Stmt]:
    if isinstance(stmt, ast.Assign):
        assign = pb2.Assign()
        for target in stmt.targets:
            target_expr = _expr_to_proto(target)
            if target_expr is None:
                continue
            assign.targets.append(target_expr)
        value_expr = _expr_to_proto(stmt.value)
        if value_expr is None:
            return None
        assign.value.CopyFrom(value_expr)
        wrapper = pb2.Stmt()
        wrapper.assign.CopyFrom(assign)
        return wrapper
    if isinstance(stmt, ast.Expr):
        expr_proto = _expr_to_proto(stmt.value)
        if expr_proto is None:
            return None
        wrapper = pb2.Stmt()
        wrapper.expr.CopyFrom(expr_proto)
        return wrapper
    if isinstance(stmt, ast.For):
        # Handle for loops in preamble
        if not isinstance(stmt.target, ast.Name):
            return None  # Only support simple loop variables
        target_expr = _expr_to_proto(stmt.target)
        iter_expr = _expr_to_proto(stmt.iter)
        if target_expr is None or iter_expr is None:
            return None
        body_stmts = []
        for body_stmt in stmt.body:
            body_proto = _stmt_to_proto(body_stmt)
            if body_proto is not None:
                body_stmts.append(body_proto)
        for_proto = pb2.For()
        for_proto.target.CopyFrom(target_expr)
        for_proto.iter.CopyFrom(iter_expr)
        for_proto.body.extend(body_stmts)
        wrapper = pb2.Stmt()
        wrapper.for_stmt.CopyFrom(for_proto)
        return wrapper
    if isinstance(stmt, ast.AugAssign):
        # Handle augmented assignment (e.g., x += 1)
        target_expr = _expr_to_proto(stmt.target)
        value_expr = _expr_to_proto(stmt.value)
        if target_expr is None or value_expr is None:
            return None
        op_map = {
            ast.Add: pb2.BinOpKind.BIN_OP_KIND_ADD,
            ast.Sub: pb2.BinOpKind.BIN_OP_KIND_SUB,
            ast.Mult: pb2.BinOpKind.BIN_OP_KIND_MULT,
            ast.Div: pb2.BinOpKind.BIN_OP_KIND_DIV,
            ast.Mod: pb2.BinOpKind.BIN_OP_KIND_MOD,
            ast.FloorDiv: pb2.BinOpKind.BIN_OP_KIND_FLOORDIV,
            ast.Pow: pb2.BinOpKind.BIN_OP_KIND_POW,
        }
        op = op_map.get(type(stmt.op), pb2.BinOpKind.BIN_OP_KIND_UNSPECIFIED)
        aug_proto = pb2.AugAssign()
        aug_proto.target.CopyFrom(target_expr)
        aug_proto.op = op
        aug_proto.value.CopyFrom(value_expr)
        wrapper = pb2.Stmt()
        wrapper.aug_assign.CopyFrom(aug_proto)
        return wrapper
    return None


def _expr_to_proto(expr: ast.AST) -> Optional[pb2.Expr]:
    if isinstance(expr, ast.JoinedStr):
        parts: list[pb2.Expr] = []
        for value in expr.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                part = pb2.Expr(constant=pb2.Constant(string_value=value.value))
            elif isinstance(value, ast.FormattedValue):
                inner = _expr_to_proto(value.value)
                if inner is None:
                    return None
                part = pb2.Expr(
                    call=pb2.Call(
                        func=pb2.Expr(name=pb2.Name(id="str")),
                        args=[inner],
                        keywords=[],
                    )
                )
            else:
                return None
            parts.append(part)
        if not parts:
            return None
        expr_proto = parts[0]
        for part in parts[1:]:
            expr_proto = pb2.Expr(
                bin_op=pb2.BinOp(
                    left=expr_proto,
                    op=pb2.BinOpKind.BIN_OP_KIND_ADD,
                    right=part,
                )
            )
        return expr_proto
    if isinstance(expr, ast.Name):
        return pb2.Expr(name=pb2.Name(id=expr.id))
    if isinstance(expr, ast.Constant):
        const = pb2.Constant()
        if expr.value is None:
            const.is_none = True
        elif isinstance(expr.value, bool):
            const.bool_value = expr.value
        elif isinstance(expr.value, int):
            const.int_value = expr.value
        elif isinstance(expr.value, float):
            const.float_value = expr.value
        elif isinstance(expr.value, str):
            const.string_value = expr.value
        else:
            return None
        return pb2.Expr(constant=const)
    if isinstance(expr, ast.Attribute):
        value = _expr_to_proto(expr.value)
        if value is None:
            return None
        return pb2.Expr(attribute=pb2.Attribute(value=value, attr=expr.attr))
    if isinstance(expr, ast.Subscript):
        value = _expr_to_proto(expr.value)
        if value is None:
            return None
        slice_expr = _expr_to_proto(expr.slice) if isinstance(expr.slice, ast.AST) else None
        if slice_expr is None:
            return None
        return pb2.Expr(subscript=pb2.Subscript(value=value, slice=slice_expr))
    if isinstance(expr, ast.BinOp):
        left = _expr_to_proto(expr.left)
        right = _expr_to_proto(expr.right)
        if left is None or right is None:
            return None
        op = _BIN_OP_KIND.get(type(expr.op))
        if op is None:
            return None
        return pb2.Expr(bin_op=pb2.BinOp(left=left, op=op, right=right))
    if isinstance(expr, ast.BoolOp):
        values = [_expr_to_proto(v) for v in expr.values]
        if any(v is None for v in values):
            return None
        op = _BOOL_OP_KIND.get(type(expr.op))
        if op is None:
            return None
        return pb2.Expr(bool_op=pb2.BoolOp(op=op, values=[v for v in values if v is not None]))
    if isinstance(expr, ast.Compare):
        left = _expr_to_proto(expr.left)
        comps = [_expr_to_proto(c) for c in expr.comparators]
        if left is None or any(c is None for c in comps):
            return None
        ops: list[pb2.CmpOpKind] = []
        for op in expr.ops:
            mapped = _CMP_OP_KIND.get(type(op))
            if mapped is None:
                return None
            ops.append(mapped)
        return pb2.Expr(
            compare=pb2.Compare(
                left=left,
                ops=ops,
                comparators=[c for c in comps if c is not None],
            )
        )
    if isinstance(expr, ast.Call):
        func_expr = _expr_to_proto(expr.func)
        if func_expr is None:
            return None
        args: list[pb2.Expr] = []
        for arg in expr.args:
            proto_arg = _expr_to_proto(arg)
            if proto_arg is None:
                return None
            args.append(proto_arg)
        keywords: list[pb2.Keyword] = []
        for kw in expr.keywords:
            if kw.arg is None:
                return None
            kw_expr = _expr_to_proto(kw.value)
            if kw_expr is None:
                return None
            keyword = pb2.Keyword(arg=kw.arg, value=kw_expr)
            keywords.append(keyword)
        return pb2.Expr(call=pb2.Call(func=func_expr, args=args, keywords=keywords))
    if isinstance(expr, ast.List):
        elts = [_expr_to_proto(e) for e in expr.elts]
        if any(e is None for e in elts):
            return None
        return pb2.Expr(list=pb2.List(elts=[e for e in elts if e is not None]))
    if isinstance(expr, ast.Tuple):
        elts = [_expr_to_proto(e) for e in expr.elts]
        if any(e is None for e in elts):
            return None
        return pb2.Expr(tuple=pb2.Tuple(elts=[e for e in elts if e is not None]))
    if isinstance(expr, ast.Dict):
        keys: list[pb2.Expr] = []
        values: list[pb2.Expr] = []
        for key, value in zip(expr.keys, expr.values, strict=False):
            if key is None:
                return None
            key_expr = _expr_to_proto(key)
            value_expr = _expr_to_proto(value)
            if key_expr is None or value_expr is None:
                return None
            keys.append(key_expr)
            values.append(value_expr)
        return pb2.Expr(dict=pb2.Dict(keys=keys, values=values))
    if isinstance(expr, ast.UnaryOp):
        operand = _expr_to_proto(expr.operand)
        if operand is None:
            return None
        op = _UNARY_OP_KIND.get(type(expr.op))
        if op is None:
            return None
        return pb2.Expr(unary_op=pb2.UnaryOp(op=op, operand=operand))
    return None


_BIN_OP_KIND: dict[type[ast.AST], pb2.BinOpKind] = {
    ast.Add: pb2.BinOpKind.BIN_OP_KIND_ADD,
    ast.Sub: pb2.BinOpKind.BIN_OP_KIND_SUB,
    ast.Mult: pb2.BinOpKind.BIN_OP_KIND_MULT,
    ast.Div: pb2.BinOpKind.BIN_OP_KIND_DIV,
    ast.Mod: pb2.BinOpKind.BIN_OP_KIND_MOD,
    ast.FloorDiv: pb2.BinOpKind.BIN_OP_KIND_FLOORDIV,
    ast.Pow: pb2.BinOpKind.BIN_OP_KIND_POW,
}

_BOOL_OP_KIND: dict[type[ast.AST], pb2.BoolOpKind] = {
    ast.And: pb2.BoolOpKind.BOOL_OP_KIND_AND,
    ast.Or: pb2.BoolOpKind.BOOL_OP_KIND_OR,
}

_CMP_OP_KIND: dict[type[ast.AST], pb2.CmpOpKind] = {
    ast.Eq: pb2.CmpOpKind.CMP_OP_KIND_EQ,
    ast.NotEq: pb2.CmpOpKind.CMP_OP_KIND_NOT_EQ,
    ast.Lt: pb2.CmpOpKind.CMP_OP_KIND_LT,
    ast.LtE: pb2.CmpOpKind.CMP_OP_KIND_LT_E,
    ast.Gt: pb2.CmpOpKind.CMP_OP_KIND_GT,
    ast.GtE: pb2.CmpOpKind.CMP_OP_KIND_GT_E,
    ast.In: pb2.CmpOpKind.CMP_OP_KIND_IN,
    ast.NotIn: pb2.CmpOpKind.CMP_OP_KIND_NOT_IN,
    ast.Is: pb2.CmpOpKind.CMP_OP_KIND_IS,
    ast.IsNot: pb2.CmpOpKind.CMP_OP_KIND_IS_NOT,
}

_UNARY_OP_KIND: dict[type[ast.AST], pb2.UnaryOpKind] = {
    ast.USub: pb2.UnaryOpKind.UNARY_OP_KIND_USUB,
    ast.UAdd: pb2.UnaryOpKind.UNARY_OP_KIND_UADD,
    ast.Not: pb2.UnaryOpKind.UNARY_OP_KIND_NOT,
}


def _running_under_pytest() -> bool:
    return bool(os.environ.get("PYTEST_CURRENT_TEST"))


def _skip_wait_for_instance() -> bool:
    value = os.environ.get("CARABINER_SKIP_WAIT_FOR_INSTANCE")
    if not value:
        return False
    return value.strip().lower() not in {"0", "false", "no"}


def _node_type_to_proto(node_type: str) -> pb2.NodeType:
    """Convert NodeType string to proto enum."""
    if node_type == NodeType.LOOP_HEAD:
        return pb2.NodeType.NODE_TYPE_LOOP_HEAD
    return pb2.NodeType.NODE_TYPE_ACTION


def _edge_type_to_proto(edge_type: str) -> pb2.EdgeType:
    """Convert EdgeType string to proto enum."""
    mapping = {
        EdgeType.FORWARD: pb2.EdgeType.EDGE_TYPE_FORWARD,
        EdgeType.CONTINUE: pb2.EdgeType.EDGE_TYPE_CONTINUE,
        EdgeType.BREAK: pb2.EdgeType.EDGE_TYPE_BREAK,
        EdgeType.BACK: pb2.EdgeType.EDGE_TYPE_BACK,
    }
    return mapping.get(edge_type, pb2.EdgeType.EDGE_TYPE_FORWARD)


def _loop_head_meta_to_proto(meta: LoopHeadMeta) -> pb2.LoopHeadMeta:
    """Convert LoopHeadMeta to proto message."""
    proto_meta = pb2.LoopHeadMeta(
        iterator_source=meta.iterator_source,
        loop_var=meta.loop_var,
        body_entry=list(meta.body_entry),
        body_tail=meta.body_tail,
        exit_target=meta.exit_target,
        body_nodes=list(meta.body_nodes),
    )
    # Add accumulators
    for acc in meta.accumulators:
        proto_acc = pb2.AccumulatorSpec(var=acc.var)
        if acc.source_node:
            proto_acc.source_node = acc.source_node
        if acc.source_expr:
            proto_acc.source_expr = acc.source_expr
        proto_meta.accumulators.append(proto_acc)
    # Add preamble operations
    for op in meta.preamble:
        proto_op = pb2.PreambleOp()
        if op.op_type == "set_iterator_index":
            proto_op.set_iterator_index.CopyFrom(pb2.SetIteratorIndex(var=op.var))
        elif op.op_type == "set_iterator_value":
            proto_op.set_iterator_value.CopyFrom(pb2.SetIteratorValue(var=op.var))
        elif op.op_type == "set_accumulator_len":
            proto_op.set_accumulator_len.CopyFrom(
                pb2.SetAccumulatorLen(var=op.var, accumulator=op.accumulator or "")
            )
        proto_meta.preamble.append(proto_op)
    return proto_meta
