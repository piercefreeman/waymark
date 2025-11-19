from __future__ import annotations

import hashlib
import inspect
import logging
import os
import sys
from functools import wraps
from threading import RLock
from typing import Any, ClassVar, Dict, Optional, Sequence, TextIO, TypeVar

from proto import messages_pb2 as pb2

from . import bridge
from .actions import deserialize_result_payload
from .formatter import Formatter, supports_color
from .workflow_dag import DagNode, WorkflowDag, build_workflow_dag

logger = logging.getLogger(__name__)

TWorkflow = TypeVar("TWorkflow", bound="Workflow")


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
    def _build_registration_payload(cls) -> pb2.WorkflowRegistration:
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
            if node.guard:
                proto_node.guard = node.guard
            for edge in node.exception_edges:
                proto_edge = proto_node.exception_edges.add()
                proto_edge.source_node_id = edge.source_node_id
                if edge.exception_type:
                    proto_edge.exception_type = edge.exception_type
                if edge.exception_module:
                    proto_edge.exception_module = edge.exception_module
            dag_definition.nodes.append(proto_node)
        if dag.return_variable:
            dag_definition.return_variable = dag.return_variable
        dag_bytes = dag_definition.SerializeToString()
        dag_hash = hashlib.sha256(dag_bytes).hexdigest()
        message = pb2.WorkflowRegistration(
            workflow_name=cls.short_name(),
            dag=dag_definition,
            dag_hash=dag_hash,
        )
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
        payload = cls._build_registration_payload()
        run_result = await bridge.run_instance(payload.SerializeToString())
        cls._workflow_version_id = run_result.workflow_version_id
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
        return result.result

    cls.__workflow_run_impl__ = run_impl
    cls.run = run_public  # type: ignore[assignment]
    workflow_registry.register(cls.short_name(), cls)
    return cls


def _running_under_pytest() -> bool:
    return bool(os.environ.get("PYTEST_CURRENT_TEST"))
