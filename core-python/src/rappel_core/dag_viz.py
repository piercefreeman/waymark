"""Graphviz rendering for DAGs."""

from __future__ import annotations

from pathlib import Path

import graphviz
from graphviz.backend import ExecutableNotFound

from .dag import DAG, DAGEdge, DAGNode, EdgeType, assert_never


def build_dag_graph(dag: DAG) -> graphviz.Digraph:
    graph = graphviz.Digraph("rappel_dag", engine="dot")
    graph.attr(
        rankdir="LR",
        fontname="Helvetica",
        fontsize="10",
        nodesep="0.35",
        ranksep="0.6",
    )
    graph.attr(
        "node",
        shape="box",
        style="rounded,filled",
        fillcolor="#F8F9FA",
        color="#4B4B4B",
        fontname="Helvetica",
        fontsize="10",
    )
    graph.attr(
        "edge",
        fontname="Helvetica",
        fontsize="9",
        color="#4B4B4B",
    )

    for node_id in sorted(dag.nodes):
        node = dag.nodes[node_id]
        graph.node(node.id, label=_node_label(node), **_node_attrs(node))

    for edge in dag.edges:
        attrs = _edge_attrs(edge)
        graph.edge(edge.source, edge.target, **attrs)

    return graph


def render_dag_image(dag: DAG, output_path: Path) -> Path:
    output_path = output_path.resolve()
    graph = build_dag_graph(dag)
    if output_path.suffix:
        graph.format = output_path.suffix.lstrip(".")
    else:
        graph.format = "png"
        output_path = output_path.with_suffix(".png")
    try:
        rendered_path = graph.render(
            filename=output_path.stem,
            directory=str(output_path.parent),
            cleanup=True,
        )
    except ExecutableNotFound as exc:
        raise RuntimeError(
            "graphviz executable not found; install Graphviz to render DAG images"
        ) from exc
    return Path(rendered_path)


def _node_label(node: DAGNode) -> str:
    parts = [node.id, node.label]
    return "\n".join(parts)


def _node_attrs(node: DAGNode) -> dict[str, str]:
    attrs: dict[str, str] = {}
    if node.is_input:
        attrs["shape"] = "oval"
        attrs["fillcolor"] = "#E8F5E9"
    elif node.is_output:
        attrs["shape"] = "oval"
        attrs["fillcolor"] = "#FFF3E0"
    else:
        match node.node_type:
            case "action_call":
                attrs["fillcolor"] = "#E3F2FD"
            case "fn_call":
                attrs["fillcolor"] = "#E0F7FA"
            case "parallel":
                attrs["shape"] = "diamond"
                attrs["fillcolor"] = "#F3E5F5"
            case "aggregator":
                attrs["shape"] = "diamond"
                attrs["fillcolor"] = "#FCE4EC"
            case "branch":
                attrs["shape"] = "diamond"
                attrs["fillcolor"] = "#FFF9C4"
            case "join":
                attrs["shape"] = "circle"
                attrs["fillcolor"] = "#EDE7F6"
            case "return":
                attrs["shape"] = "box"
                attrs["fillcolor"] = "#FFF9C4"
            case "assignment":
                attrs["fillcolor"] = "#F8F9FA"
            case "expression":
                attrs["fillcolor"] = "#ECEFF1"
            case "break":
                attrs["fillcolor"] = "#FFEBEE"
            case "continue":
                attrs["fillcolor"] = "#E3F2FD"
            case "input" | "output":
                attrs["fillcolor"] = "#F8F9FA"
            case _:
                attrs["fillcolor"] = "#F8F9FA"
    if node.is_spread:
        attrs["color"] = "#D32F2F"
        attrs["penwidth"] = "2"
    return attrs


def _edge_attrs(edge: DAGEdge) -> dict[str, str]:
    attrs: dict[str, str] = {}
    label = _edge_label(edge)
    if label:
        attrs["label"] = label
    match edge.edge_type:
        case EdgeType.DATA_FLOW:
            attrs["color"] = "#1F77B4"
            attrs["style"] = "dashed"
        case EdgeType.STATE_MACHINE:
            attrs["color"] = "#4B4B4B"
        case _:
            assert_never(edge.edge_type)
    if edge.is_loop_back:
        attrs["color"] = "#D32F2F"
        attrs["style"] = "dashed"
    return attrs


def _edge_label(edge: DAGEdge) -> str | None:
    parts: list[str] = []
    if edge.edge_type == EdgeType.DATA_FLOW and edge.variable:
        parts.append(edge.variable)
    if edge.edge_type == EdgeType.STATE_MACHINE:
        if edge.condition:
            parts.append(edge.condition)
        if edge.guard_string:
            parts.append(edge.guard_string)
    if edge.is_loop_back:
        parts.append("loop")
    if not parts:
        return None
    return "\\n".join(parts)
