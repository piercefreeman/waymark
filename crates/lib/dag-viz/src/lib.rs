//! Graphviz rendering for DAGs.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use thiserror::Error;

use waymark_dag::{DAG, DAGEdge, DAGNode, EdgeType};

#[derive(Debug, Error)]
pub enum DagVizError {
    #[error("graphviz executable not found; install Graphviz to render DAG images")]
    MissingGraphviz,
    #[error("failed to render graph: {0}")]
    RenderFailed(String),
}

pub fn build_dag_graph(dag: &DAG) -> String {
    let mut lines: Vec<String> = vec![
        "digraph waymark_dag {".to_string(),
        "  rankdir=LR;".to_string(),
        "  fontname=\"Helvetica\";".to_string(),
        "  fontsize=10;".to_string(),
        "  nodesep=0.35;".to_string(),
        "  ranksep=0.6;".to_string(),
        "  node [shape=box style=\"rounded,filled\" fillcolor=\"#F8F9FA\" color=\"#4B4B4B\" fontname=\"Helvetica\" fontsize=10];".to_string(),
        "  edge [fontname=\"Helvetica\" fontsize=9 color=\"#4B4B4B\"];".to_string(),
    ];

    let mut node_ids: Vec<String> = dag.nodes.keys().cloned().collect();
    node_ids.sort();
    let node_id_map: std::collections::HashMap<String, String> = node_ids
        .iter()
        .enumerate()
        .map(|(idx, node_id)| (node_id.clone(), format!("node_{idx}")))
        .collect();

    for node_id in &node_ids {
        if let Some(node) = dag.nodes.get(node_id) {
            let label = node_label(node);
            let attrs = node_attrs(node);
            let attrs_str = attrs
                .iter()
                .map(|(key, value)| format!("{key}=\"{value}\""))
                .collect::<Vec<String>>()
                .join(" ");
            let mapped_id = node_id_map.get(node_id).unwrap();
            if attrs_str.is_empty() {
                lines.push(format!("  {mapped_id} [label=\"{label}\"];"));
            } else {
                lines.push(format!("  {mapped_id} [label=\"{label}\" {attrs_str}];"));
            }
        }
    }

    for edge in &dag.edges {
        let attrs = edge_attrs(edge);
        let attrs_str = attrs
            .iter()
            .map(|(key, value)| format!("{key}=\"{value}\""))
            .collect::<Vec<String>>()
            .join(" ");
        let source = node_id_map.get(&edge.source);
        let target = node_id_map.get(&edge.target);
        if let (Some(source), Some(target)) = (source, target) {
            if attrs_str.is_empty() {
                lines.push(format!("  {source} -> {target};"));
            } else {
                lines.push(format!("  {source} -> {target} [{attrs_str}];"));
            }
        }
    }

    lines.push("}".to_string());
    lines.join("\n")
}

pub fn render_dag_image(dag: &DAG, output_path: &Path) -> Result<PathBuf, DagVizError> {
    let mut output_path = output_path.to_path_buf();
    let format = output_path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("png")
        .to_string();
    if output_path.extension().is_none() {
        output_path = output_path.with_extension("png");
    }

    let dot = build_dag_graph(dag);

    let mut cmd = Command::new("dot");
    cmd.arg(format!("-T{format}"))
        .arg("-o")
        .arg(&output_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().map_err(|_| DagVizError::MissingGraphviz)?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(dot.as_bytes())
            .map_err(|err| DagVizError::RenderFailed(err.to_string()))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|err| DagVizError::RenderFailed(err.to_string()))?;
    if !output.status.success() {
        return Err(DagVizError::RenderFailed(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }
    Ok(output_path)
}

fn node_label(node: &DAGNode) -> String {
    format!("{}\n{}", node.id(), node.label())
}

fn node_attrs(node: &DAGNode) -> Vec<(String, String)> {
    let mut attrs: Vec<(String, String)> = Vec::new();
    if node.is_input() {
        attrs.push(("shape".to_string(), "oval".to_string()));
        attrs.push(("fillcolor".to_string(), "#E8F5E9".to_string()));
    } else if node.is_output() {
        attrs.push(("shape".to_string(), "oval".to_string()));
        attrs.push(("fillcolor".to_string(), "#FFF3E0".to_string()));
    } else {
        match node.node_type() {
            "action_call" => attrs.push(("fillcolor".to_string(), "#E3F2FD".to_string())),
            "fn_call" => attrs.push(("fillcolor".to_string(), "#E0F7FA".to_string())),
            "parallel" => {
                attrs.push(("shape".to_string(), "diamond".to_string()));
                attrs.push(("fillcolor".to_string(), "#F3E5F5".to_string()));
            }
            "aggregator" => {
                attrs.push(("shape".to_string(), "diamond".to_string()));
                attrs.push(("fillcolor".to_string(), "#FCE4EC".to_string()));
            }
            "branch" => {
                attrs.push(("shape".to_string(), "diamond".to_string()));
                attrs.push(("fillcolor".to_string(), "#FFF9C4".to_string()));
            }
            "join" => {
                attrs.push(("shape".to_string(), "circle".to_string()));
                attrs.push(("fillcolor".to_string(), "#EDE7F6".to_string()));
            }
            "return" => {
                attrs.push(("shape".to_string(), "box".to_string()));
                attrs.push(("fillcolor".to_string(), "#FFF9C4".to_string()));
            }
            "assignment" => attrs.push(("fillcolor".to_string(), "#F8F9FA".to_string())),
            "expression" => attrs.push(("fillcolor".to_string(), "#ECEFF1".to_string())),
            "break" => attrs.push(("fillcolor".to_string(), "#FFEBEE".to_string())),
            "continue" => attrs.push(("fillcolor".to_string(), "#E3F2FD".to_string())),
            "input" | "output" => attrs.push(("fillcolor".to_string(), "#F8F9FA".to_string())),
            _ => attrs.push(("fillcolor".to_string(), "#F8F9FA".to_string())),
        }
    }
    if node.is_spread() {
        attrs.push(("color".to_string(), "#D32F2F".to_string()));
        attrs.push(("penwidth".to_string(), "2".to_string()));
    }
    attrs
}

fn edge_attrs(edge: &DAGEdge) -> Vec<(String, String)> {
    let mut attrs: Vec<(String, String)> = Vec::new();
    if let Some(label) = edge_label(edge) {
        attrs.push(("label".to_string(), label));
    }
    match edge.edge_type {
        EdgeType::DataFlow => {
            attrs.push(("color".to_string(), "#1F77B4".to_string()));
            attrs.push(("style".to_string(), "dashed".to_string()));
        }
        EdgeType::StateMachine => {
            attrs.push(("color".to_string(), "#4B4B4B".to_string()));
        }
    }
    if edge.is_loop_back {
        attrs.push(("color".to_string(), "#D32F2F".to_string()));
        attrs.push(("style".to_string(), "dashed".to_string()));
    }
    attrs
}

fn edge_label(edge: &DAGEdge) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    if edge.edge_type == EdgeType::DataFlow
        && let Some(variable) = &edge.variable
    {
        parts.push(variable.clone());
    }
    if edge.edge_type == EdgeType::StateMachine {
        if let Some(condition) = &edge.condition {
            parts.push(condition.clone());
        }
        if let Some(guard) = &edge.guard_string {
            parts.push(guard.clone());
        }
    }
    if edge.is_loop_back {
        parts.push("loop".to_string());
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\\n"))
    }
}
