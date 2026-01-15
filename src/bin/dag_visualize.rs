//! DAG Visualizer - Generate visualization data from a Python workflow file.
//!
//! This binary:
//! 1. Reads a Python workflow file
//! 2. Calls the Python IR generator
//! 3. Parses the IR and converts to DAG
//! 4. Outputs JSON suitable for HTML visualization

use anyhow::{Context, Result};
use base64::Engine;
use clap::Parser;
use prost::Message;
use rappel::{EdgeType, convert_to_dag, ir_ast, ir_printer, parse};
use serde::Serialize;
use std::path::PathBuf;
use std::process::Command;

const DAG_VISUALIZE_TEMPLATE: &str = include_str!("fixtures/dag_visualize.html");

#[derive(Parser)]
#[command(name = "dag-visualize")]
#[command(about = "Generate DAG visualization data from a Python workflow file")]
struct Args {
    /// Path to Python workflow file
    #[arg(required = true)]
    workflow_file: PathBuf,

    /// Output HTML file (if not specified, outputs JSON to stdout)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Path to Python executable (default: python3)
    #[arg(long, default_value = "python3")]
    python: String,
}

#[derive(Serialize)]
struct VisualizationData {
    python_source: String,
    ir_text: String,
    dag: DagData,
}

#[derive(Serialize)]
struct DagData {
    nodes: Vec<NodeData>,
    edges: Vec<EdgeData>,
}

#[derive(Serialize)]
struct NodeData {
    id: String,
    node_type: String,
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    action_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    module_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    targets: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    guard_expr: Option<String>,
    is_input: bool,
    is_output: bool,
    is_aggregator: bool,
    is_spread: bool,
}

#[derive(Serialize)]
struct EdgeData {
    source: String,
    target: String,
    edge_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    condition: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    variable: Option<String>,
    is_loop_back: bool,
}

fn generate_ir(python_path: &str, workflow_file: &PathBuf) -> Result<String> {
    // Get the scripts directory relative to the binary
    let script_path = std::env::current_dir()?.join("scripts/generate_ir.py");

    let output = Command::new(python_path)
        .arg(&script_path)
        .arg(workflow_file)
        .arg("--format")
        .arg("base64")
        .output()
        .context("Failed to execute Python IR generator")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("Python IR generation failed: {}", stderr);
    }

    let base64_ir = String::from_utf8(output.stdout)
        .context("Invalid UTF-8 in IR output")?
        .trim()
        .to_string();

    Ok(base64_ir)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    // Read Python source
    let python_source =
        std::fs::read_to_string(&args.workflow_file).context("Failed to read workflow file")?;

    // Generate IR via Python
    let base64_ir = generate_ir(&args.python, &args.workflow_file)?;

    // Decode and parse IR
    let ir_bytes = base64::engine::general_purpose::STANDARD
        .decode(&base64_ir)
        .context("Failed to decode base64 IR")?;

    // Decode into ir_ast::Program for pretty printing
    let ir_program =
        ir_ast::Program::decode(ir_bytes.as_slice()).context("Failed to decode protobuf IR")?;

    // Pretty print IR
    let ir_text = ir_printer::print_program(&ir_program);

    // Parse the IR text to get parser::ast::Program for DAG conversion
    let program = parse(&ir_text).context("Failed to parse IR text")?;

    // Convert to DAG
    let dag =
        convert_to_dag(&program).map_err(|e| anyhow::anyhow!("Failed to convert to DAG: {}", e))?;

    // Build visualization data
    let mut nodes: Vec<NodeData> = dag
        .nodes
        .values()
        .map(|node| {
            let guard_expr_str = node
                .guard_expr
                .as_ref()
                .map(rappel::ast_printer::print_expr);

            NodeData {
                id: node.id.clone(),
                node_type: node.node_type.clone(),
                label: node.label.clone(),
                action_name: node.action_name.clone(),
                module_name: node.module_name.clone(),
                targets: node.targets.clone(),
                guard_expr: guard_expr_str,
                is_input: node.is_input,
                is_output: node.is_output,
                is_aggregator: node.is_aggregator,
                is_spread: node.is_spread,
            }
        })
        .collect();

    // Sort nodes by ID for consistent output
    nodes.sort_by(|a, b| a.id.cmp(&b.id));

    let edges: Vec<EdgeData> = dag
        .edges
        .iter()
        .map(|edge| EdgeData {
            source: edge.source.clone(),
            target: edge.target.clone(),
            edge_type: match edge.edge_type {
                EdgeType::StateMachine => "control_flow".to_string(),
                EdgeType::DataFlow => "data_flow".to_string(),
            },
            condition: edge.condition.clone(),
            variable: edge.variable.clone(),
            is_loop_back: edge.is_loop_back,
        })
        .collect();

    let viz_data = VisualizationData {
        python_source,
        ir_text,
        dag: DagData { nodes, edges },
    };

    if let Some(output_path) = args.output {
        // Generate HTML
        let html = generate_html(&viz_data)?;
        std::fs::write(&output_path, html).context("Failed to write HTML output")?;
        println!("Visualization written to: {}", output_path.display());
    } else {
        // Output JSON to stdout
        let json = serde_json::to_string_pretty(&viz_data)?;
        println!("{}", json);
    }

    Ok(())
}

fn generate_html(data: &VisualizationData) -> Result<String> {
    let json_data = serde_json::to_string(data)?;

    if !DAG_VISUALIZE_TEMPLATE.contains("__JSON_DATA__") {
        anyhow::bail!("DAG visualize template missing __JSON_DATA__ placeholder");
    }

    Ok(DAG_VISUALIZE_TEMPLATE.replace("__JSON_DATA__", &json_data))
}
