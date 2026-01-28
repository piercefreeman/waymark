use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use prost::Message;

use rappel::{ast, convert_to_dag, messages::proto};

#[derive(Debug, Parser)]
#[command(name = "dag-dump")]
#[command(about = "Dump DAG nodes/edges for a workflow registration.")]
struct Args {
    #[arg(long)]
    registration: PathBuf,
    #[arg(long, default_value = "loop_results")]
    variable: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let registration_bytes =
        std::fs::read(&args.registration).context("read registration bytes")?;
    let registration = proto::WorkflowRegistration::decode(registration_bytes.as_slice())
        .context("decode registration")?;
    let program = ast::Program::decode(registration.ir.as_slice()).context("decode program")?;
    let dag = convert_to_dag(&program)
        .map_err(|err| anyhow::anyhow!(err))
        .context("convert to dag")?;

    println!("== Nodes defining/using '{}' ==", args.variable);
    for node in dag.nodes.values() {
        let targets = node.targets.as_ref();
        let has_target = node.target.as_deref() == Some(&args.variable);
        let has_targets = targets.is_some_and(|t| t.iter().any(|v| v == &args.variable));
        if has_target || has_targets {
            println!(
                "{} type={} label={} target={:?} targets={:?}",
                node.id, node.node_type, node.label, node.target, node.targets
            );
        }
    }

    println!("\n== DataFlow edges for '{}' ==", args.variable);
    for edge in dag.edges.iter().filter(|e| {
        e.edge_type == rappel::EdgeType::DataFlow
            && e.variable.as_deref() == Some(args.variable.as_str())
    }) {
        println!("{} -> {}", edge.source, edge.target);
    }

    Ok(())
}
