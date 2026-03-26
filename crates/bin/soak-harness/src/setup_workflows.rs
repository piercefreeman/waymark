use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use uuid::Uuid;
use waymark_backend_postgres::PostgresBackend;
use waymark_dag::DAG;
use waymark_dag_builder::convert_to_dag;
use waymark_ir_parser::parse_program;
use waymark_workflow_registry_backend::{WorkflowRegistration, WorkflowRegistryBackend as _};

const DEFAULT_WORKFLOW_NAME: &str = "waymark_soak_timeout_mix_v1";

#[derive(Debug, Clone)]
pub struct RegisteredWorkflow {
    pub workflow_name: String,
    pub workflow_version_id: Uuid,
    pub dag: Arc<DAG>,
    pub entry_template_id: String,
}

pub async fn register_workflow(
    backend: &PostgresBackend,
    timeout_seconds: u32,
    actions_per_workflow: usize,
    user_module: &str,
) -> Result<RegisteredWorkflow> {
    let source = workflow_source(user_module, timeout_seconds, actions_per_workflow);

    let program = parse_program(source.trim()).map_err(|err| anyhow!(err.to_string()))?;
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let dag = Arc::new(convert_to_dag(&program).map_err(|err| anyhow!(err.to_string()))?);
    let entry_template_id = dag
        .entry_node
        .clone()
        .ok_or_else(|| anyhow!("compiled workflow has no entry node"))?;

    let workflow_version_id = backend
        .upsert_workflow_version(&WorkflowRegistration {
            workflow_name: DEFAULT_WORKFLOW_NAME.to_string(),
            workflow_version: ir_hash.clone(),
            ir_hash,
            program_proto,
            concurrent: false,
        })
        .await
        .context("upsert soak workflow version")?;

    Ok(RegisteredWorkflow {
        workflow_name: DEFAULT_WORKFLOW_NAME.to_string(),
        workflow_version_id,
        dag,
        entry_template_id,
    })
}

fn workflow_source(user_module: &str, timeout_seconds: u32, actions_per_workflow: usize) -> String {
    let mut input_names = Vec::with_capacity(actions_per_workflow * 3);
    let mut lines = Vec::with_capacity(actions_per_workflow + 3);
    lines.push("fn main(input: [".to_string());

    for step in 0..actions_per_workflow {
        let idx = step + 1;
        input_names.push(format!("delay_ms_{idx}"));
        input_names.push(format!("should_fail_{idx}"));
        input_names.push(format!("payload_bytes_{idx}"));
    }

    lines[0].push_str(&input_names.join(", "));
    lines[0].push_str("], output: [result]):");

    for step in 0..actions_per_workflow {
        let idx = step + 1;
        lines.push(format!(
            "    step_{idx} = @{user_module}.simulated_action(delay_ms=delay_ms_{idx}, should_fail=should_fail_{idx}, payload_bytes=payload_bytes_{idx})[ActionTimeout -> retry: 1, backoff: 1 s][timeout: {timeout_seconds} s]"
        ));
    }

    lines.push(format!("    result = step_{actions_per_workflow}"));
    lines.push("    return result".to_string());
    lines.join("\n")
}
