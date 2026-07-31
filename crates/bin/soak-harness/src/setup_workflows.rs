use std::{num::NonZeroUsize, sync::Arc};

use color_eyre::eyre::eyre;
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use waymark_backend_postgres::PostgresBackend;
use waymark_ids::WorkflowVersionId;
use waymark_ir_parser::parse_program;

const DEFAULT_WORKFLOW_NAME: &str = "waymark_soak_timeout_mix_v1";

/// The workflow services the soak harness submits VMs through.
pub struct SoakServices {
    pub executables: waymark_workflow_service_vm_executables::ExecutablesService<
        PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
        waymark_system_vm::Spec,
        waymark_system_vm::Lowering,
    >,
    pub registration: waymark_workflow_service_vm_runtimes::RegistrationService<
        PostgresBackend,
        waymark_vm_codec_rmp::RmpCodec,
    >,
}

pub fn soak_services(backend: &PostgresBackend) -> SoakServices {
    let codec = waymark_vm_codec_rmp::RmpCodec;
    SoakServices {
        executables: waymark_workflow_service_vm_executables::ExecutablesService::new(
            backend.clone(),
            codec,
        ),
        registration: waymark_workflow_service_vm_runtimes::RegistrationService::new(
            backend.clone(),
            codec,
        ),
    }
}

#[derive(Debug, Clone)]
pub struct RegisteredWorkflow {
    pub workflow_name: String,
    pub workflow_version_id: WorkflowVersionId,
    pub executable: Arc<waymark_system_vm::Executable>,
    pub metadata: waymark_vm_compiler_for_ast_old_core::Metadata,
}

pub async fn register_workflow(
    services: &SoakServices,
    timeout_seconds: u32,
    actions_per_workflow: NonZeroUsize,
    user_module: &str,
) -> Result<RegisteredWorkflow, color_eyre::eyre::Report> {
    let source = workflow_source(user_module, timeout_seconds, actions_per_workflow);

    let program = parse_program(source.trim()).map_err(|err| eyre!(err.to_string()))?;
    let program_proto = program.encode_to_vec();
    let ir_hash = format!("{:x}", Sha256::digest(&program_proto));
    let program = waymark_vm_ast_old_proto::convert(program)
        .map_err(|err| eyre!("convert soak workflow to the VM AST: {err}"))?;

    let (workflow_version_id, executable, metadata) = services
        .executables
        .compile_and_store(DEFAULT_WORKFLOW_NAME, &ir_hash, &program)
        .await
        .map_err(|err| eyre!("compile and store soak workflow: {err}"))?;

    Ok(RegisteredWorkflow {
        workflow_name: DEFAULT_WORKFLOW_NAME.to_string(),
        workflow_version_id,
        executable: Arc::new(executable),
        metadata,
    })
}

fn workflow_source(
    user_module: &str,
    timeout_seconds: u32,
    actions_per_workflow: NonZeroUsize,
) -> String {
    let mut input_names = Vec::with_capacity(actions_per_workflow.get() * 4);
    let mut lines = Vec::with_capacity(actions_per_workflow.get() + 3);
    lines.push("fn main(input: [".to_string());

    for step in 0..actions_per_workflow.get() {
        let idx = step + 1;
        input_names.push(format!("delay_ms_{idx}"));
        input_names.push(format!("should_fail_{idx}"));
        input_names.push(format!("payload_bytes_{idx}"));
        input_names.push(format!("include_payload_{idx}"));
    }

    lines[0].push_str(&input_names.join(", "));
    lines[0].push_str("], output: [result]):");

    for step in 0..actions_per_workflow.get() {
        let idx = step + 1;
        lines.push(format!(
            "    step_{idx} = @{user_module}.simulated_action(delay_ms=delay_ms_{idx}, should_fail=should_fail_{idx}, payload_bytes=payload_bytes_{idx}, include_payload=include_payload_{idx})[ActionTimeout -> retry: 1, backoff: 1 s][timeout: {timeout_seconds} s]"
        ));
    }

    lines.push(format!("    result = step_{actions_per_workflow}"));
    lines.push("    return result".to_string());
    lines.join("\n")
}
