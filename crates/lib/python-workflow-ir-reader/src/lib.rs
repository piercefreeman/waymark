//! Tooling to execute a python workflow file and read the workflow IR from it.
//!
//! Intended primarily for preparing the test cases, but may have other uses.

use std::{path::Path, process::Stdio};

use waymark_proto::ast as ir;

#[derive(Debug, thiserror::Error)]
pub enum ReadWorkflowIrError {
    #[error("process spawn: {0}")]
    ProcessSpawn(#[source] std::io::Error),

    #[error("process exited: {}", output.status)]
    ProcessExitFail { output: std::process::Output },

    #[error("protobuf decoding: {0}")]
    ProstDecode(#[source] prost::DecodeError),
}

pub struct ReadWorkflowIrParams<'a> {
    pub workflow_file: &'a Path,
    pub workflow_class: Option<&'a str>,
    pub workdir: Option<&'a Path>,
}

pub async fn read_workflow_ir(
    params: ReadWorkflowIrParams<'_>,
) -> Result<ir::Program, ReadWorkflowIrError> {
    let ReadWorkflowIrParams {
        workflow_file,
        workflow_class,
        workdir,
    } = params;

    let mut command = tokio::process::Command::new("uv");

    command
        .args(["run", "python/scripts/export_workflow_ir.py"])
        // Pass workflow file to read.
        .arg(workflow_file)
        // Write to stddout
        .args(["-o", "-"]);

    if let Some(workflow_class) = workflow_class {
        command.args(["-w", workflow_class]);
    }

    if let Some(workdir) = workdir {
        command.current_dir(workdir);
    }

    command.stdout(Stdio::null());

    let output = command
        .output()
        .await
        .map_err(ReadWorkflowIrError::ProcessSpawn)?;

    if !output.status.success() {
        return Err(ReadWorkflowIrError::ProcessExitFail { output });
    }

    let std::process::Output { stdout, .. } = output;

    let program = <ir::Program as prost::Message>::decode(stdout.as_ref())
        .map_err(ReadWorkflowIrError::ProstDecode)?;

    Ok(program)
}
