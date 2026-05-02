use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum LoadFixtureAstOldError {
    #[error("reading workflow AST: {0}")]
    Reader(waymark_python_workflow_ir_reader::ReadWorkflowIrError),

    #[error("converting workflow AST from protobuf: {0}")]
    Conversion(waymark_vm_ast_old_proto::ConvertError),
}

pub async fn load_fixture_ast_old(
    path: &Path,
) -> Result<waymark_vm_ast_old::Program, LoadFixtureAstOldError> {
    let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = crate_root.join("..").join("..").join("..");

    let params = waymark_python_workflow_ir_reader::ReadWorkflowIrParams {
        workflow_file: path,
        workflow_class: None, // autodetect
        workdir: Some(&repo_root),
    };

    let program = waymark_python_workflow_ir_reader::read_workflow_ir(params)
        .await
        .map_err(LoadFixtureAstOldError::Reader)?;

    let program =
        waymark_vm_ast_old_proto::convert(program).map_err(LoadFixtureAstOldError::Conversion)?;

    Ok(program)
}
