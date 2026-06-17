use waymark_convert_core::Convert as _;
use waymark_proto::messages as proto;

/// Build a [`waymark_vm_runtime::CallSpec`] for the entry function from a
/// registration's `initial_context` and the entry function's declared input
/// names.
pub(crate) fn build_entry_call_spec(
    initial_context: Option<proto::WorkflowArguments>,
    entry_input_names: &[String],
) -> Result<
    waymark_vm_runtime::CallSpec<
        <waymark_system_vm::Executable as waymark_vm_executable::Functions>::FunctionId,
        waymark_system_vm::Value,
    >,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let args = if entry_input_names.is_empty() {
        Vec::new()
    } else {
        let ctx = initial_context.ok_or_else(|| {
            anyhow::anyhow!(
                "entry function expects {} argument(s) ({}) but no initial_context was provided",
                entry_input_names.len(),
                entry_input_names.join(", "),
            )
        })?;
        waymark_workflow_initialization_convert_proto::InitialContextConverter::convert((
            ctx,
            entry_input_names,
        ))
    };
    Ok(waymark_vm_runtime::CallSpec {
        func: Default::default(),
        args,
    })
}
