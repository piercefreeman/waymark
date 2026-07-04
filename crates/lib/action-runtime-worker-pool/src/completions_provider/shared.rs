use waymark_action_runtime_core::ActionCallCompletion;
use waymark_convert_core::TryConvert as _;

/// Errors that can occur when resolving a raw completion into an
/// [`ActionCallCompletion`].
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    /// Failed to convert a raw worker-pool result into an
    /// [`waymark_action_runtime_core::ActionCallOutcome`].
    #[error("action result conversion: {0}")]
    ActionResultConversion(
        #[source]
        waymark_convert_core::ConvertErrorFor<
            waymark_action_runtime_convert::Converter,
            waymark_runner_executor_core::UncheckedExecutionResult,
            waymark_action_runtime_core::ActionCallOutcome<waymark_vm_value::ReadyValue>,
        >,
    ),
}

/// Convert a raw worker-pool completion into an [`ActionCallCompletion`]
/// by looking up the dispatch token in the correlation map and converting
/// the result value.
pub(crate) fn resolve_completion<Metadata>(
    completion: waymark_worker_core::ActionCompletion,
    metadata: Metadata,
) -> Result<ActionCallCompletion<waymark_vm_value::ReadyValue, Metadata>, ResolveError> {
    let waymark_worker_core::ActionCompletion {
        executor_id: _,
        execution_id: _,
        attempt_number: _,
        dispatch_token: _,
        result,
    } = completion;

    let outcome = waymark_action_runtime_convert::Converter::try_convert(result)
        .map_err(ResolveError::ActionResultConversion)?;

    Ok(ActionCallCompletion { outcome, metadata })
}
