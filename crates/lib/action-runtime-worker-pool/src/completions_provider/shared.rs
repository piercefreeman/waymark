use uuid::Uuid;
use waymark_action_runtime_core::ActionCallCompletion;
use waymark_convert_core::TryConvert as _;

use crate::DispatchCorrelationMap;

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

    /// A completion arrived with a dispatch token that was not found
    /// in the correlation map.
    #[error("dispatch token not found in correlation map: {0}")]
    DispatchTokenNotFound(Uuid),
}

/// Convert a raw worker-pool completion into an [`ActionCallCompletion`]
/// by looking up the dispatch token in the correlation map and converting
/// the result value.
pub(crate) fn resolve_completion(
    correlation_map: &DispatchCorrelationMap,
    completion: waymark_worker_core::ActionCompletion,
) -> Result<ActionCallCompletion<waymark_vm_value::ReadyValue>, ResolveError> {
    let waymark_worker_core::ActionCompletion {
        executor_id: _,
        execution_id: _,
        attempt_number: _,
        dispatch_token: _,
        result,
    } = completion;

    let outcome = waymark_action_runtime_convert::Converter::try_convert(result)
        .map_err(ResolveError::ActionResultConversion)?;

    let (effect_number, promise_state_id) = {
        let mut map = correlation_map.lock().unwrap();
        map.remove(&completion.dispatch_token)
            .ok_or(ResolveError::DispatchTokenNotFound(
                completion.dispatch_token,
            ))?
    };

    Ok(ActionCallCompletion {
        effect_number,
        promise_state_id,
        outcome,
    })
}
