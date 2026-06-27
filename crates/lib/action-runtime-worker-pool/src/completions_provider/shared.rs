use waymark_action_runtime_core::ActionCallCompletion;
use waymark_convert_core::Convert as _;

use crate::DispatchCorrelationMap;

/// Convert a raw worker-pool completion into an [`ActionCallCompletion`]
/// by looking up the dispatch token in the correlation map and converting
/// the result value.
pub(crate) fn resolve_completion(
    correlation_map: &DispatchCorrelationMap,
    completion: waymark_worker_core::ActionCompletion,
) -> Option<ActionCallCompletion<waymark_vm_value::ReadyValue>> {
    let waymark_worker_core::ActionCompletion {
        executor_id: _,
        execution_id: _,
        attempt_number: _,
        dispatch_token,
        result,
    } = completion;

    let outcome = waymark_action_runtime_convert::Converter::convert(result);

    let correlation = {
        let mut map = correlation_map.lock().unwrap();
        map.remove(&dispatch_token)
    };

    let Some((effect_number, promise_state_id)) = correlation else {
        tracing::warn!(?dispatch_token, "unable to correlation action completion");
        return None;
    };

    Some(ActionCallCompletion {
        effect_number,
        promise_state_id,
        outcome,
    })
}
