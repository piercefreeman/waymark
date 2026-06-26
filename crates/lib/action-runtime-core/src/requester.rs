use waymark_action_core::ActionRef;
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// A request to dispatch an action call.
#[derive(Debug, Clone)]
pub struct ActionCallRequest<Arguments> {
    /// The action to invoke.
    pub action_ref: ActionRef,

    /// The sequential number of the effect that triggered this call.
    pub effect_number: EffectNumber,

    /// The id of a promise state this action fulfills.
    pub promise_state_id: PromiseStateId,

    /// The arguments to pass to the action.
    pub arguments: Arguments,
}

/// A requester that dispatches action calls.
pub trait ActionCallRequester {
    /// The error returned when requesting an action call fails.
    type Error;

    /// The type of the arguments passed to the action call.
    type Arguments;

    /// Request that an action call be dispatched.
    fn request_action_call(
        &self,
        request: ActionCallRequest<Self::Arguments>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_;
}
