//! Concrete effect handler for the fullset interpreter.

#![warn(missing_docs)]

use std::sync::Arc;

/// An [`EffectHandler`] type used by the execution effector.
pub type EffectHandler<Backend, Codec, WorkflowWorkflowCompletionValue, ActionCallRequester> =
    waymark_fullset_effect_handler::EffectHandler<
        waymark_workflow_completion::EffectHandler<Backend, Codec, WorkflowWorkflowCompletionValue>,
        waymark_extcall_reconciler::EffectHandler<
            <Backend as waymark_workflow_completion_backend::HasVmId>::VmId,
            ActionCallRequester,
        >,
    >;

/// An [`PromiseSettler`] type used by the execution effector.
pub type PromiseSettler<Backend, ActionCallCompletionsProvider> =
    waymark_extcall_reconciler::PromiseSettler<
        <Backend as waymark_workflow_completion_backend::HasVmId>::VmId,
        ActionCallCompletionsProvider,
    >;

/// Create a paired fullset effect handler and promise settler.
///
/// The returned handler and settler can be combined into a tuple
/// `(handler, settler)` that satisfies both
/// [`waymark_vm_driver_core::EffectHandler`] and
/// [`waymark_vm_driver_core::PromiseSettler`], suitable for passing as the
/// effector to [`waymark_state_vm_runtimes::SpawningFactory`].
///
/// Core effects (`Complete`, `UnhandledException`) are recorded via the
/// provided `backend` under the given `vm_id`.
pub fn new<
    Backend,
    Codec,
    WorkflowWorkflowCompletionValue,
    ActionCallRequester,
    ActionCallCompletionsProvider,
>(
    vm_id: Backend::VmId,
    backend: Arc<Backend>,
    codec: Codec,
    action_call_requester: ActionCallRequester,
    action_call_complations_provider: ActionCallCompletionsProvider,
) -> (
    EffectHandler<Backend, Codec, WorkflowWorkflowCompletionValue, ActionCallRequester>,
    PromiseSettler<Backend, ActionCallCompletionsProvider>,
)
where
    Backend: waymark_workflow_completion_backend::RecordCompletion,
    Backend: waymark_workflow_completion_backend::RecordException,
    Backend: Send + Sync + 'static,
    Backend::VmId: Clone + Send + 'static,
    Codec: Send + 'static,
    ActionCallRequester: waymark_action_runtime_core::ActionCallRequester,
    ActionCallCompletionsProvider: waymark_action_runtime_core::ActionCallCompletionsProvider,
{
    let (extcall_reconciler_handler, extcall_reconciler_settler) = waymark_extcall_reconciler::new(
        vm_id.clone(),
        action_call_requester,
        action_call_complations_provider,
    );
    let completion_handler = waymark_workflow_completion::EffectHandler::new(backend, vm_id, codec);

    let handler = waymark_fullset_effect_handler::EffectHandler {
        core: completion_handler,
        extcall: extcall_reconciler_handler,
    };

    (handler, extcall_reconciler_settler)
}
