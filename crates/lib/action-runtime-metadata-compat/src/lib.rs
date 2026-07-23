//! Compatibility adapters bridging [`waymark_action_runtime_core`]
//! transports with the [`waymark_action_runtime_metadata`] shapes.

#![warn(missing_docs)]

use waymark_action_runtime_metadata::{ActionCallCorrelation, WithVmId};

/// [`waymark_action_runtime_core::ActionCallRequester`] adapter that injects
/// a VM id into the correlation metadata of every request.
///
/// Callers keep producing bare [`ActionCallCorrelation`]s; the wrapped
/// requester transports [`WithVmId`]-wrapped metadata, so completions can be
/// routed back to the originating VM.
pub struct WithVmIdActionCallRequester<VmId, ActionCallRequester> {
    /// The VM instance that owns the calls this requester dispatches.
    pub vm_id: VmId,

    /// The wrapped action call requester.
    pub action_call_requester: ActionCallRequester,
}

impl<VmId, ActionCallRequester> waymark_action_runtime_core::ActionCallRequester
    for WithVmIdActionCallRequester<VmId, ActionCallRequester>
where
    ActionCallRequester: waymark_action_runtime_core::ActionCallRequester<
            Metadata = WithVmId<VmId, ActionCallCorrelation>,
        >,
    ActionCallRequester: Sync,
    ActionCallRequester::Argument: Send,
    VmId: Copy + Send + Sync,
{
    type Error = ActionCallRequester::Error;
    type Argument = ActionCallRequester::Argument;
    type Metadata = ActionCallCorrelation;

    async fn request_action_call(
        &self,
        request: waymark_action_runtime_core::ActionCallRequest<Self::Argument, Self::Metadata>,
    ) -> Result<(), Self::Error> {
        let waymark_action_runtime_core::ActionCallRequest {
            action_ref,
            arguments,
            metadata,
        } = request;
        self.action_call_requester
            .request_action_call(waymark_action_runtime_core::ActionCallRequest {
                action_ref,
                arguments,
                metadata: WithVmId {
                    vm_id: self.vm_id,
                    inner: metadata,
                },
            })
            .await
    }
}
