use waymark_action_core::ActionRef;

use crate::WithActionCallMetadata;

/// A request to dispatch an action call.
#[derive(Debug, Clone)]
pub struct ActionCallRequest<Argument, Metadata> {
    /// The action to invoke.
    pub action_ref: ActionRef,

    /// The arguments to pass to the action.
    pub arguments: Vec<Argument>,

    /// Metadata about the action call.
    pub metadata: Metadata,
}

/// A convenience alias for [`ActionCallRequest`] that infers the `Argument` and
/// `Metadata` type parameters from a provider that implements
/// [`ActionCallRequester`].
pub type ActionCallRequestFor<Reqester> = ActionCallRequest<
    <Reqester as self::ActionCallRequester>::Argument,
    crate::ActionCallMetadataFor<Reqester>,
>;

/// A requester that dispatches action calls.
pub trait ActionCallRequester: WithActionCallMetadata {
    /// The error returned when requesting an action call fails.
    type Error;

    /// The type of the argument passed to the action call.
    type Argument;

    /// Request that an action call be dispatched.
    fn request_action_call(
        &self,
        request: ActionCallRequestFor<Self>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_;
}
