use waymark_action_core::ActionRef;

/// A request to dispatch an action call.
#[derive(Debug, Clone)]
pub struct ActionCallRequest<Argument, Metadata> {
    /// The action to invoke.
    pub action_ref: ActionRef,

    /// The arguments to pass to the action.
    pub arguments: Vec<Argument>,

    /// Correlation metadata for routing the completion back to the caller.
    pub metadata: Metadata,
}

/// A requester that dispatches action calls.
pub trait ActionCallRequester {
    /// The error returned when requesting an action call fails.
    type Error: core::fmt::Debug;

    /// The type of the argument passed to the action call.
    type Argument;

    /// The correlation metadata carried by each request.
    type Metadata;

    /// Request that an action call be dispatched.
    fn request_action_call(
        &self,
        request: ActionCallRequest<Self::Argument, Self::Metadata>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + '_;
}
