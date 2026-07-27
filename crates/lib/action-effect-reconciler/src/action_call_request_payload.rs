//! The codec-encoded request payload stored in a request row.

use waymark_action_core::ActionRef;

/// Everything needed to (re)issue an action call, as stored in the
/// `request` blob of a durable request row.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ActionCallRequestPayload<Argument> {
    /// The action to invoke.
    pub action_ref: ActionRef,

    /// The arguments to pass to the action.
    pub arguments: Vec<Argument>,
}
