//! Core types for the extcall subsystem.

#![warn(missing_docs)]

/// Static metadata for an action call.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActionRef {
    /// The name of the action to invoke.
    pub action_name: String,

    /// Optional module the action belongs to.
    pub module_name: Option<String>,

    /// Ordered names of the keyword arguments expected by the action.
    pub call_args: Vec<String>,
}
