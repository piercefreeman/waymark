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

    /// Timeout in seconds, computed from the action's policies.
    pub timeout_seconds: u32,

    /// Maximum retry attempts (0 means no retries).
    pub max_retries: u32,

    /// Exception types that should trigger a retry.
    pub exception_types: Vec<String>,
}
