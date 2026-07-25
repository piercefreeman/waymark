//! Core types for the extcall subsystem.

#![warn(missing_docs)]

/// Runtime required to execute an action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ActionRuntime {
    /// Execute the action in a Python worker.
    Python,

    /// Execute the action in a JavaScript worker.
    JavaScript,
}

impl std::fmt::Display for ActionRuntime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Python => formatter.write_str("python"),
            Self::JavaScript => formatter.write_str("javascript"),
        }
    }
}

/// Error returned when parsing an unsupported action runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseActionRuntimeError;

impl std::fmt::Display for ParseActionRuntimeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("expected one of: python, javascript")
    }
}

impl std::error::Error for ParseActionRuntimeError {}

impl std::str::FromStr for ActionRuntime {
    type Err = ParseActionRuntimeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "python" => Ok(Self::Python),
            "javascript" => Ok(Self::JavaScript),
            _ => Err(ParseActionRuntimeError),
        }
    }
}

/// Static metadata for an action call.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActionRef {
    /// Runtime required to execute the action.
    pub runtime: ActionRuntime,

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_action_runtimes_without_a_default() {
        assert_eq!("python".parse(), Ok(ActionRuntime::Python));
        assert_eq!("javascript".parse(), Ok(ActionRuntime::JavaScript));
        assert_eq!(
            "Python".parse::<ActionRuntime>(),
            Err(ParseActionRuntimeError)
        );
    }
}
