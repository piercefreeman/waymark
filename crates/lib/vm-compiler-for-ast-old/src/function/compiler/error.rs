//! Public error surface for function compilation.

pub use super::r#loop::LoopControlKind;
pub use super::plan::unsupported::*;

/// Errors produced while compiling an individual function body.
#[derive(Debug, thiserror::Error)]
pub enum Error<LiteralLoweringError, ActionLoweringError> {
    /// Two inputs inside one function shared the same name.
    #[error("function `{function}` declares duplicate input `{name}`")]
    DuplicateInput {
        /// The function that owns the duplicate input.
        function: String,

        /// The duplicate input name.
        name: String,
    },

    /// A variable was referenced before being assigned or passed as an input.
    #[error("unknown variable `{name}`")]
    UnknownVariable {
        /// The missing variable name.
        name: String,
    },

    /// A loop-only control statement appeared outside of any loop body.
    #[error("{kind} statement outside of a loop")]
    LoopControlOutsideLoop {
        /// The control statement kind.
        kind: LoopControlKind,
    },

    /// A user-defined function call referred to an unknown function name.
    #[error("unknown function `{name}`")]
    UnknownFunction {
        /// The missing function name.
        name: String,
    },

    /// A function call used the wrong number of positional arguments.
    #[error("function `{function}` expects {expected} positional arguments but received {actual}")]
    FunctionArityMismatch {
        /// The function name.
        function: String,

        /// The expected positional arity.
        expected: usize,

        /// The provided positional arity.
        actual: usize,
    },

    /// A timeout policy duration cannot be represented in bytecode.
    #[error("timeout duration {seconds}s on action `{action_name}` is out of range")]
    TimeoutDurationOutOfRange {
        /// The action the timeout policy is attached to.
        action_name: String,

        /// The out-of-range duration in seconds.
        seconds: u64,
    },

    /// Lowering a literal into the target VM constant type failed.
    #[error("literal lowering failed")]
    LiteralLowering(#[source] LiteralLoweringError),

    /// Lowering an action call into the target VM action reference failed.
    #[error("lowering action `{action_name}` failed")]
    ActionLowering {
        /// The action name being lowered.
        action_name: String,

        /// The underlying lowering error.
        #[source]
        error: ActionLoweringError,
    },

    /// Compilation encountered an unsupported construct or capability.
    #[error(transparent)]
    Unsupported(#[from] Unsupported),
}
