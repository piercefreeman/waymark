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

    /// A function call supplied the wrong number of arguments.
    #[error("function `{function}` expects {expected} arguments but received {actual}")]
    FunctionArityMismatch {
        /// The function name.
        function: String,

        /// The number of inputs the function declares.
        expected: usize,

        /// The number of arguments supplied, positional and keyword combined.
        actual: usize,
    },

    /// A function call passed a keyword argument the callee does not declare.
    #[error("function `{function}` does not declare an input named `{keyword}`")]
    UnknownKeywordArgument {
        /// The function name.
        function: String,

        /// The undeclared keyword argument name.
        keyword: String,
    },

    /// A function call bound the same callee input more than once.
    #[error("function `{function}` received more than one value for input `{input}`")]
    DuplicateFunctionArgument {
        /// The function name.
        function: String,

        /// The input name bound more than once.
        input: String,
    },

    /// A timeout policy duration cannot be represented in bytecode.
    #[error("timeout duration {seconds}s on action `{action_name}` is out of range")]
    TimeoutDurationOutOfRange {
        /// The action the timeout policy is attached to.
        action_name: String,

        /// The out-of-range duration in seconds.
        seconds: u64,
    },

    /// A retry backoff duration cannot be represented in bytecode.
    #[error("backoff duration {seconds}s on action `{action_name}` is out of range")]
    BackoffDurationOutOfRange {
        /// The action the retry policy is attached to.
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
