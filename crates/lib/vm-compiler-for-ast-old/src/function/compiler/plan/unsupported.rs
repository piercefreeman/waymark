//! Unsupported-construct reporting.

use std::num::NonZeroUsize;

pub use super::{
    call::UnsupportedFunctionCall, parallel::UnsupportedParallelExprAssignment,
    statement::UnsupportedStatementKind,
};

/// Unsupported AST constructs or VM capabilities encountered during
/// function compilation.
#[derive(Debug, thiserror::Error)]
pub enum Unsupported {
    /// A statement variant is not compiled yet.
    #[error("statement `{kind}` is not supported by the compiler yet")]
    Statement {
        /// The unsupported statement kind.
        kind: UnsupportedStatementKind,
    },

    /// A parallel expression appeared outside the assignment planner.
    #[error("parallel expressions are only supported on the right-hand side of assignments")]
    ParallelExprOutsideAssignment,

    /// A spread expression appeared outside the assignment planner.
    #[error("spread expressions are only supported on the right-hand side of assignments")]
    SpreadExprOutsideAssignment,

    /// A function call shape cannot be represented by the current VM.
    #[error("function call `{name}` is not supported: {reason}")]
    FunctionCall {
        /// The function name.
        name: String,

        /// Why the call shape is unsupported.
        reason: UnsupportedFunctionCall,
    },

    /// Zero-target assignments are only supported for side-effect spread forms.
    #[error("assignment with no targets is only supported for spread expressions")]
    AssignmentNoTargets,

    /// Multiple assignment targets are not supported by the current VM subset.
    #[error("assignment with {count} targets is not supported by the compiler yet")]
    AssignmentTargetCount {
        /// The number of assignment targets.
        count: NonZeroUsize,
    },

    /// A retry policy bracket is not compiled yet.
    #[error("retry policy on action `{action_name}` is not supported by the compiler yet")]
    RetryPolicy {
        /// The action the policy is attached to.
        action_name: String,
    },

    /// A timeout policy bracket is not compiled yet.
    #[error("timeout policy on action `{action_name}` is not supported by the compiler yet")]
    TimeoutPolicy {
        /// The action the policy is attached to.
        action_name: String,
    },

    /// A parallel expression assignment shape cannot be represented directly.
    #[error(
        "parallel expression with {call_count} calls and {target_count} assignment targets is not supported: {reason}"
    )]
    ParallelExprAssignment {
        /// The number of assignment targets.
        target_count: NonZeroUsize,

        /// The number of calls inside the parallel expression.
        call_count: usize,

        /// Why the current shape is unsupported.
        reason: UnsupportedParallelExprAssignment,
    },
}
