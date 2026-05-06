/// Unsupported AST constructs or VM capabilities encountered during
/// function compilation.
#[derive(Debug, thiserror::Error)]
pub enum Unsupported {
    /// A statement variant is not compiled yet.
    #[error("statement `{kind}` is not supported by the compiler yet")]
    Statement {
        /// The unsupported statement kind.
        kind: &'static str,
    },

    /// An expression variant is not compiled yet.
    #[error("expression `{kind}` is not supported by the compiler yet")]
    Expression {
        /// The unsupported expression kind.
        kind: &'static str,
    },

    /// A binary operator is not compiled yet.
    #[error("binary operator `{op:?}` is not supported by the compiler yet")]
    BinaryOperator {
        /// The unsupported operator.
        op: waymark_vm_ast_old::BinaryOperator,
    },

    /// A function call shape cannot be represented by the current VM.
    #[error("function call `{name}` is not supported: {reason}")]
    FunctionCall {
        /// The function name.
        name: String,

        /// Why the call shape is unsupported.
        reason: &'static str,
    },

    /// The current VM subset cannot copy a value from one register to another.
    #[error("assignment to `{target}` requires a value-copy instruction that is not implemented")]
    AssignmentNeedsCopy {
        /// The assignment target.
        target: String,
    },

    /// Multiple assignment targets are not supported by the current VM subset.
    #[error("assignment with {count} targets is not supported by the compiler yet")]
    AssignmentTargetCount {
        /// The number of assignment targets.
        count: usize,
    },

    /// A parallel expression assignment shape cannot be represented directly.
    #[error(
        "parallel expression with {call_count} calls and {target_count} assignment targets is not supported: {reason}"
    )]
    ParallelExprAssignment {
        /// The number of assignment targets.
        target_count: usize,

        /// The number of calls inside the parallel expression.
        call_count: usize,

        /// Why the current shape is unsupported.
        reason: &'static str,
    },
}

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
        kind: &'static str,
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

    /// Lowering a literal into the target VM constant type failed.
    #[error("literal lowering failed")]
    LiteralLowering {
        /// The underlying lowering error.
        error: LiteralLoweringError,
    },

    /// Lowering an action call into the target VM extcall identifier failed.
    #[error("lowering action `{action_name}` failed")]
    ActionLowering {
        /// The action name being lowered.
        action_name: String,

        /// The underlying lowering error.
        error: ActionLoweringError,
    },

    /// Compilation encountered an unsupported construct or capability.
    #[error(transparent)]
    Unsupported(#[from] Unsupported),
}
