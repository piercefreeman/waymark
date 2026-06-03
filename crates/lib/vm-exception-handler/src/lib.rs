//! VM exception handler metadata.

#![warn(missing_docs)]

/// One pushed exception-handler block in lowering/execution order.
pub type ExceptionHandlerBlock<StateId, RegisterId> = Vec<ExceptionHandler<StateId, RegisterId>>;

/// Active exception-handler blocks from outermost to innermost.
pub type ExceptionHandlerBlocks<StateId, RegisterId> =
    Vec<ExceptionHandlerBlock<StateId, RegisterId>>;

/// A catch target for a raised VM exception.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ExceptionHandler<StateId, RegisterId> {
    /// State to transfer control to when this handler matches.
    pub handler_state: StateId,

    /// Exception types handled by this handler.
    ///
    /// An empty list catches all exceptions.
    pub exception_types: Vec<String>,

    /// Optional register to materialize the caught exception into.
    pub exception_dst: Option<RegisterId>,
}

impl<StateId, RegisterId> ExceptionHandler<StateId, RegisterId> {
    /// Returns whether this handler matches the provided exception type.
    pub fn matches(&self, type_id: &str) -> bool {
        self.exception_types.is_empty()
            || self
                .exception_types
                .iter()
                .any(|handled_type| handled_type == type_id)
    }
}
