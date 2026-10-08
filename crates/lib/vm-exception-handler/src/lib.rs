//! VM exception handler metadata.
//!
//! A handler is a record: where to go, what to catch, where to put the
//! caught exception. What it catches is a pattern of a language-defined
//! type - a name, a list, an enum with a catch-all arm, whatever the
//! language matches on - and the record knows nothing about it. The same
//! record serves the bytecode, listing the language's const pattern, and
//! the frame, holding the runtime one.

#![warn(missing_docs)]

/// One pushed exception-handler block in lowering/execution order.
pub type ExceptionHandlerBlock<StateId, RegisterId, Pattern> =
    Vec<ExceptionHandler<StateId, RegisterId, Pattern>>;

/// Active exception-handler blocks from outermost to innermost.
pub type ExceptionHandlerBlocks<StateId, RegisterId, Pattern> =
    Vec<ExceptionHandlerBlock<StateId, RegisterId, Pattern>>;

/// A catch target for a raised VM exception.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ExceptionHandler<StateId, RegisterId, Pattern> {
    /// State to transfer control to when this handler matches.
    pub handler_state: StateId,

    /// What this handler catches.
    pub pattern: Pattern,

    /// Optional register to materialize the caught exception into.
    pub exception_dst: Option<RegisterId>,
}

impl<StateId, RegisterId, Pattern> ExceptionHandler<StateId, RegisterId, Pattern> {
    /// The same handler with its pattern converted.
    pub fn map_pattern<OtherPattern>(
        self,
        f: impl FnOnce(Pattern) -> OtherPattern,
    ) -> ExceptionHandler<StateId, RegisterId, OtherPattern> {
        ExceptionHandler {
            handler_state: self.handler_state,
            pattern: f(self.pattern),
            exception_dst: self.exception_dst,
        }
    }

    /// The same handler over another pattern type, mapped from a borrow of
    /// this one's pattern; the state and destination are cloned. This is
    /// the bytecode's const pattern becoming the runtime one at push.
    pub fn map_pattern_ref_cloned<OtherPattern>(
        &self,
        f: impl FnOnce(&Pattern) -> OtherPattern,
    ) -> ExceptionHandler<StateId, RegisterId, OtherPattern>
    where
        StateId: Clone,
        RegisterId: Clone,
    {
        ExceptionHandler {
            handler_state: self.handler_state.clone(),
            pattern: f(&self.pattern),
            exception_dst: self.exception_dst.clone(),
        }
    }
}
