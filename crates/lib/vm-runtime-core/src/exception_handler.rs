use crate::RegisterId;

/// A catch target for a raised VM exception.
#[derive(Clone, PartialEq, Eq)]
pub struct ExceptionHandler<StateId> {
    /// State to transfer control to when this handler matches.
    pub handler_state: StateId,

    /// Exception types handled by this handler.
    ///
    /// An empty list catches all exceptions.
    pub exception_types: Vec<String>,

    /// Optional register to materialize the caught exception into.
    pub exception_dst: Option<RegisterId>,
}

impl<StateId: core::fmt::Debug> core::fmt::Debug for ExceptionHandler<StateId> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut debug = f.debug_struct("ExceptionHandler");
        debug.field("handler_state", &self.handler_state);
        debug.field("exception_types", &self.exception_types);
        if let Some(exception_dst) = self.exception_dst {
            debug.field("exception_dst", &exception_dst);
        }
        debug.finish()
    }
}

impl<StateId> ExceptionHandler<StateId> {
    /// Returns whether this handler matches the provided exception type.
    pub fn matches(&self, type_id: &str) -> bool {
        self.exception_types.is_empty()
            || self
                .exception_types
                .iter()
                .any(|handled_type| handled_type == type_id)
    }
}
