use waymark_vm_runtime_core::UnresolvedPromiseError;

/// The error for the [`crate::ExtCallSetInterpreter`].
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invoking an extcall failed.
    #[error("extcall: {0}")]
    ExtCall(#[source] ExtCallError),
}

/// Errors produced while preparing an extcall invocation.
#[derive(Debug, thiserror::Error)]
pub enum ExtCallError {
    /// An extcall argument still held an unresolved promise.
    #[error("unresolved promise argument at position {arg_pos}: {source}")]
    UnresolvedPromiseArgument {
        /// The zero-based argument position that failed to resolve.
        arg_pos: usize,

        /// The underlying unresolved promise error for the argument.
        #[source]
        source: UnresolvedPromiseError,
    },
}
