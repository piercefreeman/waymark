//! Persistence operations for VM executables (bytecode).

#![warn(missing_docs)]

/// Common base: every executable backend is associated with an executable
/// identifier type.
pub trait HasExecutableId {
    /// The executable identifier type.
    type ExecutableId;
}

/// Load a previously-stored executable.
pub trait LoadExecutable: HasExecutableId {
    /// Error type for load operations.
    type Error: std::fmt::Debug;

    /// Load a previously-stored executable.
    fn load_executable<'a>(
        &'a self,
        id: &'a Self::ExecutableId,
    ) -> impl Future<Output = Result<Vec<u8>, Self::Error>> + Send + 'a;
}
