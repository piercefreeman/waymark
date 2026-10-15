//! Persistence operations for VM executables (bytecode).

#![warn(missing_docs)]

/// Common base: every executable backend is associated with an executable
/// identifier type.
pub trait HasExecutableId {
    /// The executable identifier type.
    type ExecutableId;
}

/// A previously-stored executable, as loaded from the backend: the
/// identity it was registered under, and the executable itself.
#[derive(Debug)]
pub struct LoadedExecutable {
    /// The name of the workflow the executable was compiled from.
    pub name: String,

    /// The version of the workflow the executable was compiled from.
    pub version: String,

    /// The serialized executable.
    pub data: Vec<u8>,
}

/// Load a previously-stored executable.
pub trait LoadExecutable: HasExecutableId {
    /// Error type for load operations.
    type Error: std::fmt::Debug;

    /// Load a previously-stored executable.
    fn load_executable<'a>(
        &'a self,
        id: &'a Self::ExecutableId,
    ) -> impl Future<Output = Result<LoadedExecutable, Self::Error>> + Send + 'a;
}
