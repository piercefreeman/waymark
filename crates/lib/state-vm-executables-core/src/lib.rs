//! Core types of the state-vm-executables subsystem: what the executables
//! state hands its consumers for a loaded executable.

#![warn(missing_docs)]

use std::sync::Arc;

/// What describes a loaded executable: the identifier it was loaded by,
/// and the identity it was registered under, as stored alongside it.
#[derive(Debug)]
pub struct ExecutableMetadata<ExecutableId> {
    /// The executable's identifier.
    pub id: ExecutableId,

    /// The name of the workflow the executable was compiled from.
    pub name: String,

    /// The version of the workflow the executable was compiled from.
    pub version: String,
}

/// A loaded executable together with its metadata.
#[derive(Debug)]
pub struct Entry<ExecutableId, Executable> {
    /// The executable's metadata.
    pub metadata: ExecutableMetadata<ExecutableId>,

    /// The deserialized executable.
    pub executable: Arc<Executable>,
}
