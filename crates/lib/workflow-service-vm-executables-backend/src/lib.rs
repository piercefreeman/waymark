//! Backend traits for the workflow service VM compiler.
//!
//! Defines the contract for storing compiled VM executables.

#![warn(missing_docs)]

pub mod upsert_executable;

pub use self::upsert_executable::{Error, ErrorKind, UpsertExecutable};

/// Associates the backend with an executable identifier type.
pub trait HasExecutableId {
    /// The executable / workflow version identifier type.
    type ExecutableId;
}
