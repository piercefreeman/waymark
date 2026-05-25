//! The core types for supporting exceptions at VM runtime.

/// The exception type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exception<Value> {
    /// The exception's type identifier.
    pub type_id: String,

    /// The exception's details payload.
    pub details: Value,
}
