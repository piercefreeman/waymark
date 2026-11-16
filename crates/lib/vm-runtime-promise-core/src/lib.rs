//! Core traits and type for promises support at runtime.
//!
//! Provides abstraction layer necessary to generalize the promise-capable
//! value types.

#![warn(missing_docs)]

use index_type::{IndexTooBigError, IndexType};

/// Error returned when a raw index cannot be represented as a [`PromiseStateId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, IndexTooBigError)]
#[index_too_big_error(msg = "promise state id")]
pub struct PromiseStateIdTooBigError;

/// An opaque identifier of a promise state.
///
/// # Invariant: unique per VM
///
/// An id value belongs to at most one promise over the entire lifetime of
/// the VM that issued it — including snapshot/restore cycles.  External
/// systems durably key their state by promise state id (scoped by the VM
/// identity) and rely on exactly this property.
///
/// How ids are allocated and stored is an implementation concern of the
/// promise states store, free to change as long as the invariant holds.
///
/// Every id is meant to originate in the promise states store, which mints
/// each one. [`PromiseStateId::from_raw`] and deserialization rebuild an id
/// from its number: one a store minted and stored earlier, or one a test
/// fabricates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[index_type(error = PromiseStateIdTooBigError)]
pub struct PromiseStateId(usize);

impl PromiseStateId {
    /// The id with the number `raw`: one read back from where it was
    /// stored, or one a test fabricates. It does not mint an id.
    pub const fn from_raw(raw: usize) -> Self {
        Self(raw)
    }

    /// The id's number, for storing it.
    pub const fn to_raw(self) -> usize {
        self.0
    }
}

/// A value for a runtime that supports resolving with values.
pub trait Resolvable {
    /// A ready value type.
    type ReadyValue;

    /// Construct a promisable value from a ready value.
    fn from_ready(value: Self::ReadyValue) -> Self;

    /// Convert the value into a ready value or get an error if the value
    /// is still pending.
    fn into_ready(self) -> Result<Self::ReadyValue, (UnresolvedPromiseError, Self)>
    where
        Self: Sized;

    /// View the value as mutable ready value or get an error if the value
    /// is still pending.
    fn as_ready(&self) -> Result<&Self::ReadyValue, UnresolvedPromiseError>;

    /// View the value as mutable ready value or get an error if the value
    /// is still pending.
    fn as_ready_mut(&mut self) -> Result<&mut Self::ReadyValue, UnresolvedPromiseError>;
}

/// A resolved promise was required but the actual value was pending.
#[derive(Debug, thiserror::Error)]
#[error("an unresolved async value is used where a resolved value is expected")]
pub struct UnresolvedPromiseError {
    /// The ID of the promise state.
    ///
    /// For reconstructing the promise back if needed.
    pub promise_state_id: PromiseStateId,
}
