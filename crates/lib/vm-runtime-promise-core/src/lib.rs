//! Core traits and type for promises support at runtime.

#![warn(missing_docs)]

use index_type::{IndexTooBigError, IndexType};

/// Error returned when a raw index cannot be represented as a [`PromiseStateId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, IndexTooBigError)]
#[index_too_big_error(msg = "promise state id")]
pub struct PromiseStateIdTooBigError;

/// Index of a [`PromiseState`] in the promise states store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType, Hash)]
#[index_type(error = PromiseStateIdTooBigError)]
pub struct PromiseStateId(pub usize);

/// A value for a runtime that supports suspending.
pub trait Suspendable {
    /// Construct a promisable value from a pending value.
    fn from_pending(promise_state_id: PromiseStateId) -> Self;

    /// View the value as a promise state id.
    fn as_pending(&self) -> Option<PromiseStateId>;
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

/// A value type that supports promises.
///
/// Wraps a "resolved" value type with the possibilities of either it being
/// immediately available (i.e. resolved) or being in a placeholder
/// state waiting for a wrapped value to be resolved with.
pub trait Promisable: Suspendable + Resolvable {}

impl<T> Promisable for T where T: Suspendable + Resolvable {}

/// A resolved promise was required but the actual value was pending.
#[derive(Debug, thiserror::Error)]
#[error("an unresolved async value is used where a resolved value is expected")]
pub struct UnresolvedPromiseError {
    /// The ID of the promise state.
    ///
    /// For reconstructing the promise back if needed.
    pub promise_state_id: PromiseStateId,
}
