//! The promise state id, and the error of a pending value used where a
//! ready one is required.

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

/// A resolved promise was required but the actual value was pending.
///
/// Carries what the pending value holds: a borrow of it, the value's own
/// part, or what an error that outlives the value keeps of it.
#[derive(Debug, thiserror::Error)]
#[error("an unresolved async value is used where a resolved value is expected")]
pub struct UnresolvedPromiseError<Pending> {
    /// What the pending value holds.
    pub pending: Pending,
}

impl<Pending> UnresolvedPromiseError<Pending> {
    /// The same error, carrying `f` applied to what it carries.
    pub fn map<OtherPending>(
        self,
        f: impl FnOnce(Pending) -> OtherPending,
    ) -> UnresolvedPromiseError<OtherPending> {
        UnresolvedPromiseError {
            pending: f(self.pending),
        }
    }
}
