//! Handles to entries in [`State`](crate::State).

use core::hash::Hash;

use crate::guard::Guard;

/// A handle to an entry in the state.
///
/// See [`State::get`](crate::State::get).
#[must_use]
pub struct Handle<Key, Value>
where
    Key: Eq + Hash,
{
    /// Owns this handle's reference to the entry; its [`Drop`] releases the
    /// reference (and marks the entry for eviction, or removes it, when this
    /// was the last handle).
    guard: Guard<Key, Value>,

    /// The value.
    value: Value,
}

impl<Key, Value> Handle<Key, Value>
where
    Key: Eq + Hash,
{
    /// Wrap a produced `value` together with the `guard` owning its reference.
    pub(crate) fn new(guard: Guard<Key, Value>, value: Value) -> Self {
        Self { guard, value }
    }

    /// Return a ref to the key for this entry.
    pub fn key(this: &Self) -> &Key {
        this.guard.key()
    }

    /// Return a ref to the value for this entry.
    pub fn value(this: &Self) -> &Value {
        &this.value
    }
}

impl<Key, Value> std::ops::Deref for Handle<Key, Value>
where
    Key: Eq + Hash,
{
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
