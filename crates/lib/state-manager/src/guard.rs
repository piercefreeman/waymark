//! The ref-count [`Guard`] for entries in [`State`](crate::State).

use core::hash::Hash;
use std::sync::Weak;

use crate::storage::Maps;

/// Owns one reference to an entry in the state.
///
/// A `Guard` is created by [`State::get`](crate::State::get) the moment it
/// acquires an entry's reference — before the factory runs — and its [`Drop`]
/// releases that reference.  This makes the acquire and its release a single
/// unit regardless of how `get` ends:
///
/// * the `get` future is cancelled at its `.await`, or the factory errors:
///   the `Guard` is dropped directly and the reference is released;
/// * `get` succeeds: the `Guard` is moved into the returned
///   [`Handle`](crate::Handle), and the reference is released when the last
///   handle is dropped.
pub(crate) struct Guard<Key, Value>
where
    Key: Eq + Hash,
{
    maps: Weak<Maps<Key, Value>>,

    /// The guarded key.
    ///
    /// SAFETY: safety invariants depend on this field, which is why it is
    /// private to this module — nothing outside can take or replace it.  It is
    /// `Some` for the whole life of the guard and is taken only in [`Drop`] (so
    /// the key can be moved into the storage without cloning).
    key: Option<Key>,
}

impl<Key, Value> Guard<Key, Value>
where
    Key: Eq + Hash,
{
    /// Create a guard owning the caller's freshly-acquired reference to `key`.
    pub(crate) fn new(maps: Weak<Maps<Key, Value>>, key: Key) -> Self {
        Self {
            maps,
            key: Some(key),
        }
    }

    /// The key of the entry whose reference this guard owns.
    pub(crate) fn key(&self) -> &Key {
        // SAFETY: `key` is `Some` for the whole life of the guard — it is taken
        // only in `Drop`, during which no `&self` method can run.
        unsafe { self.key.as_ref().unwrap_unchecked() }
    }
}

impl<Key, Value> Drop for Guard<Key, Value>
where
    Key: Eq + Hash,
{
    fn drop(&mut self) {
        let Some(maps) = self.maps.upgrade() else {
            // Maps are gone; nothing to release.
            return;
        };

        // SAFETY: `key` is `Some` for the whole life of the guard, is taken
        // only here, and `drop` runs at most once.
        let key = unsafe { self.key.take().unwrap_unchecked() };

        maps.release(key);
    }
}
