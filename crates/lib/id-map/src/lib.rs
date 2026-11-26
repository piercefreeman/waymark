//! A map that mints its own ids and never hands one out twice.

#![warn(missing_docs)]

use std::hash::Hash;

#[cfg(feature = "serde")]
mod serde_impl;
#[cfg(test)]
mod tests;

/// An id an [`IdMap`] can mint: a first id, and the id after each one.
///
/// The order is the minting order: [`MintedId::FIRST`] is the least id, and
/// [`MintedId::checked_next`] returns a strictly greater one, so the ids an
/// [`IdMap`] hands out never repeat.
///
/// Implemented for every [`index_type::IndexType`].
pub trait MintedId: Copy + Ord {
    /// The id the first insert into a new map hands out.
    const FIRST: Self;

    /// The id after this one, or `None` when the id type has none left.
    fn checked_next(self) -> Option<Self>;
}

impl<Id> MintedId for Id
where
    Id: index_type::IndexType,
{
    const FIRST: Self = Id::ZERO;

    fn checked_next(self) -> Option<Self> {
        self.checked_add_scalar(<Id::Scalar as index_type::IndexScalarType>::ONE)
            .ok()
    }
}

/// A map keyed by ids it mints itself.
///
/// [`IdMap::insert`] is the only source of ids: every insert hands out the
/// next id and moves past it, so an id whose entry was removed is never
/// handed out again, and a lookup with it finds nothing. The map holds the
/// present entries only.
#[derive(Debug)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize),
    serde(bound(serialize = "
        Id: serde::Serialize,
        Entry: serde::Serialize,
    "))
)]
pub struct IdMap<Id, Entry> {
    /// The id the next insert hands out.
    next_id: Id,

    /// The present entries by their ids.
    entries: indexmap::IndexMap<Id, Entry>,
}

/// Decoding an [`IdMap`] found an entry under an id at or past the map's next
/// id, which a later insert would hand out again.
#[cfg(feature = "serde")]
#[derive(Debug, thiserror::Error)]
#[error("an id map holds an entry under {id:?}, at or past its next id {next_id:?}")]
pub struct IdMapDecodeError<Id> {
    /// The id of the offending entry.
    pub id: Id,

    /// The id the map's next insert would hand out.
    pub next_id: Id,
}

impl<Id, Entry> Default for IdMap<Id, Entry>
where
    Id: MintedId,
{
    fn default() -> Self {
        Self {
            next_id: Id::FIRST,
            entries: indexmap::IndexMap::new(),
        }
    }
}

impl<Id, Entry> IdMap<Id, Entry>
where
    Id: MintedId,
{
    /// An empty map, whose first insert hands out [`MintedId::FIRST`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl<Id, Entry> IdMap<Id, Entry>
where
    Id: MintedId + Eq + Hash,
{
    /// Store an entry under a fresh id and hand the id out.
    ///
    /// # Panics
    ///
    /// Panics when the id type has no id left after the one handed out.
    pub fn insert(&mut self, entry: Entry) -> Id {
        let id = self.next_id;

        self.next_id = id.checked_next().expect("the id space is exhausted");

        let replaced = self.entries.insert(id, entry);
        debug_assert!(replaced.is_none(), "a fresh id was already present");

        id
    }

    /// The entry under the id, if it is present.
    pub fn get(&self, id: Id) -> Option<&Entry> {
        self.entries.get(&id)
    }

    /// The entry under the id for mutation, if it is present.
    pub fn get_mut(&mut self, id: Id) -> Option<&mut Entry> {
        self.entries.get_mut(&id)
    }

    /// Take the entry under the id out of the map, if it is present.
    ///
    /// The id is spent either way: no later insert hands it out again.
    pub fn remove(&mut self, id: Id) -> Option<Entry> {
        self.entries.swap_remove(&id)
    }

    /// The number of present entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no entry is present.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The present entries with their ids.
    ///
    /// The order is deterministic: it follows from the sequence of inserts
    /// and removes alone, and a serde round trip keeps it. It is otherwise
    /// unspecified; a remove can move another entry.
    pub fn iter(&self) -> impl Iterator<Item = (Id, &Entry)> {
        self.entries.iter().map(|(id, entry)| (*id, entry))
    }
}
