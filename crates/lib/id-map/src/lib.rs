//! A map that mints its own ids and never hands one out twice.

#![warn(missing_docs)]

use std::hash::Hash;

#[cfg(test)]
mod tests;

/// A map keyed by ids it mints itself.
///
/// [`IdMap::insert`] is the only source of ids: every insert hands out the
/// next id and moves past it, so an id whose entry was removed is never
/// handed out again, and a lookup with it finds nothing. The map holds the
/// present entries only.
#[derive(Debug)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(bound(
        serialize = "
            Id: serde::Serialize,
            Entry: serde::Serialize,
        ",
        deserialize = "
            Id: serde::Deserialize<'de> + Eq + Hash,
            Entry: serde::Deserialize<'de>,
        ",
    ))
)]
pub struct IdMap<Id, Entry> {
    /// The id the next insert hands out.
    next_id: Id,

    /// The present entries by their ids.
    entries: indexmap::IndexMap<Id, Entry>,
}

impl<Id, Entry> Default for IdMap<Id, Entry>
where
    Id: index_type::IndexType,
{
    fn default() -> Self {
        Self {
            next_id: Id::ZERO,
            entries: indexmap::IndexMap::new(),
        }
    }
}

impl<Id, Entry> IdMap<Id, Entry>
where
    Id: index_type::IndexType,
{
    /// An empty map, whose first insert hands out the id type's zero.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<Id, Entry> IdMap<Id, Entry>
where
    Id: index_type::IndexType + Hash,
{
    /// Store an entry under a fresh id and hand the id out.
    ///
    /// # Panics
    ///
    /// Panics when the id type has no id left after the one handed out.
    pub fn insert(&mut self, entry: Entry) -> Id {
        let id = self.next_id;

        let next_id = id.checked_add_scalar(<Id::Scalar as index_type::IndexScalarType>::ONE);
        self.next_id = next_id.expect("the id space is exhausted");

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
    pub fn iter(&self) -> impl Iterator<Item = (Id, &Entry)> {
        self.entries.iter().map(|(id, entry)| (*id, entry))
    }
}
