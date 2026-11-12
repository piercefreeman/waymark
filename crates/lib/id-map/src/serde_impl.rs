//! Reading an [`IdMap`] back, with a seed per entry.

use std::hash::Hash;
use std::marker::PhantomData;

use serde::de::{DeserializeSeed, Deserializer, Error as _, MapAccess, SeqAccess, Visitor};

use crate::{IdMap, IdMapDecodeError};

/// The field keys of an [`IdMap`], passed to `deserialize_struct`.
const FIELDS: &[&str] = &["next_id", "entries"];

/// A field of an [`IdMap`] read under its key.
#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum Field {
    /// The id the next insert hands out.
    NextId,

    /// The present entries by their ids.
    Entries,
}

impl<Id, Entry> IdMap<Id, Entry> {
    /// Read back a map its `Serialize` wrote, building each entry through the
    /// seed `seed_for` returns for that entry's id.
    ///
    /// Each entry is read once, straight into the map. Fails with an
    /// [`IdMapDecodeError`] when an entry's id is at or past the map's next
    /// id.
    pub fn deserialize_with<'de, D, Seed, SeedFor>(
        deserializer: D,
        seed_for: SeedFor,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
        Id: serde::Deserialize<'de> + Copy + Ord + Hash + core::fmt::Debug,
        SeedFor: FnMut(Id) -> Seed,
        Seed: DeserializeSeed<'de, Value = Entry>,
    {
        deserializer.deserialize_struct(
            "IdMap",
            FIELDS,
            IdMapVisitor {
                seed_for,
                phantom: PhantomData,
            },
        )
    }
}

impl<'de, Id, Entry> serde::Deserialize<'de> for IdMap<Id, Entry>
where
    Id: serde::Deserialize<'de> + Copy + Ord + Hash + core::fmt::Debug,
    Entry: serde::Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::deserialize_with(deserializer, |_| PhantomData::<Entry>)
    }
}

/// The checked map, or the error carrying the first id at or past `next_id`.
fn checked<Id, Entry, E>(
    next_id: Id,
    entries: indexmap::IndexMap<Id, Entry>,
) -> Result<IdMap<Id, Entry>, E>
where
    Id: Copy + Ord + core::fmt::Debug,
    E: serde::de::Error,
{
    if let Some(id) = entries.keys().find(|id| **id >= next_id) {
        return Err(E::custom(IdMapDecodeError { id: *id, next_id }));
    }

    Ok(IdMap { next_id, entries })
}

/// Reads an [`IdMap`] as a sequence (positional formats) or as a map.
struct IdMapVisitor<Id, Entry, SeedFor> {
    /// The seed source for each entry.
    seed_for: SeedFor,

    /// The map's id and entry types.
    phantom: PhantomData<fn() -> (Id, Entry)>,
}

impl<'de, Id, Entry, Seed, SeedFor> Visitor<'de> for IdMapVisitor<Id, Entry, SeedFor>
where
    Id: serde::Deserialize<'de> + Copy + Ord + Hash + core::fmt::Debug,
    SeedFor: FnMut(Id) -> Seed,
    Seed: DeserializeSeed<'de, Value = Entry>,
{
    type Value = IdMap<Id, Entry>;

    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
        formatter.write_str("an id map")
    }

    fn visit_seq<A>(mut self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let next_id = seq
            .next_element()?
            .ok_or_else(|| A::Error::invalid_length(0, &self))?;
        let entries = seq
            .next_element_seed(EntriesSeed {
                seed_for: &mut self.seed_for,
                phantom: PhantomData,
            })?
            .ok_or_else(|| A::Error::invalid_length(1, &self))?;

        checked(next_id, entries)
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut next_id = None;
        let mut entries = None;

        while let Some(field) = map.next_key::<Field>()? {
            match field {
                Field::NextId => {
                    if next_id.is_some() {
                        return Err(A::Error::duplicate_field("next_id"));
                    }
                    next_id = Some(map.next_value()?);
                }
                Field::Entries => {
                    if entries.is_some() {
                        return Err(A::Error::duplicate_field("entries"));
                    }
                    entries = Some(map.next_value_seed(EntriesSeed {
                        seed_for: &mut self.seed_for,
                        phantom: PhantomData,
                    })?);
                }
            }
        }

        let next_id = next_id.ok_or_else(|| A::Error::missing_field("next_id"))?;
        let entries = entries.ok_or_else(|| A::Error::missing_field("entries"))?;

        checked(next_id, entries)
    }
}

/// Reads an [`IdMap`]'s entries, each through the seed for its id.
struct EntriesSeed<'a, Id, Entry, SeedFor> {
    /// The seed source for each entry.
    seed_for: &'a mut SeedFor,

    /// The map's id and entry types.
    phantom: PhantomData<fn() -> (Id, Entry)>,
}

impl<'de, Id, Entry, Seed, SeedFor> DeserializeSeed<'de> for EntriesSeed<'_, Id, Entry, SeedFor>
where
    Id: serde::Deserialize<'de> + Hash + Eq + Copy,
    SeedFor: FnMut(Id) -> Seed,
    Seed: DeserializeSeed<'de, Value = Entry>,
{
    type Value = indexmap::IndexMap<Id, Entry>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

impl<'de, Id, Entry, Seed, SeedFor> Visitor<'de> for EntriesSeed<'_, Id, Entry, SeedFor>
where
    Id: serde::Deserialize<'de> + Hash + Eq + Copy,
    SeedFor: FnMut(Id) -> Seed,
    Seed: DeserializeSeed<'de, Value = Entry>,
{
    type Value = indexmap::IndexMap<Id, Entry>;

    fn expecting(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
        formatter.write_str("the entries of an id map")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut entries = indexmap::IndexMap::new();

        while let Some(id) = map.next_key::<Id>()? {
            let entry = map.next_value_seed((self.seed_for)(id))?;
            entries.insert(id, entry);
        }

        Ok(entries)
    }
}
