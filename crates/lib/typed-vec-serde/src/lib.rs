//! Serde support for `index_type::typed_vec::TypedVec`.

#![warn(missing_docs)]

use index_type::{IndexType, typed_vec::TypedVec};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Serialize a `TypedVec` as a plain sequence.
pub fn serialize<I, T, S>(vec: &TypedVec<I, T>, serializer: S) -> Result<S::Ok, S::Error>
where
    I: IndexType,
    T: Serialize,
    S: Serializer,
{
    let values: Vec<&T> = vec.iter().collect();
    values.serialize(serializer)
}

/// Deserialize a `TypedVec` from a plain sequence.
pub fn deserialize<'de, I, T, D>(deserializer: D) -> Result<TypedVec<I, T>, D::Error>
where
    I: IndexType,
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    let values = Vec::<T>::deserialize(deserializer)?;
    Ok(TypedVec::from_iter(values))
}
