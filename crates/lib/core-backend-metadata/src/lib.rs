//! Core backend metadata.
//!
//! The intended use for this crate is to allow extending the core backend
//! types with metadata that is abstracted away from the core backend itself.
//!
//! This allows us to lighten up the mental model of how core backend operates
//! without concerning ourselves with the abstraction leakage from other
//! domains.
//!
//! ## When to use
//!
//! Core backend doesn't need to know the ID of a scheduler that enququed
//! the workload it has to process, but the scheduler and the webapp
//! need this data to be associated with the workload in order to show it
//! in the UI.
//!
//! Thus, scheduler want to providing this data, and webapp want to consume it,
//! while the core backend doesn't want / need to be aware of it.
//! Using core metadata, the scheduler subsystem can define a metadata item
//! type providing an optional Scheduler ID as the value, and allow backend
//! implementations to handle it (if they choose to support it,
//! deciding at compile time). Then the core backend implementation will accept
//! (and require) the metadata values and store them however backend decides to
//! store them, and other (non-core) backend implementation can rely on those
//! metadata items being stored by the core backend implementation, and use
//! utilize the private implementation details of however that's done to
//! implement their needs.

#![warn(missing_docs)]

use typle::typle;

/// A metadata item is an abstract type that declares a single metadata value
/// and specifies its type.
///
/// Users should implement this for their own metadata item marker types.
pub trait MetadataItem {
    /// The metadata item key; may be used for storage.
    const KEY: &str;

    /// The value of the metadata item.
    type Value;
}

mod private {
    pub trait Sealed {}
}

/// The metadata items list.
///
/// Automatically implemented for tuples of [`MetadataItem`]s
/// and provides a tuples with their corresponding values and an array of keys.
pub trait MetadataItems: private::Sealed {
    /// An array of [`MetadataItem`] keys.
    const KEYS: &[&str];

    /// The tuple of [`MetadataItem`] values.
    type Values;
}

#[typle(Tuple for 0..=5)]
impl<Items> private::Sealed for Items
where
    Items: Tuple,
    Items<_>: MetadataItem,
{
}

#[typle(Tuple for 0..=5)]
impl<Items> MetadataItems for Items
where
    Items: Tuple,
    Items<_>: MetadataItem,
{
    const KEYS: &[&str] = &[typle!(i in .. => <Items<{i}>>::KEY)];
    type Values = (typle!(i in .. => Items<{i}>::Value));
}

/// A typed collection of metadata values for a [`MetadataItems`] tuple.
///
/// Wraps the corresponding values tuple and provides some useful trait
/// implementations to facilitate operation on the metadata values
/// as a high-level bag-of-metadata collection rather than a simple tuple
/// of [`MetadataItems`]-associated values.
pub struct MetadataValues<Items: MetadataItems>(pub Items::Values);

#[typle(Tuple for 0..=5)]
impl<Items> core::fmt::Debug for MetadataValues<Items>
where
    Items: Tuple,
    Items<_>: MetadataItem,
    <Items<_> as MetadataItem>::Value: core::fmt::Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut s = f.debug_struct("MetadataValues");

        for typle_index!(i) in 0..Items::LEN {
            s.field(Items::KEYS[i], &self.0[[i]]);
        }

        s.finish()
    }
}

#[typle(Tuple for 0..=5)]
impl<Items> From<<Items as MetadataItems>::Values> for MetadataValues<Items>
where
    Items: Tuple,
    Items<_>: MetadataItem,
{
    fn from(values: <Items as MetadataItems>::Values) -> Self {
        Self(values)
    }
}

#[typle(Tuple for 0..=5)]
impl<Items> From<MetadataValues<Items>> for <Items as MetadataItems>::Values
where
    Items: Tuple,
    Items<_>: MetadataItem,
{
    fn from(values: MetadataValues<Items>) -> Self {
        values.0
    }
}

#[typle(Tuple for 0..=5)]
impl<Items> MetadataValues<Items>
where
    Items: Tuple,
    Items<_>: MetadataItem,
{
    /// Returns the wrapped metadata values as their underlying tuple.
    pub fn into_tuple(self) -> <Items as MetadataItems>::Values {
        self.0
    }
}
