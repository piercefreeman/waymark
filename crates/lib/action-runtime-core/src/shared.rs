//! Shared traits for action runtime.

/// A type that carries metadata about an action call.
pub trait WithActionCallMetadata {
    /// The metadata type associated with an action call.
    type ActionCallMetadata;
}

/// A convenience alias that extracts the [`ActionCallMetadata`](WithActionCallMetadata::ActionCallMetadata)
/// associated type from a type implementing [`WithActionCallMetadata`].
pub type ActionCallMetadataFor<T> = <T as WithActionCallMetadata>::ActionCallMetadata;
