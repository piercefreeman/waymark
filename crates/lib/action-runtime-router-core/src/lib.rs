//! Core traits for routing action call completions to the correct VM
//! based on per-call metadata.

#![warn(missing_docs)]

/// A type that exposes a routing key used to direct action call completions
/// to the correct destination (e.g., a specific VM).
pub trait HasRoutingKey {
    /// The raw key used to route completions to a specific VM.
    type RoutingKey: core::hash::Hash + Eq;
}

/// Metadata that can be converted into a routing key for the routed
/// completions provider.
pub trait ToRoutingKey: HasRoutingKey {
    /// Convert the metadata into its routing key.
    fn to_routing_key(&self) -> Self::RoutingKey;
}

/// A convenience alias that extracts the [`RoutingKey`](HasRoutingKey::RoutingKey)
/// associated type from a type implementing [`HasRoutingKey`].
pub type RoutingKeyFor<T> = <T as HasRoutingKey>::RoutingKey;
