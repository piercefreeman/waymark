//! Core routing trait — decoupled from any specific metadata shape.
//!
//! [`Routed`] is the single abstract bound that the completions router needs:
//! metadata that carries a routing key.  Any concrete metadata type (e.g.
//! VM-scoped correlation from `waymark-action-runtime-metadata`) implements
//! this trait, keeping the router agnostic to what the key represents.

#![warn(missing_docs)]

/// Metadata that carries a routing key identifying the destination for
/// completions.
///
/// This is the only trait the completions router binds on — it does not
/// need to know whether the key is a VM id, a workflow id, or anything else.
pub trait Routed<Key> {
    /// The routing key that identifies the destination for this completion.
    fn routing_key(&self) -> Key;
}
