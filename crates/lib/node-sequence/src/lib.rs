//! Positions in a node's stream of emissions, and the counter that
//! produces them.

#![warn(missing_docs)]

use std::sync::atomic::{AtomicU64, Ordering};

/// Position of one emission in its node's stream: ordering, and gap
/// detection — a reader following a node's stream sees a dropped or a
/// late emission as a jump.
///
/// Constructed only through purpose-specific pathways, each named for
/// what it is: minted by [`NodeSequenceCounter::next`], or brought back
/// from the persisted log by [`NodeSequence::from_persisted`]. There is
/// deliberately no bare constructor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeSequence(u64);

impl NodeSequence {
    /// A position read back from the persisted log: it was minted by a
    /// counter before it was stored, so this re-materializes rather
    /// than mints.
    pub const fn from_persisted(position: u64) -> Self {
        Self(position)
    }

    /// The position as a number.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// The per-node counter: one per process, owned by the emitter.
#[derive(Debug)]
pub struct NodeSequenceCounter(AtomicU64);

impl NodeSequenceCounter {
    /// A counter at the start of the stream.
    #[expect(
        clippy::new_without_default,
        reason = "a fresh counter is a deliberate construction, not a default value"
    )]
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// The next position: unique, and monotonic in the counter's
    /// modification order.
    pub fn next(&self) -> NodeSequence {
        NodeSequence(self.0.fetch_add(1, Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests;
