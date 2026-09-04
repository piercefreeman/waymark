//! The observability-events emitter: the one producer of events in a
//! process. It stamps each payload with the node's identity, the next
//! position in the node's stream, and the time, and hands the event to
//! the observability-events lossy batcher — synchronously, never waiting.

#![warn(missing_docs)]

mod emitter;

pub use emitter::*;

#[cfg(test)]
mod tests;
