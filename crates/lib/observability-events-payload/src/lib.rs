//! The production payload of the observability-events subsystem: the
//! closed set of kinds, and the union of every source's typed event.
//!
//! Each source has its own module holding its payload and its kinds; the
//! root types are the union over the sources. Nothing else in the
//! observability-events subsystem names them — the pipeline and the
//! stores are generic over the payload, and only the wiring instantiates
//! them with this crate's types.

#![warn(missing_docs)]

mod kind;
mod payload;

pub use kind::*;
pub use payload::*;

#[cfg(test)]
mod tests;
