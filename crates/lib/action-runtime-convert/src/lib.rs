//! A converter that provides conversion for the action runtime.

#![warn(missing_docs)]

mod from_proto;
mod from_worker_core;
mod to_proto;
mod to_worker_core;

/// A converter that provides conversion for the action runtime.
pub struct Converter;
