//! Core types for runtime operations support for VM effects.

#![warn(missing_docs)]

mod number;

pub use crate::number::EffectNumber;

/// An effect emitted by the runtime, paired with its zero-based sequence
/// number.
#[derive(Debug, PartialEq, Eq)]
pub struct EmittedEffect<Effect> {
    /// The effect emitted by the interpreter.
    pub effect: Effect,

    /// The zero-based sequence number of this effect.
    pub number: EffectNumber,
}
