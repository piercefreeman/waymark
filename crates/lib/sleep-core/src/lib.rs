//! Core types for the sleep subsystem.

#![warn(missing_docs)]

/// Provides the value an elapsed sleep resolves with.
///
/// The provider is purely type-level — the value is necessarily
/// context-free: every elapsed sleep resolves to the same (freshly
/// minted) value.
pub trait SleepValueProvider {
    /// The value an elapsed sleep resolves to.
    type Value;

    /// Produce the sleep resolution value.
    fn value() -> Self::Value;
}
