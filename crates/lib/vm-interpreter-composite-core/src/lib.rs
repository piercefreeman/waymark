//! Vocabulary traits for composite interpreters.
//!
//! A composite interpreter delegates to a set of sub-interpreters, one per
//! instruction set. To do so generically, it needs a small set of structural
//! operations on the runtime types it passes around — and nothing else.
//! This crate defines those operations as traits, so the composite machinery
//! itself stays independent of any particular runtime: the runtime crates
//! implement this vocabulary for their own types.

#![warn(missing_docs)]

/// Detect whether the frame switched to a different function state.
///
/// The composite interpreter chains its sub-interpreters' hooks only while
/// the frame stays in the same state. It captures a token before invoking
/// a sub-interpreter and asks afterwards whether the frame has since
/// switched away. How state identity is represented and compared is the
/// implementation's business.
pub trait DetectStateSwitch {
    /// An opaque token capturing the identity of the current state.
    type StateToken;

    /// Capture the identity of the current state.
    fn capture_state_token(&self) -> Self::StateToken;

    /// Whether the frame has switched away from the captured state.
    fn state_switched(&self, token: &Self::StateToken) -> bool;
}
