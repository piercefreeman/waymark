//! Value requirements.

/// An error from the [`ShouldJump`].
#[derive(Debug, thiserror::Error)]
#[error("the value is not a conditional")]
pub struct NotAConditionalError;

/// Evaluate a value and determine whether we should jump or not.
pub trait ShouldJump {
    /// Evaluate the value.
    ///
    /// Return `true` to jump, return `false` to not jump.
    /// Return an error if the value is not a conditional.
    fn should_jump(&self) -> Result<bool, NotAConditionalError>;
}

/// A unifying trait for all value requirements.
pub trait Value: ShouldJump {}

impl<T> Value for T where T: ShouldJump {}
