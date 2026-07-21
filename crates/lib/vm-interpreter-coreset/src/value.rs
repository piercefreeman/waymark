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

/// Capture the value for the purposes of using it as a function call argument.
pub trait CaptureCallArgument {
    /// Clone or otherwise convert this value for the runtime function call.
    fn capture_call_argument(&self) -> Self;
}

/// An error from the [`FromRaceArmIndex`].
#[derive(Debug, thiserror::Error)]
#[error("the race arm index is out of bounds for the value")]
pub struct FromRaceArmIndexError;

/// Construct the value that a race promise resolves with - the index of
/// the arm that settled first.
pub trait FromRaceArmIndex: Sized {
    /// Construct the arm-index value.
    ///
    /// Returns an error if the index cannot be represented by this
    /// value type.
    fn from_race_arm_index(arm_index: usize) -> Result<Self, FromRaceArmIndexError>;
}

/// A unifying trait for all value requirements.
pub trait Value:
    waymark_vm_runtime_value::RootValueAccess<RootValue = Self>
    + CaptureCallArgument
    + ShouldJump
    + FromRaceArmIndex
{
}

impl<T> Value for T where
    T: waymark_vm_runtime_value::RootValueAccess<RootValue = Self>
        + CaptureCallArgument
        + ShouldJump
        + FromRaceArmIndex
{
}
