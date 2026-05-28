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

/// A unifying trait for all value requirements.
pub trait Value:
    waymark_vm_runtime_value::RootValueAccess<RootValue = Self>
    + CaptureCallArgument
    + ShouldJump
    + waymark_vm_runtime_exception::AsException
{
}

impl<T> Value for T where
    T: waymark_vm_runtime_value::RootValueAccess<RootValue = Self>
        + CaptureCallArgument
        + ShouldJump
        + waymark_vm_runtime_exception::AsException
{
}
