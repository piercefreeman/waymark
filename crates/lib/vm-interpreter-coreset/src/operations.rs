//! Operations requirements.

/// Evaluate a value and determine whether we should jump or not.
pub trait ShouldJump<Value> {
    /// The implementation-specific error returned when the value cannot
    /// be evaluated as a conditional.
    type Error: core::fmt::Debug;

    /// Evaluate the value.
    ///
    /// Return `true` to jump, return `false` to not jump.
    /// Return an error if the value is not a conditional.
    fn should_jump(value: &Value) -> Result<bool, Self::Error>;
}

/// Capture the value for the purposes of using it as a function call argument.
pub trait CaptureCallArgument<Value> {
    /// Clone or otherwise convert this value for the runtime function call.
    fn capture_call_argument(value: &Value) -> Value;
}

/// A unifying trait for all operations requirements.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait Operations<Value>: CaptureCallArgument<Value> + ShouldJump<Value> {}

/// The error [`ShouldJump`] returns for `Value`.
pub type ShouldJumpErrorFor<Operations, Value> = <Operations as ShouldJump<Value>>::Error;

/// The exception model the operations have to satisfy for the errors of
/// the failing operations to be raised as runtime exceptions.
pub trait Exceptions<Value> {}

impl<T, Value> Exceptions<Value> for T {}
