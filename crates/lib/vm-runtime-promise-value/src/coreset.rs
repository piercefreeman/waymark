//! [`waymark_vm_interpreter_coreset`] trait implementations.

use crate::PromiseValue;

impl<T> waymark_vm_interpreter_coreset::value::ShouldJump for PromiseValue<T>
where
    T: waymark_vm_interpreter_coreset::value::ShouldJump,
{
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        let value = self
            .require_ready_ref()
            .map_err(|_| waymark_vm_interpreter_coreset::value::NotAConditionalError)?;
        value.should_jump()
    }
}

impl<T> waymark_vm_interpreter_coreset::value::CaptureCallArgument for PromiseValue<T>
where
    T: waymark_vm_interpreter_coreset::value::CaptureCallArgument,
{
    fn capture_call_argument(&self) -> Self {
        match self {
            Self::Ready(value) => Self::Ready(T::capture_call_argument(value)),
            Self::Pending(promise_state_id) => Self::Pending(*promise_state_id),
        }
    }
}
