//! The promise-level implementations.

use waymark_vm_runtime_promise_value::PromiseValue;

use crate::Operations;
use crate::promise::MaybeUnresolvedError;

impl<Variation, T> waymark_vm_interpreter_coreset::operations::CaptureCallArgument<PromiseValue<T>>
    for Operations<Variation>
where
    Operations<Variation>: waymark_vm_interpreter_coreset::operations::CaptureCallArgument<T>,
{
    fn capture_call_argument(value: &PromiseValue<T>) -> PromiseValue<T> {
        match value {
            PromiseValue::Ready(value) => PromiseValue::Ready(
                <Operations<Variation> as waymark_vm_interpreter_coreset::operations::CaptureCallArgument<
                    T,
                >>::capture_call_argument(value),
            ),
            PromiseValue::Pending(promise_state_id) => PromiseValue::Pending(*promise_state_id),
        }
    }
}

impl<Variation, T> waymark_vm_interpreter_coreset::operations::ShouldJump<PromiseValue<T>>
    for Operations<Variation>
where
    Operations<Variation>: waymark_vm_interpreter_coreset::operations::ShouldJump<T>,
{
    type Error = MaybeUnresolvedError<
        <Operations<Variation> as waymark_vm_interpreter_coreset::operations::ShouldJump<T>>::Error,
    >;

    fn should_jump(value: &PromiseValue<T>) -> Result<bool, Self::Error> {
        let value = value
            .require_ready_ref()
            .map_err(MaybeUnresolvedError::Unresolved)?;
        <Operations<Variation> as waymark_vm_interpreter_coreset::operations::ShouldJump<T>>::should_jump(
            value,
        )
        .map_err(MaybeUnresolvedError::Ready)
    }
}
