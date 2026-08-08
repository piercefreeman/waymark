//! The ready-level implementations.

use waymark_vm_value::ReadyValue;

use crate::Operations;

impl<Variation> waymark_vm_interpreter_coreset::operations::CaptureCallArgument<ReadyValue>
    for Operations<Variation>
{
    fn capture_call_argument(value: &ReadyValue) -> ReadyValue {
        value.clone()
    }
}

impl<Variation> waymark_vm_interpreter_coreset::operations::ShouldJump<ReadyValue>
    for Operations<Variation>
where
    Variation: waymark_vm_interpreter_coreset::operations::ShouldJump<ReadyValue>,
{
    type Error =
        <Variation as waymark_vm_interpreter_coreset::operations::ShouldJump<ReadyValue>>::Error;

    fn should_jump(value: &ReadyValue) -> Result<bool, Self::Error> {
        <Variation as waymark_vm_interpreter_coreset::operations::ShouldJump<ReadyValue>>::should_jump(
            value,
        )
    }
}
