//! [`waymark_vm_interpreter_coreset`] operations implementations for the
//! Python variation.

use waymark_vm_value::ReadyValue;

use crate::PythonVariation;

impl waymark_vm_interpreter_coreset::operations::ShouldJump<ReadyValue> for PythonVariation {
    type Error = core::convert::Infallible;

    fn should_jump(value: &ReadyValue) -> Result<bool, Self::Error> {
        Ok(value.is_truthy())
    }
}
