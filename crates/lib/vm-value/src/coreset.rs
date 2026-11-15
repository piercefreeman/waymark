//! [`waymark_vm_interpreter_coreset`] trait implementations.

use crate::MaterialValue;

impl<Flavor: crate::Flavor> waymark_vm_interpreter_coreset::value::ShouldJump
    for MaterialValue<Flavor>
{
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        Ok(self.is_truthy())
    }
}

impl<Flavor: crate::Flavor> waymark_vm_interpreter_coreset::value::CaptureCallArgument
    for MaterialValue<Flavor>
where
    Flavor::Extension: Clone,
    Flavor::ExceptionValue: Clone,
{
    fn capture_call_argument(&self) -> Self {
        self.clone()
    }
}
