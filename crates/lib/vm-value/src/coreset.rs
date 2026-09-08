//! [`waymark_vm_interpreter_coreset`] trait implementations.

use crate::ReadyValue;

impl<Flavor: crate::Flavor> waymark_vm_interpreter_coreset::value::ShouldJump
    for ReadyValue<Flavor>
{
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        Ok(self.is_truthy())
    }
}

impl<Flavor: crate::Flavor> waymark_vm_interpreter_coreset::value::CaptureCallArgument
    for ReadyValue<Flavor>
where
    Flavor::Extension: Clone,
{
    fn capture_call_argument(&self) -> Self {
        self.clone()
    }
}
