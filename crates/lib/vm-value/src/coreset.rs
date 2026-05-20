//! [`waymark_vm_interpreter_coreset`] trait implementations.

use crate::ReadyValue;

impl waymark_vm_interpreter_coreset::value::ShouldJump for ReadyValue {
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        Ok(self.is_truthy())
    }
}

impl waymark_vm_interpreter_coreset::value::CaptureCallArgument for ReadyValue {
    fn capture_call_argument(&self) -> Self {
        self.clone()
    }
}
