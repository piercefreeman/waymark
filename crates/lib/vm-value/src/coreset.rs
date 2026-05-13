//! [`waymark_vm_interpreter_coreset`] trait implementations for [`Value`].

use crate::Value;

impl waymark_vm_interpreter_coreset::value::ShouldJump for Value {
    fn should_jump(
        &self,
    ) -> Result<bool, waymark_vm_interpreter_coreset::value::NotAConditionalError> {
        Ok(self.is_truthy())
    }
}
