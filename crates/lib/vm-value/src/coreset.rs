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

impl waymark_vm_interpreter_coreset::value::FromRaceArmIndex for ReadyValue {
    fn from_race_arm_index(
        arm_index: usize,
    ) -> Result<Self, waymark_vm_interpreter_coreset::value::FromRaceArmIndexError> {
        let arm_index = i64::try_from(arm_index)
            .map_err(|_| waymark_vm_interpreter_coreset::value::FromRaceArmIndexError)?;
        Ok(Self::Int(arm_index))
    }
}
