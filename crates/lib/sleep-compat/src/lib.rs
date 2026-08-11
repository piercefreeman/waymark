//! Compatibility glue implementing the sleep-core traits over the VM
//! value model.

#![warn(missing_docs)]

/// Sleep value provider for the VM value model: sleeps resolve to
/// [`waymark_vm_value_python::ReadyValue::None`].
pub struct ReadyValueSleepProvider;

impl waymark_sleep_core::SleepValueProvider for ReadyValueSleepProvider {
    type Value = waymark_vm_value_python::ReadyValue;

    fn value() -> Self::Value {
        waymark_vm_value_python::ReadyValue::None
    }
}
