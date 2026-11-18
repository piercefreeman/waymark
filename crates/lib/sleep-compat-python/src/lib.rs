//! Compatibility glue implementing the sleep-core traits over the VM
//! value model.

#![warn(missing_docs)]

/// Sleep value provider for the VM value model: sleeps resolve to
/// [`waymark_vm_value_python::MaterialValue::None`].
pub struct MaterialValueSleepProvider;

impl waymark_sleep_core::SleepValueProvider for MaterialValueSleepProvider {
    type Value = waymark_vm_value_python::MaterialValue;

    fn value() -> Self::Value {
        waymark_vm_value_python::MaterialValue::None
    }
}
