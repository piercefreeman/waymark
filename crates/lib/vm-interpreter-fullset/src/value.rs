//! Value requirements.

/// A unifying trait for all value requirements.
pub trait Value:
    waymark_vm_interpreter_coreset::Value
    + waymark_vm_interpreter_extcallset::Value
    + waymark_vm_interpreter_pureset::Value
{
}

impl<T> Value for T where
    T: waymark_vm_interpreter_coreset::Value
        + waymark_vm_interpreter_extcallset::Value
        + waymark_vm_interpreter_pureset::Value
{
}
