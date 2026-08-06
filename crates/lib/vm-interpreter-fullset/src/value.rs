//! Value requirements.

/// A unifying trait for all value requirements.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait Value:
    waymark_vm_interpreter_coreset::Value
    + waymark_vm_interpreter_extcallset::Value
    + waymark_vm_interpreter_pureset::Value
{
}
