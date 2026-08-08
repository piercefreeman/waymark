//! Operations requirements.

/// A unifying trait for all operations requirements.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait Operations<Value>:
    waymark_vm_interpreter_coreset::Operations<Value>
    + waymark_vm_interpreter_extcallset::Operations<Value>
    + waymark_vm_interpreter_pureset::Operations<Value>
where
    Value: waymark_vm_runtime_value::RootValueAccess,
{
}

/// A unifying trait for all exception-model requirements.
#[waymark_blanket_impl_macros::blanket_impl]
pub trait Exceptions<Value>:
    waymark_vm_interpreter_coreset::operations::Exceptions<Value>
    + waymark_vm_interpreter_extcallset::operations::Exceptions<Value>
    + waymark_vm_interpreter_pureset::operations::Exceptions<Value>
where
    Value: waymark_vm_runtime_value::RootValueAccess,
{
}
