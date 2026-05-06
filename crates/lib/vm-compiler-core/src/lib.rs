pub trait SpecRequirements:
    waymark_vm_instructions_coreset::Spec<
        RegisterId = waymark_vm_runtime_core::RegisterId,
        FunctionId = waymark_vm_bytecode_core::FunctionId,
        StateId = waymark_vm_bytecode_core::StateId,
    > + waymark_vm_instructions_pureset::Spec
    + waymark_vm_instructions_fullset::Spec
{
}

impl<T> SpecRequirements for T where
    T: waymark_vm_instructions_coreset::Spec<
            RegisterId = waymark_vm_runtime_core::RegisterId,
            FunctionId = waymark_vm_bytecode_core::FunctionId,
            StateId = waymark_vm_bytecode_core::StateId,
        > + waymark_vm_instructions_pureset::Spec
        + waymark_vm_instructions_fullset::Spec
{
}
