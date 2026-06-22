//! Shared types and lowering traits for compilers that target `vm-ast-old`.
//!
//! The main `vm-compiler-for-ast-old` crate uses this crate for lowering
//! interfaces, spec bounds, plus a few convenience aliases for compiled
//! instruction and executable types.

#![warn(missing_docs, clippy::missing_docs_in_private_items)]

pub mod lowering;

/// Trait bound collecting the instruction set spec requirements for
/// an AST-old compiler.
pub trait SpecRequirements:
    waymark_vm_instructions_coreset::Spec<
        RegisterId = waymark_vm_runtime_core::RegisterId,
        FunctionId = waymark_vm_bytecode_core::FunctionId,
        StateId = waymark_vm_bytecode_core::StateId,
    > + waymark_vm_instructions_extcallset::Spec<
        RegisterId = waymark_vm_runtime_core::RegisterId,
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
        > + waymark_vm_instructions_extcallset::Spec<
            RegisterId = waymark_vm_runtime_core::RegisterId,
            StateId = waymark_vm_bytecode_core::StateId,
        > + waymark_vm_instructions_pureset::Spec
        + waymark_vm_instructions_fullset::Spec
{
}

/// Convenience alias for the instruction type (full instruction set)
/// for an AST-old compiler.
pub type InstructionFor<Spec> = waymark_vm_instructions_fullset::FullSet<Spec>;

/// Convenience alias for an executable produced by an AST-old compiler.
pub type ExecutableFor<Spec> = waymark_vm_bytecode::Executable<InstructionFor<Spec>>;

/// Convenience alias for [`Metadata`](waymark_vm_compiler_metadata::Metadata)
/// using this compiler's [`FunctionId`](waymark_vm_bytecode_core::FunctionId).
pub type Metadata = waymark_vm_compiler_metadata::Metadata<waymark_vm_bytecode_core::FunctionId>;
