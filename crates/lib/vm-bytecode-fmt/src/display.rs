use crate::Fmt;
use index_type::typed_vec::TypedVec;
use waymark_vm_bytecode_core::StateId;

impl<'a, Instruction> core::fmt::Display for Fmt<'a, waymark_vm_bytecode::Executable<Instruction>>
where
    Instruction: core::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> core::fmt::Result {
        self.padded_fmt(f, 0)
    }
}

impl<'a, Instruction> core::fmt::Display for Fmt<'a, waymark_vm_bytecode::Function<Instruction>>
where
    Instruction: core::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> core::fmt::Result {
        self.padded_fmt(f, 0)
    }
}

impl<'a, Instruction> core::fmt::Display for Fmt<'a, waymark_vm_bytecode::State<Instruction>>
where
    Instruction: core::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> core::fmt::Result {
        self.padded_fmt(f, 0)
    }
}

impl<'a, Instruction> core::fmt::Display
    for Fmt<'a, TypedVec<StateId, waymark_vm_bytecode::State<Instruction>>>
where
    Instruction: core::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> core::fmt::Result {
        self.padded_fmt(f, 0)
    }
}
