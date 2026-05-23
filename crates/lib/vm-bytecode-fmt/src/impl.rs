use index_type::typed_vec::TypedVec;
use waymark_vm_bytecode_core::{FunctionId, InstructionId, StateId};

use crate::Fmt;

impl<'a, Instruction> Fmt<'a, TypedVec<InstructionId, Instruction>>
where
    Instruction: core::fmt::Debug,
{
    pub fn padded_fmt(&self, f: &mut std::fmt::Formatter<'_>, padding: usize) -> core::fmt::Result {
        let pad = " ".repeat(padding);

        for instruction in self.0.iter() {
            writeln!(f, "{pad}{:?}", instruction)?;
        }

        Ok(())
    }
}

impl<'a, Instruction> Fmt<'a, waymark_vm_bytecode::State<Instruction>>
where
    Instruction: core::fmt::Debug,
{
    pub fn padded_fmt(&self, f: &mut std::fmt::Formatter<'_>, padding: usize) -> core::fmt::Result {
        Fmt(&self.0.instructions).padded_fmt(f, padding)
    }
}

impl<'a, Instruction> Fmt<'a, TypedVec<StateId, waymark_vm_bytecode::State<Instruction>>>
where
    Instruction: core::fmt::Debug,
{
    pub fn padded_fmt(&self, f: &mut std::fmt::Formatter<'_>, padding: usize) -> core::fmt::Result {
        let pad = " ".repeat(padding);

        for (index, state) in self.0.iter_enumerated() {
            writeln!(f, "{pad}{:?}:", index)?;
            Fmt(state).padded_fmt(f, padding + 2)?;
        }

        Ok(())
    }
}

impl<'a, Instruction> Fmt<'a, waymark_vm_bytecode::Function<Instruction>>
where
    Instruction: core::fmt::Debug,
{
    pub fn padded_fmt(&self, f: &mut std::fmt::Formatter<'_>, padding: usize) -> core::fmt::Result {
        Fmt(&self.0.states).padded_fmt(f, padding)
    }
}

impl<'a, Instruction> Fmt<'a, TypedVec<FunctionId, waymark_vm_bytecode::Function<Instruction>>>
where
    Instruction: core::fmt::Debug,
{
    pub fn padded_fmt(&self, f: &mut std::fmt::Formatter<'_>, padding: usize) -> core::fmt::Result {
        let pad = " ".repeat(padding);

        for (index, function) in self.0.iter_enumerated() {
            writeln!(f, "{pad}{:?}: [{} registers]", index, function.num_regs)?;
            Fmt(&function.states).padded_fmt(f, padding + 2)?;
        }

        Ok(())
    }
}

impl<'a, Instruction> Fmt<'a, waymark_vm_bytecode::Executable<Instruction>>
where
    Instruction: core::fmt::Debug,
{
    pub fn padded_fmt(&self, f: &mut std::fmt::Formatter<'_>, padding: usize) -> core::fmt::Result {
        Fmt(&self.0.functions).padded_fmt(f, padding)
    }
}
