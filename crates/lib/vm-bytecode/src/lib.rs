//! Generic VM bytecode shape definition.

#![warn(missing_docs)]

use index_type::typed_vec::TypedVec;
use waymark_vm_bytecode_core::{FunctionId, InstructionId, StateId};

/// An executable with the given `Instruction`s.
///
/// A collection of functions.
#[derive(Debug)]
pub struct Executable<Instruction> {
    /// Functions this executable contains.
    pub functions: TypedVec<FunctionId, Function<Instruction>>,
}

/// A function with the given `Instruction`s.
#[derive(Debug)]
pub struct Function<Instruction> {
    /// States (as in state-machine states) this function consists of.
    pub states: TypedVec<StateId, State<Instruction>>,

    /// The number of registers this function uses;
    pub num_regs: usize,
}

/// A state (as in state-machine states) with the given `Instruction`s.
#[derive(Debug)]
pub struct State<Instruction> {
    /// The sequence of `Instruction`s.
    pub instructions: TypedVec<InstructionId, Instruction>,
}

impl<Instruction> waymark_vm_executable::Functions for Executable<Instruction> {
    type FunctionId = FunctionId;
}

impl<Instruction> waymark_vm_executable::FunctionStates for Executable<Instruction> {
    type StateId = StateId;
}

impl<Instruction> waymark_vm_executable::FunctionInfo for Executable<Instruction> {
    fn function_num_regs(&self, function_id: Self::FunctionId) -> Option<usize> {
        let function = self.functions.get(function_id)?;
        Some(function.num_regs)
    }
}

impl<Instruction> waymark_vm_executable::InstructionsProvider for Executable<Instruction> {
    type Instruction = Instruction;

    fn function_state_instructions(
        &self,
        function_id: Self::FunctionId,
        state_id: Self::StateId,
    ) -> Option<impl IntoIterator<Item = &Self::Instruction> + '_> {
        let function = self.functions.get(function_id)?;
        let state = function.states.get(state_id)?;
        Some(state.instructions.iter())
    }
}
