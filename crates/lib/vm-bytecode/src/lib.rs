use index_type::{IndexType, typed_vec::TypedVec};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType)]
pub struct FunctionId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType)]
pub struct StateId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType)]
pub struct InstructionId(pub usize);

pub struct Executable<Instruction> {
    pub functions: TypedVec<FunctionId, Function<Instruction>>,
}

pub struct Function<Instruction> {
    pub states: TypedVec<StateId, State<Instruction>>,

    /// The number of registries this function uses;
    pub num_regs: usize,
}

pub struct State<Instruction> {
    pub instructions: TypedVec<InstructionId, Instruction>,
}
