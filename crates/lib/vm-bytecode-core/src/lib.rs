//! Core VM bytecode primitives.

#![warn(missing_docs)]

use index_type::{IndexTooBigError, IndexType};

/// Identifies a function within a VM executable.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType, Default)]
#[index_type(error = FunctionIdTooBigError)]
pub struct FunctionId(pub usize);

/// Identifies a state-machine state within a function.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType, Default)]
#[index_type(error = StateIdTooBigError)]
pub struct StateId(pub usize);

/// Identifies an instruction within a state.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType, Default)]
#[index_type(error = InstructionIdTooBigError)]
pub struct InstructionId(pub usize);

/// Error returned when a raw index cannot be represented as a [`FunctionId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, IndexTooBigError)]
#[index_too_big_error(msg = "function id")]
pub struct FunctionIdTooBigError;

/// Error returned when a raw index cannot be represented as a [`StateId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, IndexTooBigError)]
#[index_too_big_error(msg = "state id")]
pub struct StateIdTooBigError;

/// Error returned when a raw index cannot be represented as an [`InstructionId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, IndexTooBigError)]
#[index_too_big_error(msg = "instruction id")]
pub struct InstructionIdTooBigError;

impl core::fmt::Debug for FunctionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "f{}", self.0)
    }
}

impl core::fmt::Debug for StateId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s{}", self.0)
    }
}

impl core::fmt::Debug for InstructionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "x{}", self.0)
    }
}
