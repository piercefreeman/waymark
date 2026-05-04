//! Core VM bytecode primitives.

#![warn(missing_docs)]

use index_type::{IndexTooBigError, IndexType};

/// Identifies a function within a VM executable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType, Default)]
#[index_type(error = FunctionIdTooBigError)]
pub struct FunctionId(pub usize);

/// Identifies a state-machine state within a function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType, Default)]
#[index_type(error = StateIdTooBigError)]
pub struct StateId(pub usize);

/// Identifies an instruction within a state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, IndexType, Default)]
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
