//! Interfaces for abstract executables supported by the VM.

#![warn(missing_docs)]

use std::sync::Arc;

/// A trait of an abstract executable having functions.
pub trait Functions {
    /// The function ID.
    ///
    /// A type that allows referring to a function in the executable.
    ///
    /// Typically an index in the functions table.
    type FunctionId;
}

/// A trait of an abstract executable having function states.
pub trait FunctionStates: Functions {
    /// The state ID.
    ///
    /// A type that allows referring to a state with in a function in
    /// the executable.
    ///
    /// Typically an index in the states table (inside the functions table).
    type StateId;
}

/// A way for an abstract executable to provide information about a function.
pub trait FunctionInfo: Functions {
    /// How many registers a compiled function is expecting to have available
    /// while executing.
    ///
    /// Typically determined by a compiler at compile time and stored in
    /// the bytecode.
    ///
    /// Used for allocating a frame for running the function.
    ///
    /// Returns `None` if no function with such `function_id` exists in
    /// the executable.
    fn function_num_regs(&self, function_id: Self::FunctionId) -> Option<usize>;
}

/// A way for an abstract executable to provide instructions sequence for
/// a given function's sub-state.
pub trait InstructionsProvider: FunctionStates {
    /// The instruction type.
    ///
    /// Typically an enum representing an instruction set.
    type Instruction;

    /// Instructions for a given function's sub-state.
    ///
    /// Typically used to fetch the instructions for execution.
    ///
    /// Returns `None` if a function with such `function_id` or,
    /// in the corresponding function, a state with such `state_id`
    /// don't exist in the executable.
    fn function_state_instructions(
        &self,
        function_id: Self::FunctionId,
        state_id: Self::StateId,
    ) -> Option<impl IntoIterator<Item = &Self::Instruction> + '_>;
}

// ---------------------------------------------------------------------------
// Blanket impls for Arc
// ---------------------------------------------------------------------------

impl<T: Functions> Functions for Arc<T> {
    type FunctionId = T::FunctionId;
}

impl<T: FunctionStates> FunctionStates for Arc<T> {
    type StateId = T::StateId;
}

impl<T: FunctionInfo> FunctionInfo for Arc<T> {
    fn function_num_regs(&self, function_id: Self::FunctionId) -> Option<usize> {
        (**self).function_num_regs(function_id)
    }
}

impl<T: InstructionsProvider> InstructionsProvider for Arc<T> {
    type Instruction = T::Instruction;

    fn function_state_instructions(
        &self,
        function_id: Self::FunctionId,
        state_id: Self::StateId,
    ) -> Option<impl IntoIterator<Item = &Self::Instruction> + '_> {
        (**self).function_state_instructions(function_id, state_id)
    }
}
