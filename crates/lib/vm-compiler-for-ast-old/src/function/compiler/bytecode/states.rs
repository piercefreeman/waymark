//! State-machine storage for one compiled function.
//!
//! The compiler emits one [`waymark_vm_bytecode::State`] at a time, and this
//! helper keeps the active state selected while the rest remain stored in the
//! output vector.

use index_type::typed_vec::TypedVec;
use waymark_vm_bytecode_core::{InstructionId, StateId};

/// Owns the output state vector while tracking the currently active state.
pub struct FunctionStates<Instruction> {
    /// All states emitted for the current function.
    states: TypedVec<StateId, waymark_vm_bytecode::State<Instruction>>,

    /// Id of the state currently receiving instructions, if any.
    current_state_id: Option<StateId>,
}

impl<Instruction> FunctionStates<Instruction> {
    /// Creates a state collection with the initial state selected.
    pub fn new() -> Self {
        let mut states = TypedVec::<StateId, _>::with_capacity(1);
        let current_state_id = states.push(Self::empty_state());

        Self {
            states,
            current_state_id: Some(current_state_id),
        }
    }

    /// Returns whether there is an active state to emit into.
    pub fn is_active(&self) -> bool {
        self.current_state_id.is_some()
    }

    /// Appends an instruction to the currently active state.
    pub fn emit(&mut self, instruction: Instruction) {
        let Some(current_state_id) = self.current_state_id else {
            unreachable!("compiler should not emit instructions after a terminal");
        };

        self.states[current_state_id].instructions.push(instruction);
    }

    /// Reserves a new empty state and returns its id.
    pub fn reserve_state(&mut self) -> StateId {
        self.states.push(Self::empty_state())
    }

    /// Switches emission to an already-reserved state.
    pub fn switch_to(&mut self, state_id: StateId) {
        self.current_state_id = Some(state_id);
    }

    /// Stops emitting instructions until another state is selected.
    pub fn terminate(&mut self) {
        self.current_state_id = None;
    }

    /// Finishes compilation and returns all recorded states.
    pub fn finish(self) -> TypedVec<StateId, waymark_vm_bytecode::State<Instruction>> {
        self.states
    }

    /// Creates an empty bytecode state.
    fn empty_state() -> waymark_vm_bytecode::State<Instruction> {
        waymark_vm_bytecode::State {
            instructions: TypedVec::<InstructionId, _>::with_capacity(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use index_type::IndexType;

    use super::FunctionStates;
    use waymark_vm_bytecode_core::StateId;

    #[test]
    fn new_starts_with_an_active_initial_state() {
        let states = FunctionStates::<i32>::new();

        assert!(states.is_active());
        assert_eq!(states.current_state_id, Some(StateId(0)));
        assert_eq!(states.states.len().to_scalar(), 1);
    }

    #[test]
    fn reserve_state_returns_the_new_state_id() {
        let mut states = FunctionStates::<i32>::new();

        let next_state = states.reserve_state();

        assert_eq!(next_state, StateId(1));
        assert_eq!(states.states.len().to_scalar(), 2);
    }

    #[test]
    fn emit_switch_terminate_and_finish_use_the_current_state() {
        let mut states = FunctionStates::<i32>::new();
        let next_state = states.reserve_state();

        states.emit(10);
        states.switch_to(next_state);
        states.emit(20);
        states.terminate();

        assert!(!states.is_active());

        let states = states.finish();

        assert_eq!(states[StateId(0)].instructions.len().to_scalar(), 1);
        assert_eq!(
            states[StateId(0)]
                .instructions
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![10]
        );
        assert_eq!(states[StateId(1)].instructions.len().to_scalar(), 1);
        assert_eq!(
            states[StateId(1)]
                .instructions
                .iter()
                .copied()
                .collect::<Vec<_>>(),
            vec![20]
        );
    }
}
