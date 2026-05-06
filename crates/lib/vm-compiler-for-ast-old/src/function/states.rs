//! Function states tracking.

use index_type::typed_vec::TypedVec;
use waymark_vm_bytecode_core::{InstructionId, StateId};

pub(crate) struct FunctionStates<Instruction> {
    states: TypedVec<StateId, waymark_vm_bytecode::State<Instruction>>,
    current_state: waymark_vm_bytecode::State<Instruction>,
    current_state_id: Option<StateId>,
}

impl<Instruction> FunctionStates<Instruction> {
    pub fn new() -> Self {
        let mut states = TypedVec::<StateId, _>::with_capacity(1);
        states.push(Self::empty_state());

        Self {
            states,
            current_state: Self::empty_state(),
            current_state_id: Some(StateId(0)),
        }
    }

    pub fn is_active(&self) -> bool {
        self.current_state_id.is_some()
    }

    pub fn emit(&mut self, instruction: Instruction) {
        assert!(
            self.current_state_id.is_some(),
            "compiler should not emit instructions after a terminal"
        );
        self.current_state.instructions.push(instruction);
    }

    pub fn reserve_state(&mut self) -> StateId {
        let state_id = self.states.len();
        self.states.push(Self::empty_state());
        state_id
    }

    pub fn switch_to(&mut self, state_id: StateId) {
        self.persist_current_state();
        self.current_state = std::mem::replace(&mut self.states[state_id], Self::empty_state());
        self.current_state_id = Some(state_id);
    }

    pub fn terminate(&mut self) {
        self.persist_current_state();
        self.current_state_id = None;
    }

    pub fn finish(mut self) -> TypedVec<StateId, waymark_vm_bytecode::State<Instruction>> {
        if self.current_state_id.is_some() {
            self.persist_current_state();
            self.current_state_id = None;
        }

        self.states
    }

    fn persist_current_state(&mut self) {
        let state_id = self
            .current_state_id
            .expect("compiler should not switch states after a terminal");
        self.states[state_id] = std::mem::replace(&mut self.current_state, Self::empty_state());
    }

    fn empty_state() -> waymark_vm_bytecode::State<Instruction> {
        waymark_vm_bytecode::State {
            instructions: TypedVec::<InstructionId, _>::with_capacity(0),
        }
    }
}
