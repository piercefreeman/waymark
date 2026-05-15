//! Bytecode emitter for a single function.

use index_type::typed_vec::TypedVec;
use waymark_vm_bytecode_core::{FunctionId, StateId};
use waymark_vm_compiler_for_ast_old_core::InstructionFor;
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use super::states::FunctionStates;
use super::suspend::PromiseMarker;

/// Emits bytecode instructions into the states of a single function.
pub struct FunctionEmitter<Spec>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// State storage for the function being compiled.
    function_states: FunctionStates<InstructionFor<Spec>>,
}

impl<Spec> FunctionEmitter<Spec>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// Creates an emitter with an initial active state.
    pub fn new() -> Self {
        Self {
            function_states: FunctionStates::new(),
        }
    }

    /// Returns whether the current state can still accept instructions.
    pub fn is_active(&self) -> bool {
        self.function_states.is_active()
    }

    /// Finishes emission and returns the compiled function states.
    pub fn finish(self) -> TypedVec<StateId, waymark_vm_bytecode::State<InstructionFor<Spec>>> {
        self.function_states.finish()
    }

    /// Reserves a new empty state and returns its id.
    pub fn reserve_state(&mut self) -> StateId {
        self.function_states.reserve_state()
    }

    /// Switches emission to an already-reserved state.
    pub fn switch_to(&mut self, state_id: StateId) {
        self.function_states.switch_to(state_id);
    }

    /// Emits a constant-load instruction.
    pub fn emit_load_const(
        &mut self,
        dst: RegisterId,
        value: <Spec as waymark_vm_instructions_pureset::Spec>::ConstValue,
    ) {
        self.emit(waymark_vm_instructions_pureset::PureSet::LoadConst { dst, value }.into());
    }

    /// Emits a register copy instruction.
    pub fn emit_copy(&mut self, dst: RegisterId, src: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Copy { dst, src }.into());
    }

    /// Emits a list-construction instruction.
    pub fn emit_make_list(&mut self, dst: RegisterId, items: Vec<RegisterId>) {
        self.emit(waymark_vm_instructions_pureset::PureSet::MakeList { dst, items }.into());
    }

    /// Emits a dictionary-construction instruction.
    pub fn emit_make_dict(
        &mut self,
        dst: RegisterId,
        entries: Vec<waymark_vm_instructions_pureset::DictEntry<RegisterId>>,
    ) {
        self.emit(waymark_vm_instructions_pureset::PureSet::MakeDict { dst, entries }.into());
    }

    /// Emits a user-function call that writes a promise register.
    pub fn emit_call(
        &mut self,
        dst: Marked<RegisterId, PromiseMarker>,
        function_id: FunctionId,
        args: Vec<RegisterId>,
    ) {
        self.emit(
            waymark_vm_instructions_coreset::CoreSet::Call {
                dst: *dst,
                function_id,
                args,
            }
            .into(),
        );
    }

    /// Emits an external action call that resumes at `resume`.
    pub fn emit_extcall(
        &mut self,
        dst: Marked<RegisterId, PromiseMarker>,
        action_ref: <Spec as waymark_vm_instructions_extcallset::Spec>::ActionRef,
        args: Vec<RegisterId>,
        resume: StateId,
    ) {
        self.emit(
            waymark_vm_instructions_extcallset::ExtCallSet::ActionCall {
                dst: *dst,
                action_ref,
                args,
                resume,
            }
            .into(),
        );
    }

    /// Emits a sleep instruction that resumes at `resume`.
    pub fn emit_sleep(
        &mut self,
        dst: Marked<RegisterId, PromiseMarker>,
        duration: RegisterId,
        resume: StateId,
    ) {
        self.emit(
            waymark_vm_instructions_extcallset::ExtCallSet::Sleep {
                dst: *dst,
                duration,
                resume,
            }
            .into(),
        );
    }

    /// Emits an await instruction for a promise register.
    pub fn emit_await(
        &mut self,
        dst: RegisterId,
        src: Marked<RegisterId, PromiseMarker>,
        resume: StateId,
    ) {
        self.emit(
            waymark_vm_instructions_coreset::CoreSet::Await {
                dst,
                src: *src,
                resume,
            }
            .into(),
        );
    }

    /// Emits a conditional jump.
    pub fn emit_jump_if(&mut self, target_state: StateId, cond: RegisterId) {
        self.emit(waymark_vm_instructions_coreset::CoreSet::JumpIf { target_state, cond }.into());
    }

    /// Emits an unconditional jump and terminates the current state.
    pub fn emit_jump(&mut self, target_state: StateId) {
        self.emit(waymark_vm_instructions_coreset::CoreSet::Jump { target_state }.into());
        self.function_states.terminate();
    }

    /// Emits a return and terminates the current state.
    pub fn emit_return(&mut self, src: RegisterId) {
        self.emit(waymark_vm_instructions_coreset::CoreSet::Return { src }.into());
        self.function_states.terminate();
    }

    /// Emits a binary pureset instruction with the provided operation kind.
    pub fn emit_binary(
        &mut self,
        kind: waymark_vm_instructions_pureset::BinaryOpKind,
        dst: RegisterId,
        a: RegisterId,
        b: RegisterId,
    ) {
        self.emit(
            waymark_vm_instructions_pureset::PureSet::Binary {
                kind,
                op: waymark_vm_instructions_pureset::BinaryOp { dst, a, b },
            }
            .into(),
        );
    }

    /// Emits a unary pureset instruction with the provided operation kind.
    pub fn emit_unary(
        &mut self,
        kind: waymark_vm_instructions_pureset::UnaryOpKind,
        dst: RegisterId,
        src: RegisterId,
    ) {
        self.emit(
            waymark_vm_instructions_pureset::PureSet::Unary {
                kind,
                op: waymark_vm_instructions_pureset::UnaryOp { dst, src },
            }
            .into(),
        );
    }

    /// Appends an instruction to the current state.
    fn emit(&mut self, instruction: InstructionFor<Spec>) {
        self.function_states.emit(instruction);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use index_type::IndexType;
    use waymark_vm_bytecode_core::{FunctionId, StateId};
    use waymark_vm_compiler_for_ast_old_test_support::{TestConstValue, TestSpec};
    use waymark_vm_runtime_core::RegisterId;

    #[test]
    fn jump_terminates_the_current_state() {
        let mut emitter = FunctionEmitter::<TestSpec>::new();

        emitter.emit_jump(StateId(3));

        assert!(!emitter.is_active());
    }

    #[test]
    fn switch_to_moves_emission_into_the_target_state() {
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let next_state = emitter.reserve_state();

        emitter.emit_load_const(RegisterId(0), TestConstValue::Int(5));
        emitter.switch_to(next_state);
        emitter.emit_call(
            Marked::mark(RegisterId(1)),
            FunctionId(7),
            vec![RegisterId(0)],
        );
        emitter.emit_return(RegisterId(1));

        let states = emitter.finish();

        assert_eq!(states.len().to_scalar(), 2);
        assert_eq!(states[StateId(0)].instructions.len().to_scalar(), 1);
        assert_eq!(states[next_state].instructions.len().to_scalar(), 2);
    }
}
