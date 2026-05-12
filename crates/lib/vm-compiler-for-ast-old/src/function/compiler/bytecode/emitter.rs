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

    /// Emits an integer/string addition instruction.
    pub fn emit_add(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Add { dst, a, b }.into());
    }

    /// Emits a subtraction instruction.
    pub fn emit_sub(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Sub { dst, a, b }.into());
    }

    /// Emits a multiplication instruction.
    pub fn emit_mul(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Mul { dst, a, b }.into());
    }

    /// Emits a division instruction.
    pub fn emit_div(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Div { dst, a, b }.into());
    }

    /// Emits a floor-division instruction.
    pub fn emit_floor_div(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::FloorDiv { dst, a, b }.into());
    }

    /// Emits a modulo instruction.
    pub fn emit_mod(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Mod { dst, a, b }.into());
    }

    /// Emits an equality comparison instruction.
    pub fn emit_eq(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Eq { dst, a, b }.into());
    }

    /// Emits an inequality comparison instruction.
    pub fn emit_ne(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Ne { dst, a, b }.into());
    }

    /// Emits a less-than comparison instruction.
    pub fn emit_lt(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Lt { dst, a, b }.into());
    }

    /// Emits a less-than-or-equal comparison instruction.
    pub fn emit_le(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Le { dst, a, b }.into());
    }

    /// Emits a greater-than comparison instruction.
    pub fn emit_gt(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Gt { dst, a, b }.into());
    }

    /// Emits a greater-than-or-equal comparison instruction.
    pub fn emit_ge(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Ge { dst, a, b }.into());
    }

    /// Emits a membership instruction.
    pub fn emit_in(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::In { dst, a, b }.into());
    }

    /// Emits a negated-membership instruction.
    pub fn emit_not_in(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::NotIn { dst, a, b }.into());
    }

    /// Emits a logical-and instruction.
    pub fn emit_and(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::And { dst, a, b }.into());
    }

    /// Emits a logical-or instruction.
    pub fn emit_or(&mut self, dst: RegisterId, a: RegisterId, b: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Or { dst, a, b }.into());
    }

    /// Emits a unary negation instruction.
    pub fn emit_neg(&mut self, dst: RegisterId, src: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Neg { dst, src }.into());
    }

    /// Emits a logical-not instruction.
    pub fn emit_not(&mut self, dst: RegisterId, src: RegisterId) {
        self.emit(waymark_vm_instructions_pureset::PureSet::Not { dst, src }.into());
    }

    /// Emits a list-construction instruction.
    pub fn emit_make_list(&mut self, dst: RegisterId, items: Vec<RegisterId>) {
        self.emit(waymark_vm_instructions_pureset::PureSet::MakeList { dst, items }.into());
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
        extcall_id: <Spec as waymark_vm_instructions_coreset::Spec>::ExtCallId,
        args: Vec<RegisterId>,
        resume: StateId,
    ) {
        self.emit(
            waymark_vm_instructions_coreset::CoreSet::ExtCall {
                dst: *dst,
                extcall_id,
                args,
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

    /// Appends an instruction to the current state.
    fn emit(&mut self, instruction: InstructionFor<Spec>) {
        self.function_states.emit(instruction);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::function::compiler::test_helpers::{TestConstValue, TestSpec};
    use index_type::IndexType;
    use waymark_vm_bytecode_core::{FunctionId, StateId};
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
