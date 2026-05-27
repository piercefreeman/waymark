//! Shared control-flow scaffold for `for` loops.

use waymark_vm_ast_old::{Block, Spanned};
use waymark_vm_bytecode_core::StateId;
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_runtime_core::RegisterId;

use super::super::LoopControlKind;
use super::super::env::FlowState;
use super::super::plan::r#loop::ForLoopPlan;
use super::{ErrorFor, ForLoopCompiler};

impl<'borrow, 'table, Spec, Lowering> ForLoopCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Emits the common scaffold shared by every `for` loop lowering.
    ///
    /// The caller supplies three closures:
    /// - `emit_condition` runs starting in the condition state and must end
    ///   with control transferred to either the body state or the loop's break
    ///   target via [`emit_compare_and_branch`].
    /// - `prepare_body` runs at the top of the body state to materialize loop
    ///   variable bindings before the body block compiles.
    /// - `emit_continue_update` runs in the continue state and must advance the
    ///   loop state before the skeleton jumps back to the condition.
    pub(super) fn compile_loop_skeleton<C, B, U>(
        &mut self,
        body: &Spanned<Block>,
        emit_condition: C,
        prepare_body: B,
        emit_continue_update: U,
    ) -> Result<(), ErrorFor<Spec, Lowering>>
    where
        C: FnOnce(&mut Self, &ForLoopPlan) -> Result<(), ErrorFor<Spec, Lowering>>,
        B: FnOnce(&mut Self) -> Result<(), ErrorFor<Spec, Lowering>>,
        U: FnOnce(&mut Self) -> Result<(), ErrorFor<Spec, Lowering>>,
    {
        let incoming_flow = self.context.flow_state.clone();
        let for_loop = ForLoopPlan::new(
            &incoming_flow,
            self.new_state(),
            self.new_state(),
            self.new_state(),
            self.new_state(),
        );

        self.context.emitter.emit_jump(for_loop.condition_state());

        self.switch_to_with_flow(for_loop.condition_state(), for_loop.condition_flow());
        emit_condition(self, &for_loop)?;

        self.compile_loop_body(&for_loop, body, prepare_body)?;

        self.switch_to_with_flow(for_loop.continue_state(), for_loop.continue_flow());
        emit_continue_update(self)?;
        self.context.emitter.emit_jump(for_loop.condition_state());

        let (exit_state, exit_flow) = for_loop.finish();
        self.switch_to_with_flow(exit_state, exit_flow);

        Ok(())
    }

    /// Emits `if cmp(left, right) jump on_true else jump on_false`.
    ///
    /// Allocates a fresh temporary register for the boolean comparison result
    /// rather than reusing one supplied by the caller, since the value is
    /// consumed immediately by `emit_jump_if` and never read again. The
    /// trailing unconditional `emit_jump(on_false)` closes the current state
    /// so callers do not need to terminate it themselves.
    pub(super) fn emit_compare_and_branch(
        &mut self,
        op: BinaryOpKind,
        left: RegisterId,
        right: RegisterId,
        on_true: StateId,
        on_false: StateId,
    ) {
        let condition_register = self.context.local_frame.allocate_temporary_register();
        self.context
            .emitter
            .emit_binary(op, condition_register.register(), left, right);
        self.context
            .emitter
            .emit_jump_if(on_true, condition_register.register());
        self.context.emitter.emit_jump(on_false);
    }

    /// Compiles the loop body and routes fallthrough to the `continue` target.
    ///
    /// Pushes the loop's scope onto [`LoopControlStack`] before recursing into
    /// the nested statement compiler so any `break`/`continue` inside the body
    /// resolves to this loop's reserved states. After the body finishes, we
    /// only emit the trailing jump to the continue target when the emitter is
    /// still active (i.e. the body did not already terminate the current
    /// state via `return`, an unconditional `break`, or similar).
    fn compile_loop_body<F>(
        &mut self,
        for_loop: &ForLoopPlan,
        body: &Spanned<Block>,
        prepare_body: F,
    ) -> Result<(), ErrorFor<Spec, Lowering>>
    where
        F: FnOnce(&mut Self) -> Result<(), ErrorFor<Spec, Lowering>>,
    {
        let body_loop_scope = for_loop.loop_scope();
        let body_loop_control = self.loop_control.with_loop(body_loop_scope);

        self.switch_to_with_flow(for_loop.body_state(), for_loop.body_flow());
        prepare_body(self)?;

        let mut body_compiler = self.context.statement_compiler(body_loop_control);
        body_compiler.compile_block(body)?;

        if self.context.emitter.is_active() {
            self.context
                .emitter
                .emit_jump(body_loop_scope.target(LoopControlKind::Continue));
        }

        Ok(())
    }

    /// Switches the emitter and flow state to a reserved state id.
    pub(super) fn switch_to_with_flow(&mut self, state_id: StateId, flow_state: FlowState) {
        self.context.emitter.switch_to(state_id);
        *self.context.flow_state = flow_state;
    }

    /// Reserves a new bytecode state id.
    pub(super) fn new_state(&mut self) -> StateId {
        self.context.emitter.reserve_state()
    }
}
