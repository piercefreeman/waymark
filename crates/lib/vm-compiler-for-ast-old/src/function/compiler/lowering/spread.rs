//! Spread lowering.

use waymark_vm_ast_old::{ActionCall, Expr, Spanned};
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use super::CompilerContextMut;
use super::ErrorFor;
use super::ForLoopCompiler;
use super::env::{AssignmentTargetMarker, LocalSlot, RegisterHandle};
use super::r#loop::LoopControlStack;
use super::plan::call::{ActionCallPlan, CallPlan};
use super::plan::spread::SpreadPlan;

/// Lowers spread expressions and statements into bytecode.
pub struct SpreadCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Mutable compiler context for spread lowering.
    context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,
}

impl<'borrow, 'table, Spec, Lowering> SpreadCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Creates a spread compiler over the provided context.
    pub fn new(context: CompilerContextMut<'borrow, 'table, Spec, Lowering>) -> Self {
        Self { context }
    }

    /// Compiles a spread assignment and materializes the collected results.
    pub fn compile_assignment(
        &mut self,
        target: Marked<LocalSlot, AssignmentTargetMarker>,
        spread: SpreadPlan<'_>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let (collection, loop_var_name, action) = spread.into_parts();
        let promise_list_register = self.context.local_frame.allocate_register();

        self.emit_empty_list(promise_list_register);
        self.compile_collect_promise_loop(
            collection,
            loop_var_name,
            action,
            promise_list_register,
        )?;

        self.emit_empty_list(target.register());
        self.compile_await_loop(promise_list_register, Some(target.register()))?;

        target.mark_initialized(self.context.flow_state);
        Ok(())
    }

    /// Compiles a spread statement used only for side effects.
    pub fn compile_statement(
        &mut self,
        spread: SpreadPlan<'_>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let (collection, loop_var_name, action) = spread.into_parts();
        let promise_list_register = self.context.local_frame.allocate_register();

        self.emit_empty_list(promise_list_register);
        self.compile_collect_promise_loop(
            collection,
            loop_var_name,
            action,
            promise_list_register,
        )?;
        self.compile_await_loop(promise_list_register, None)?;

        Ok(())
    }

    /// Starts one action per collection item and accumulates the produced
    /// promises in source order.
    fn compile_collect_promise_loop(
        &mut self,
        collection: &Spanned<Expr>,
        loop_var_name: &str,
        action: &ActionCall,
        promise_list_register: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let action_plan = ActionCallPlan::lower::<Spec, Lowering, _>(action)?;

        self.for_loop_compiler()
            .compile_each_value(collection, move |compiler, item_register| {
                let promise_register = compiler
                    .value_compiler()
                    .with_scoped_register(loop_var_name, item_register)
                    .compile_call_start(
                        CallPlan::Action(action_plan),
                        super::value::ResultTarget::Allocate,
                    )?;
                compiler.emit_append_to_list(promise_list_register, promise_register.register());

                Ok(())
            })
    }

    /// Awaits each stored promise in order and optionally accumulates the
    /// resolved values into the provided register.
    fn compile_await_loop(
        &mut self,
        promise_list_register: RegisterId,
        result_register: Option<RegisterId>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.for_loop_compiler().compile_each_list_item(
            promise_list_register,
            move |compiler, promise_register| {
                let promise = Marked::mark(RegisterHandle::Existing(promise_register));
                let awaited_register = compiler.allocate_register();

                compiler
                    .value_compiler()
                    .compile_await(awaited_register, &promise);

                if let Some(result_register) = result_register {
                    compiler.emit_append_to_list(result_register, awaited_register);
                }

                Ok(())
            },
        )
    }

    /// Emits an empty list into `target_register`.
    fn emit_empty_list(&mut self, target_register: RegisterId) {
        self.context
            .emitter
            .emit_make_list(target_register, Vec::new());
    }

    /// Creates a for-loop compiler borrowing the current context mutably.
    fn for_loop_compiler(&mut self) -> ForLoopCompiler<'_, 'table, Spec, Lowering> {
        ForLoopCompiler::new(self.context.reborrow_mut(), LoopControlStack::new())
    }
}
