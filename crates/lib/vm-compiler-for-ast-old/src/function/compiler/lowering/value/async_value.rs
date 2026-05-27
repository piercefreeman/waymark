//! Async value lowering.

use waymark_vm_ast_old::Expr;
use waymark_vm_ast_old::Spanned;
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use super::super::ErrorFor;
use super::super::env::RegisterHandle;
use super::super::exception::ExceptionScopeStack;
use super::super::plan::call::{
    ActionCallPlanFor, CallPlan, CallPlanFor, FunctionCallPlan, compile_expr_registers,
};
use super::super::suspend::PromiseMarker;
use super::{ResultTarget, ValueCompiler};

/// Lowers async call start, await, and exception-dispatch behavior for values.
pub(super) struct AsyncValueCompiler<'compiler, 'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Parent value compiler that owns expression lowering state.
    values: &'compiler mut ValueCompiler<'borrow, 'table, Spec, Lowering>,
}

impl<'compiler, 'borrow, 'table, Spec, Lowering>
    AsyncValueCompiler<'compiler, 'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Creates an async-lowering helper over one value compiler.
    pub(super) fn new(
        values: &'compiler mut ValueCompiler<'borrow, 'table, Spec, Lowering>,
    ) -> Self {
        Self { values }
    }

    /// Compiles a sleep statement as a dedicated async suspension point.
    pub(super) fn compile_sleep_statement(
        &mut self,
        duration: &Spanned<Expr>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let promise_register =
            Marked::mark(self.values.allocate_result_register(ResultTarget::Allocate));
        let result_register = promise_register.register();

        self.compile_sleep_start(duration, &promise_register)?;
        self.compile_await(result_register, &promise_register);

        Ok(())
    }

    /// Compiles a call and awaits its result.
    pub(super) fn compile_call(
        &mut self,
        call: CallPlanFor<'_, Spec>,
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let promise_register = self.compile_call_start(call, target)?;
        let result_register = promise_register.register();

        self.compile_await(result_register, &promise_register);

        Ok(promise_register.into_register())
    }

    /// Starts a user-function call into the given promise register.
    pub(super) fn compile_function_call_start(
        &mut self,
        call: FunctionCallPlan<'_>,
        dst: &Marked<RegisterHandle, PromiseMarker>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let args = compile_expr_registers(
            call.args(),
            |arg| arg,
            |arg| self.values.compile_expr(arg, ResultTarget::Allocate),
        )?;
        let arg_registers = args.iter().map(RegisterHandle::register).collect();
        self.values
            .context
            .emitter
            .emit_call(dst.marked(), call.function_id(), arg_registers);
        drop(args);
        Ok(())
    }

    /// Starts an action call into the given promise register.
    pub(super) fn compile_action_call_start(
        &mut self,
        call: ActionCallPlanFor<'_, Spec>,
        dst: &Marked<RegisterHandle, PromiseMarker>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let (action_ref, kwargs) = call.into_parts();
        let args = compile_expr_registers(
            kwargs,
            |kwarg| &kwarg.value,
            |arg| self.values.compile_expr(arg, ResultTarget::Allocate),
        )?;
        let arg_registers = args.iter().map(RegisterHandle::register).collect();

        let resume_state = self.reserve_state();

        self.values.context.emitter.emit_extcall(
            dst.marked(),
            action_ref,
            arg_registers,
            resume_state,
        );

        drop(args);

        self.values.context.emitter.switch_to(resume_state);

        Ok(())
    }

    /// Starts a sleep suspension into the given promise register.
    pub(super) fn compile_sleep_start(
        &mut self,
        duration: &Spanned<Expr>,
        dst: &Marked<RegisterHandle, PromiseMarker>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let duration_register = self.values.compile_expr(duration, ResultTarget::Allocate)?;

        let resume_state = self.reserve_state();

        self.values.context.emitter.emit_sleep(
            dst.marked(),
            duration_register.register(),
            resume_state,
        );

        self.values.context.emitter.switch_to(resume_state);

        Ok(())
    }

    /// Starts a call and returns the promise register that holds its result.
    pub(super) fn compile_call_start(
        &mut self,
        call: CallPlanFor<'_, Spec>,
        target: ResultTarget,
    ) -> Result<Marked<RegisterHandle, PromiseMarker>, ErrorFor<Spec, Lowering>> {
        let dst = Marked::mark(self.values.allocate_result_register(target));

        match call {
            CallPlan::Function(call) => {
                self.compile_function_call_start(call, &dst)?;
            }
            CallPlan::Action(call) => {
                self.compile_action_call_start(call, &dst)?;
            }
        }

        Ok(dst)
    }

    /// Emits an await into `target_register` and advances to the resume state.
    pub(super) fn compile_await(
        &mut self,
        target_register: RegisterId,
        promise_register: &Marked<RegisterHandle, PromiseMarker>,
    ) {
        let resume_state = self.reserve_state();

        self.values.context.emitter.emit_await(
            target_register,
            promise_register.marked(),
            resume_state,
        );

        self.values.context.emitter.switch_to(resume_state);
        self.compile_exception_dispatch_if_needed(target_register);
    }

    /// Emits exception dispatch after an awaited result if a try-scope is active.
    fn compile_exception_dispatch_if_needed(&mut self, result_register: RegisterId) {
        let Some(scope) = self.values.context.exception_scope.current_scope() else {
            return;
        };

        let continue_state = self.reserve_state();
        let exception_state = self.reserve_state();
        let is_exception = self.values.allocate_result_register(ResultTarget::Allocate);

        self.values.context.emitter.emit_is_exception(
            is_exception.register(),
            result_register,
            None,
        );
        self.values
            .context
            .emitter
            .emit_jump_if(exception_state, is_exception.register());
        self.values.context.emitter.emit_jump(continue_state);

        self.values.context.emitter.switch_to(exception_state);
        if scope.exception_register() != result_register {
            self.values
                .context
                .emitter
                .emit_copy(scope.exception_register(), result_register);
        }
        let exception_scope = self.values.context.exception_scope.clone();
        self.emit_known_exception_dispatch(&exception_scope, scope.exception_register());
        self.values.context.emitter.switch_to(continue_state);
    }

    /// Emits handler dispatch for a value already known to be an exception.
    fn emit_known_exception_dispatch(
        &mut self,
        exception_scope: &ExceptionScopeStack,
        exception_register: RegisterId,
    ) {
        let Some(scope) = exception_scope.current_scope() else {
            self.values.context.emitter.emit_return(exception_register);
            return;
        };

        let entry_flow = self.values.context.flow_state;

        for (handler_index, handler) in scope.handlers().iter().enumerate() {
            scope.record_handler_flow(handler_index, entry_flow);

            if handler.is_catch_all() {
                self.values.context.emitter.emit_jump(handler.entry_state());
                return;
            }

            for &exception_type_register in handler.exception_type_registers() {
                let matches = self.values.allocate_result_register(ResultTarget::Allocate);
                self.values.context.emitter.emit_is_exception(
                    matches.register(),
                    exception_register,
                    Some(exception_type_register),
                );
                self.values
                    .context
                    .emitter
                    .emit_jump_if(handler.entry_state(), matches.register());
            }
        }

        let outer_scope = scope.outer();
        if outer_scope.is_empty() {
            self.values.context.emitter.emit_return(exception_register);
            return;
        }

        let outer = outer_scope
            .current_scope()
            .expect("non-empty outer exception scope should exist");
        if outer.exception_register() != exception_register {
            self.values
                .context
                .emitter
                .emit_copy(outer.exception_register(), exception_register);
        }

        self.emit_known_exception_dispatch(&outer_scope, outer.exception_register());
    }

    /// Reserves a new bytecode state for a future resume point.
    fn reserve_state(&mut self) -> waymark_vm_bytecode_core::StateId {
        self.values.context.emitter.reserve_state()
    }
}
