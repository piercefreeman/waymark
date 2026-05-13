//! Value lowering.

use waymark_vm_ast_old::{ActionCall, Expr, FunctionCall, Literal, Spanned};
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use super::CompilerContextRef;
use super::plan::call::{
    ActionCallPlanFor, CallPlan, CallPlanFor, FunctionCallPlan, compile_expr_registers,
};
use super::plan::expr::ExpressionPlan;
use super::suspend::PromiseMarker;
use super::{Error, ErrorFor};

/// Lowers expressions and calls into bytecode values and control flow.
pub struct ValueCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Shared compiler context used for value lowering.
    context: CompilerContextRef<'borrow, 'table, Spec, Lowering>,
}

/// Where an expression result should be written.
#[derive(Clone, Copy)]
pub enum ResultTarget {
    /// Allocate a fresh register for the result.
    Allocate,

    /// Write the result into an existing register.
    Existing(RegisterId),
}

impl<'borrow, 'table, Spec, Lowering> ValueCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Creates a value compiler over the provided context.
    pub fn new(context: CompilerContextRef<'borrow, 'table, Spec, Lowering>) -> Self {
        Self { context }
    }

    /// Compiles an expression and returns the register containing its result.
    pub fn compile_expr(
        &mut self,
        expr: &Spanned<Expr>,
        target: ResultTarget,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        match ExpressionPlan::build(expr)? {
            ExpressionPlan::Literal { value } => self.compile_literal(value, target),
            ExpressionPlan::Variable { name } => self.resolve_variable(name),
            ExpressionPlan::Add { left, right } => self.compile_add_expr(left, right, target),
            ExpressionPlan::FunctionCall { call } => {
                self.compile_call(self.plan_function_call(call)?, target)
            }
            ExpressionPlan::ActionCall { call } => {
                self.compile_call(self.plan_action_call(call)?, target)
            }
        }
    }

    /// Compiles the `None` literal into a fresh register.
    pub fn compile_none_literal(&mut self) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        self.compile_literal(&Literal::None, ResultTarget::Allocate)
    }

    /// Compiles an action call used as a statement.
    pub fn compile_action_statement(
        &mut self,
        call: &ActionCall,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let _ = self.compile_call(self.plan_action_call(call)?, ResultTarget::Allocate)?;
        Ok(())
    }

    /// Compiles a return statement.
    pub fn compile_return_statement(
        &mut self,
        value: Option<&Spanned<Expr>>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.emit_return_value(value)
    }

    /// Compiles an expression statement for its side effects.
    pub fn compile_expression_statement(
        &mut self,
        expr: &Spanned<Expr>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let _ = self.compile_expr(expr, ResultTarget::Allocate)?;
        Ok(())
    }

    /// Emits a return of `None` from the current function.
    pub fn emit_return_none(&mut self) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.emit_return_value(None)
    }

    /// Emits a return instruction for an optional expression value.
    fn emit_return_value(
        &mut self,
        value: Option<&Spanned<Expr>>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let register = match value {
            Some(value) => self.compile_expr(value, ResultTarget::Allocate)?,
            None => self.compile_none_literal()?,
        };
        self.context.emitter.emit_return(register);
        Ok(())
    }

    /// Starts a user-function call into the given promise register.
    pub fn compile_function_call_start(
        &mut self,
        call: FunctionCallPlan<'_>,
        dst: Marked<RegisterId, PromiseMarker>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let args = compile_expr_registers(
            call.args(),
            |arg| arg,
            |arg| self.compile_expr(arg, ResultTarget::Allocate),
        )?;
        self.context
            .emitter
            .emit_call(dst, call.function_id(), args);
        Ok(())
    }

    /// Starts an action call into the given promise register.
    pub fn compile_action_call_start(
        &mut self,
        call: ActionCallPlanFor<'_, Spec>,
        dst: Marked<RegisterId, PromiseMarker>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let (extcall_id, kwargs) = call.into_parts();
        let args = compile_expr_registers(
            kwargs,
            |kwarg| &kwarg.value,
            |arg| self.compile_expr(arg, ResultTarget::Allocate),
        )?;

        let resume_state = self.reserve_state();

        self.context
            .emitter
            .emit_extcall(dst, extcall_id, args, resume_state);

        self.context.emitter.switch_to(resume_state);

        Ok(())
    }

    /// Starts a call and returns the promise register that holds its result.
    pub fn compile_call_start(
        &mut self,
        call: CallPlanFor<'_, Spec>,
        target: ResultTarget,
    ) -> Result<Marked<RegisterId, PromiseMarker>, ErrorFor<Spec, Lowering>> {
        let dst = self.allocate_result_register(target);
        let dst = Marked::mark(dst);

        match call {
            CallPlan::Function(call) => {
                self.compile_function_call_start(call, dst)?;
            }
            CallPlan::Action(call) => {
                self.compile_action_call_start(call, dst)?;
            }
        }

        Ok(dst)
    }

    /// Compiles an addition expression.
    fn compile_add_expr(
        &mut self,
        left: &Spanned<Expr>,
        right: &Spanned<Expr>,
        target: ResultTarget,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        let left_register = self.compile_expr(left, ResultTarget::Allocate)?;
        let right_register = self.compile_expr(right, ResultTarget::Allocate)?;
        let dst = self.allocate_result_register(target);
        self.context
            .emitter
            .emit_add(dst, left_register, right_register);
        Ok(dst)
    }

    /// Compiles a call and awaits its result.
    fn compile_call(
        &mut self,
        call: CallPlanFor<'_, Spec>,
        target: ResultTarget,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        let reg_promise = self.compile_call_start(call, target)?;

        // Place the result into the same register.
        let reg_result = Marked::unmark(reg_promise);

        self.compile_await(reg_result, reg_promise);

        Ok(reg_result)
    }

    /// Compiles a lowered literal into the target register.
    fn compile_literal(
        &mut self,
        literal: &Literal,
        target: ResultTarget,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        let dst = self.allocate_result_register(target);
        let value = Lowering::lower_literal(literal).map_err(Error::LiteralLowering)?;
        self.context.emitter.emit_load_const(dst, value);
        Ok(dst)
    }

    /// Resolves an initialized local variable into its register.
    fn resolve_variable(&self, name: &str) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        let Some(local) = self
            .context
            .local_frame
            .resolve_initialized_local(name, self.context.flow_state)
        else {
            return Err(Error::UnknownVariable {
                name: name.to_owned(),
            });
        };

        Ok(local.register())
    }

    /// Plans a user-function call against the current function table.
    fn plan_function_call<'call>(
        &self,
        call: &'call FunctionCall,
    ) -> Result<CallPlanFor<'call, Spec>, ErrorFor<Spec, Lowering>> {
        CallPlan::build_function(call, self.context.function_table)
    }

    /// Plans an action call against the current lowering implementation.
    fn plan_action_call<'call>(
        &self,
        call: &'call ActionCall,
    ) -> Result<CallPlanFor<'call, Spec>, ErrorFor<Spec, Lowering>> {
        CallPlan::build_action::<Spec, Lowering, _>(call)
    }

    /// Chooses the register where the next result should be stored.
    fn allocate_result_register(&mut self, target: ResultTarget) -> RegisterId {
        match target {
            ResultTarget::Allocate => self.context.local_frame.allocate_register(),
            ResultTarget::Existing(register) => register,
        }
    }

    /// Emits an await into `target_register` and advances to the resume state.
    pub fn compile_await(
        &mut self,
        target_register: RegisterId,
        promise_register: Marked<RegisterId, PromiseMarker>,
    ) {
        let resume_state = self.reserve_state();

        self.context
            .emitter
            .emit_await(target_register, promise_register, resume_state);

        self.context.emitter.switch_to(resume_state);
    }

    /// Reserves a new bytecode state for a future resume point.
    fn reserve_state(&mut self) -> waymark_vm_bytecode_core::StateId {
        self.context.emitter.reserve_state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use index_type::IndexType;
    use waymark_vm_ast_old_helpers::{action_call, add, function_call, int};
    use waymark_vm_bytecode_core::{FunctionId, StateId};
    use waymark_vm_instructions_coreset::CoreSet;
    use waymark_vm_instructions_fullset::FullSet as InstructionSet;
    use waymark_vm_instructions_pureset::PureSet;
    use waymark_vm_runtime_core::RegisterId;

    use crate::function::compiler::{
        CompilerContextMut,
        bytecode::emitter::FunctionEmitter,
        env::{FlowState, LocalFrame},
        test_helpers::{
            TestConstValue, TestExtCallId, TestLowering, TestSpec, build_function_table,
        },
    };

    #[test]
    fn function_calls_emit_call_then_await() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();

        let dst = {
            let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
                CompilerContextMut::new(
                    &function_table,
                    &mut emitter,
                    &mut local_frame,
                    &mut flow_state,
                )
                .into_ref(),
            );

            values
                .compile_call(
                    values
                        .plan_function_call(&function_call("child", vec![int(1)]))
                        .expect("function call should plan"),
                    ResultTarget::Allocate,
                )
                .expect("function call should compile")
        };

        let states = emitter.finish();
        assert_eq!(dst, RegisterId(0));
        assert_eq!(states.len().to_scalar(), 2);

        let mut instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(1),
            })) if *dst == RegisterId(1)
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Call {
                dst,
                function_id,
                args,
            })) if *dst == RegisterId(0)
                && *function_id == FunctionId(0)
                && args == &[RegisterId(1)]
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Await { dst, src, resume }))
                if *dst == RegisterId(0)
                    && *src == RegisterId(0)
                    && *resume == StateId(1)
        ));
        assert!(instructions.next().is_none());
    }

    #[test]
    fn action_calls_emit_extcall_then_await_in_resume_state() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();

        let dst = {
            let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
                CompilerContextMut::new(
                    &function_table,
                    &mut emitter,
                    &mut local_frame,
                    &mut flow_state,
                )
                .into_ref(),
            );

            values
                .compile_call(
                    values
                        .plan_action_call(&action_call("fetch", vec![("value", int(2))]))
                        .expect("action call should plan"),
                    ResultTarget::Allocate,
                )
                .expect("action call should compile")
        };

        let states = emitter.finish();
        assert_eq!(dst, RegisterId(0));
        assert_eq!(states.len().to_scalar(), 3);

        let mut start_instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            start_instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(2),
            })) if *dst == RegisterId(1)
        ));
        assert!(matches!(
            start_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::ExtCall {
                dst,
                extcall_id: TestExtCallId(extcall_id),
                args,
                resume,
            })) if *dst == RegisterId(0)
                && extcall_id == "fetch"
                && args == &[RegisterId(1)]
                && *resume == StateId(1)
        ));
        assert!(start_instructions.next().is_none());

        let mut resume_instructions = states[StateId(1)].instructions.iter();
        assert!(matches!(
            resume_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Await { dst, src, resume }))
                if *dst == RegisterId(0)
                    && *src == RegisterId(0)
                    && *resume == StateId(2)
        ));
        assert!(resume_instructions.next().is_none());
    }

    #[test]
    fn return_statements_without_values_emit_none_return() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();

        {
            let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
                CompilerContextMut::new(
                    &function_table,
                    &mut emitter,
                    &mut local_frame,
                    &mut flow_state,
                )
                .into_ref(),
            );

            values
                .compile_return_statement(None)
                .expect("return without a value should compile");
        }

        let states = emitter.finish();
        assert_eq!(states.len().to_scalar(), 1);

        let mut instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::None,
            })) if *dst == RegisterId(0)
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Return { src })) if *src == RegisterId(0)
        ));
        assert!(instructions.next().is_none());
    }

    #[test]
    fn literals_use_preferred_dst_without_allocating_more_registers() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let preferred_dst = local_frame.allocate_register();
        let mut flow_state = FlowState::new();

        let dst = {
            let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
                CompilerContextMut::new(
                    &function_table,
                    &mut emitter,
                    &mut local_frame,
                    &mut flow_state,
                )
                .into_ref(),
            );

            values
                .compile_expr(&int(7), ResultTarget::Existing(preferred_dst))
                .expect("literal with preferred dst should compile")
        };

        let states = emitter.finish();
        assert_eq!(dst, preferred_dst);
        assert_eq!(local_frame.num_registers(), 1);

        let mut instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(7),
            })) if *dst == preferred_dst
        ));
        assert!(instructions.next().is_none());
    }

    #[test]
    fn add_expressions_use_preferred_dst_for_the_result_register() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let preferred_dst = local_frame.allocate_register();
        let mut flow_state = FlowState::new();

        let dst = {
            let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
                CompilerContextMut::new(
                    &function_table,
                    &mut emitter,
                    &mut local_frame,
                    &mut flow_state,
                )
                .into_ref(),
            );

            values
                .compile_expr(&add(int(1), int(2)), ResultTarget::Existing(preferred_dst))
                .expect("add expression with preferred dst should compile")
        };

        let states = emitter.finish();
        assert_eq!(dst, preferred_dst);
        assert_eq!(local_frame.num_registers(), 3);

        let mut instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(1),
            })) if *dst == RegisterId(1)
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(2),
            })) if *dst == RegisterId(2)
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::Add { dst, a, b }))
                if *dst == preferred_dst
                    && *a == RegisterId(1)
                    && *b == RegisterId(2)
        ));
        assert!(instructions.next().is_none());
    }

    #[test]
    fn function_calls_use_preferred_dst_for_the_result_register() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let preferred_dst = local_frame.allocate_register();
        let mut flow_state = FlowState::new();

        let dst = {
            let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
                CompilerContextMut::new(
                    &function_table,
                    &mut emitter,
                    &mut local_frame,
                    &mut flow_state,
                )
                .into_ref(),
            );

            values
                .compile_call(
                    values
                        .plan_function_call(&function_call("child", vec![int(1)]))
                        .expect("function call should plan"),
                    ResultTarget::Existing(preferred_dst),
                )
                .expect("function call with preferred dst should compile")
        };

        let states = emitter.finish();
        assert_eq!(dst, preferred_dst);
        assert_eq!(local_frame.num_registers(), 2);

        let mut instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(1),
            })) if *dst == RegisterId(1)
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Call {
                dst,
                function_id,
                args,
            })) if *dst == preferred_dst
                && *function_id == FunctionId(0)
                && args == &[RegisterId(1)]
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Await { dst, src, resume }))
                if *dst == preferred_dst
                    && *src == preferred_dst
                    && *resume == StateId(1)
        ));
        assert!(instructions.next().is_none());
    }

    #[test]
    fn action_calls_use_preferred_dst_for_the_result_register() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let preferred_dst = local_frame.allocate_register();
        let mut flow_state = FlowState::new();

        let dst = {
            let mut values = ValueCompiler::<TestSpec, TestLowering>::new(
                CompilerContextMut::new(
                    &function_table,
                    &mut emitter,
                    &mut local_frame,
                    &mut flow_state,
                )
                .into_ref(),
            );

            values
                .compile_call(
                    values
                        .plan_action_call(&action_call("fetch", vec![("value", int(2))]))
                        .expect("action call should plan"),
                    ResultTarget::Existing(preferred_dst),
                )
                .expect("action call with preferred dst should compile")
        };

        let states = emitter.finish();
        assert_eq!(dst, preferred_dst);
        assert_eq!(local_frame.num_registers(), 2);

        let mut start_instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            start_instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(2),
            })) if *dst == RegisterId(1)
        ));
        assert!(matches!(
            start_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::ExtCall {
                dst,
                extcall_id: TestExtCallId(extcall_id),
                args,
                resume,
            })) if *dst == preferred_dst
                && *extcall_id == "fetch"
                && args == &[RegisterId(1)]
                && *resume == StateId(1)
        ));
        assert!(start_instructions.next().is_none());

        let mut resume_instructions = states[StateId(1)].instructions.iter();
        assert!(matches!(
            resume_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Await { dst, src, resume }))
                if *dst == preferred_dst
                    && *src == preferred_dst
                    && *resume == StateId(2)
        ));
        assert!(resume_instructions.next().is_none());
    }
}
