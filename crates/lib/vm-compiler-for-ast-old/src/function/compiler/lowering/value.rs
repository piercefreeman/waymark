//! Value lowering.

use waymark_vm_ast_old::{
    ActionCall, BinaryOperator, Expr, FunctionCall, Literal, Spanned, UnaryOperator,
};
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use super::CompilerContextRef;
use super::env::RegisterHandle;
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
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        match ExpressionPlan::build(expr)? {
            ExpressionPlan::Literal { value } => self.compile_literal(value, target),
            ExpressionPlan::Variable { name } => Ok(self.resolve_variable(name)?),
            ExpressionPlan::BinaryOp { left, op, right } => {
                self.compile_binary_expr(left, &op, right, target)
            }
            ExpressionPlan::UnaryOp { op, operand } => {
                self.compile_unary_expr(&op, operand, target)
            }
            ExpressionPlan::FunctionCall { call } => {
                self.compile_call(self.plan_function_call(call)?, target)
            }
            ExpressionPlan::ActionCall { call } => {
                self.compile_call(self.plan_action_call(call)?, target)
            }
        }
    }

    /// Compiles the `None` literal into a fresh register.
    pub fn compile_none_literal(&mut self) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
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
        self.context.emitter.emit_return(register.register());
        Ok(())
    }

    /// Starts a user-function call into the given promise register.
    pub fn compile_function_call_start(
        &mut self,
        call: FunctionCallPlan<'_>,
        dst: &Marked<RegisterHandle, PromiseMarker>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let args = compile_expr_registers(
            call.args(),
            |arg| arg,
            |arg| self.compile_expr(arg, ResultTarget::Allocate),
        )?;
        let arg_registers = args.iter().map(RegisterHandle::register).collect();
        self.context
            .emitter
            .emit_call(dst.marked(), call.function_id(), arg_registers);
        drop(args);
        Ok(())
    }

    /// Starts an action call into the given promise register.
    pub fn compile_action_call_start(
        &mut self,
        call: ActionCallPlanFor<'_, Spec>,
        dst: &Marked<RegisterHandle, PromiseMarker>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let (action_ref, kwargs) = call.into_parts();
        let args = compile_expr_registers(
            kwargs,
            |kwarg| &kwarg.value,
            |arg| self.compile_expr(arg, ResultTarget::Allocate),
        )?;
        let arg_registers = args.iter().map(RegisterHandle::register).collect();

        let resume_state = self.reserve_state();

        self.context
            .emitter
            .emit_extcall(dst.marked(), action_ref, arg_registers, resume_state);

        drop(args);

        self.context.emitter.switch_to(resume_state);

        Ok(())
    }

    /// Starts a call and returns the promise register that holds its result.
    pub fn compile_call_start(
        &mut self,
        call: CallPlanFor<'_, Spec>,
        target: ResultTarget,
    ) -> Result<Marked<RegisterHandle, PromiseMarker>, ErrorFor<Spec, Lowering>> {
        let dst = Marked::mark(self.allocate_result_register(target));

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

    /// Compiles a scalar binary expression and releases any temporary operands.
    fn compile_binary_expr(
        &mut self,
        left: &Spanned<Expr>,
        op: &BinaryOperator,
        right: &Spanned<Expr>,
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let left_register = self.compile_expr(left, ResultTarget::Allocate)?;
        let right_register = self.compile_expr(right, ResultTarget::Allocate)?;
        let dst = self.allocate_result_register(target);
        self.emit_binary_instruction(
            op,
            dst.register(),
            left_register.register(),
            right_register.register(),
        );
        Ok(dst)
    }

    /// Compiles a scalar unary expression and releases its temporary operand.
    fn compile_unary_expr(
        &mut self,
        op: &UnaryOperator,
        operand: &Spanned<Expr>,
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let operand_register = self.compile_expr(operand, ResultTarget::Allocate)?;
        let dst = self.allocate_result_register(target);
        self.emit_unary_instruction(op, dst.register(), operand_register.register());
        Ok(dst)
    }

    /// Compiles a call and awaits its result.
    fn compile_call(
        &mut self,
        call: CallPlanFor<'_, Spec>,
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let promise_register = self.compile_call_start(call, target)?;
        let result_register = promise_register.register();

        self.compile_await(result_register, &promise_register);

        Ok(promise_register.into_register())
    }

    /// Compiles a lowered literal into the target register.
    fn compile_literal(
        &mut self,
        literal: &Literal,
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let dst = self.allocate_result_register(target);
        let value = Lowering::lower_literal(literal).map_err(Error::LiteralLowering)?;
        self.context.emitter.emit_load_const(dst.register(), value);
        Ok(dst)
    }

    /// Resolves an initialized local variable into its register.
    fn resolve_variable(&self, name: &str) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let Some(local) = self
            .context
            .local_frame
            .resolve_initialized_local(name, self.context.flow_state)
        else {
            return Err(Error::UnknownVariable {
                name: name.to_owned(),
            });
        };

        Ok(RegisterHandle::Existing(local.register()))
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
    fn allocate_result_register(&mut self, target: ResultTarget) -> RegisterHandle {
        match target {
            ResultTarget::Allocate => {
                RegisterHandle::Temporary(self.context.local_frame.allocate_temporary_register())
            }
            ResultTarget::Existing(register) => RegisterHandle::Existing(register),
        }
    }

    /// Emits a scalar binary instruction for the selected operator.
    fn emit_binary_instruction(
        &mut self,
        op: &BinaryOperator,
        dst: RegisterId,
        left: RegisterId,
        right: RegisterId,
    ) {
        let kind = match op {
            BinaryOperator::Add => waymark_vm_instructions_pureset::BinaryOpKind::Add,
            BinaryOperator::Sub => waymark_vm_instructions_pureset::BinaryOpKind::Sub,
            BinaryOperator::Mul => waymark_vm_instructions_pureset::BinaryOpKind::Mul,
            BinaryOperator::Div => waymark_vm_instructions_pureset::BinaryOpKind::Div,
            BinaryOperator::FloorDiv => waymark_vm_instructions_pureset::BinaryOpKind::FloorDiv,
            BinaryOperator::Mod => waymark_vm_instructions_pureset::BinaryOpKind::Mod,
            BinaryOperator::Eq => waymark_vm_instructions_pureset::BinaryOpKind::Eq,
            BinaryOperator::Ne => waymark_vm_instructions_pureset::BinaryOpKind::Ne,
            BinaryOperator::Lt => waymark_vm_instructions_pureset::BinaryOpKind::Lt,
            BinaryOperator::Le => waymark_vm_instructions_pureset::BinaryOpKind::Le,
            BinaryOperator::Gt => waymark_vm_instructions_pureset::BinaryOpKind::Gt,
            BinaryOperator::Ge => waymark_vm_instructions_pureset::BinaryOpKind::Ge,
            BinaryOperator::In => waymark_vm_instructions_pureset::BinaryOpKind::In,
            BinaryOperator::NotIn => waymark_vm_instructions_pureset::BinaryOpKind::NotIn,
            BinaryOperator::And => waymark_vm_instructions_pureset::BinaryOpKind::And,
            BinaryOperator::Or => waymark_vm_instructions_pureset::BinaryOpKind::Or,
        };

        self.context.emitter.emit_binary(kind, dst, left, right);
    }

    /// Emits a scalar unary instruction for the selected operator.
    fn emit_unary_instruction(&mut self, op: &UnaryOperator, dst: RegisterId, src: RegisterId) {
        let kind = match op {
            UnaryOperator::Neg => waymark_vm_instructions_pureset::UnaryOpKind::Neg,
            UnaryOperator::Not => waymark_vm_instructions_pureset::UnaryOpKind::Not,
        };

        self.context.emitter.emit_unary(kind, dst, src);
    }

    /// Emits an await into `target_register` and advances to the resume state.
    pub fn compile_await(
        &mut self,
        target_register: RegisterId,
        promise_register: &Marked<RegisterHandle, PromiseMarker>,
    ) {
        let resume_state = self.reserve_state();

        self.context
            .emitter
            .emit_await(target_register, promise_register.marked(), resume_state);

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
    use waymark_vm_ast_old::BinaryOperator;
    use waymark_vm_ast_old_helpers::{action_call, binary_expr, function_call, int};
    use waymark_vm_bytecode_core::{FunctionId, StateId};
    use waymark_vm_instructions_coreset::CoreSet;
    use waymark_vm_instructions_extcallset::ExtCallSet;
    use waymark_vm_instructions_fullset::FullSet as InstructionSet;
    use waymark_vm_instructions_pureset::PureSet;
    use waymark_vm_runtime_core::RegisterId;

    use crate::function::compiler::{
        CompilerContextMut,
        bytecode::emitter::FunctionEmitter,
        env::{FlowState, LocalFrame},
        test_helpers::{
            TestActionRef, TestConstValue, TestLowering, TestSpec, build_function_table,
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
        assert_eq!(dst.register(), RegisterId(0));
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
        assert_eq!(dst.register(), RegisterId(0));
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
            Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
                dst,
                action_ref: TestActionRef(action_ref),
                args,
                resume,
            })) if *dst == RegisterId(0)
                && action_ref == "fetch"
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
        assert_eq!(dst.register(), preferred_dst);
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
                .compile_expr(
                    &binary_expr(int(1), BinaryOperator::Add, int(2)),
                    ResultTarget::Existing(preferred_dst),
                )
                .expect("add expression with preferred dst should compile")
        };

        let states = emitter.finish();
        assert_eq!(dst.register(), preferred_dst);
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
            Some(InstructionSet::PureSet(PureSet::Binary {
                kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
                op: waymark_vm_instructions_pureset::BinaryOp { dst, a, b },
            }))
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
        assert_eq!(dst.register(), preferred_dst);
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
        assert_eq!(dst.register(), preferred_dst);
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
            Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
                dst,
                action_ref: TestActionRef(action_ref),
                args,
                resume,
            })) if *dst == preferred_dst
                && *action_ref == "fetch"
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

    #[test]
    fn expression_statements_reuse_temporary_result_registers() {
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
                .compile_expression_statement(&int(1))
                .expect("first expression statement should compile");
            values
                .compile_expression_statement(&int(2))
                .expect("second expression statement should compile");
        }

        let states = emitter.finish();
        assert_eq!(local_frame.num_registers(), 1);

        let mut instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(1),
            })) if *dst == RegisterId(0)
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(2),
            })) if *dst == RegisterId(0)
        ));
        assert!(instructions.next().is_none());
    }

    #[test]
    fn action_statements_reuse_temporary_result_registers() {
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
                .compile_action_statement(&action_call("fetch_first", Vec::new()))
                .expect("first action statement should compile");
            values
                .compile_action_statement(&action_call("fetch_second", Vec::new()))
                .expect("second action statement should compile");
        }

        let states = emitter.finish();
        assert_eq!(local_frame.num_registers(), 1);

        let mut first_state_instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            first_state_instructions.next(),
            Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
                dst,
                action_ref: TestActionRef(action_ref),
                args,
                resume,
            })) if *dst == RegisterId(0)
                && *action_ref == "fetch_first"
                && args.is_empty()
                && *resume == StateId(1)
        ));
        assert!(first_state_instructions.next().is_none());

        let mut third_state_instructions = states[StateId(2)].instructions.iter();
        assert!(matches!(
            third_state_instructions.next(),
            Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
                dst,
                action_ref: TestActionRef(action_ref),
                args,
                resume,
            })) if *dst == RegisterId(0)
                && *action_ref == "fetch_second"
                && args.is_empty()
                && *resume == StateId(3)
        ));
        assert!(third_state_instructions.next().is_none());
    }

    #[test]
    fn action_statements_reuse_temporary_argument_registers() {
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
                .compile_action_statement(&action_call("fetch_first", vec![("value", int(1))]))
                .expect("first action statement should compile");
            values
                .compile_action_statement(&action_call("fetch_second", vec![("value", int(2))]))
                .expect("second action statement should compile");
        }

        let states = emitter.finish();
        assert_eq!(local_frame.num_registers(), 2);

        let mut first_state_instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            first_state_instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(1),
            })) if *dst == RegisterId(1)
        ));
        assert!(matches!(
            first_state_instructions.next(),
            Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
                dst,
                action_ref: TestActionRef(action_ref),
                args,
                resume,
            })) if *dst == RegisterId(0)
                && *action_ref == "fetch_first"
                && args == &[RegisterId(1)]
                && *resume == StateId(1)
        ));
        assert!(first_state_instructions.next().is_none());

        let mut third_state_instructions = states[StateId(2)].instructions.iter();
        assert!(matches!(
            third_state_instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(2),
            })) if *dst == RegisterId(1)
        ));
        assert!(matches!(
            third_state_instructions.next(),
            Some(InstructionSet::ExtCallSet(ExtCallSet::ActionCall {
                dst,
                action_ref: TestActionRef(action_ref),
                args,
                resume,
            })) if *dst == RegisterId(0)
                && *action_ref == "fetch_second"
                && args == &[RegisterId(1)]
                && *resume == StateId(3)
        ));
        assert!(third_state_instructions.next().is_none());
    }

    #[test]
    fn nested_adds_release_temporary_registers_after_use() {
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
                .compile_expression_statement(&binary_expr(
                    binary_expr(int(1), BinaryOperator::Add, int(2)),
                    BinaryOperator::Add,
                    int(3),
                ))
                .expect("nested add expression statement should compile");
        }

        let states = emitter.finish();
        assert_eq!(local_frame.num_registers(), 3);

        let mut instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(1),
            })) if *dst == RegisterId(0)
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(2),
            })) if *dst == RegisterId(1)
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::Binary {
                kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
                op: waymark_vm_instructions_pureset::BinaryOp { dst, a, b },
            }))
                if *dst == RegisterId(2)
                    && *a == RegisterId(0)
                    && *b == RegisterId(1)
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(3),
            })) if *dst == RegisterId(0)
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::Binary {
                kind: waymark_vm_instructions_pureset::BinaryOpKind::Add,
                op: waymark_vm_instructions_pureset::BinaryOp { dst, a, b },
            }))
                if *dst == RegisterId(1)
                    && *a == RegisterId(2)
                    && *b == RegisterId(0)
        ));
        assert!(instructions.next().is_none());
    }
}
