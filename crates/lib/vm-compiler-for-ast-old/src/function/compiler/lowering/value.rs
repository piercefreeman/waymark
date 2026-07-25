//! Value lowering.

use waymark_vm_ast_old::{
    ActionCall, BinaryOperator, DictEntry, Expr, FunctionCall, GlobalFunction, Literal, Spanned,
    UnaryOperator,
};
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use super::CompilerContextRef;
use super::env::RegisterHandle;
use super::plan::call::{
    ActionCallPlanFor, CallPlan, CallPlanFor, FunctionCallPlan, UnsupportedFunctionCall,
    compile_expr_registers,
};
use super::plan::expr::ExpressionPlan;
use super::suspend::PromiseMarker;
use super::{Error, ErrorFor, Unsupported};

/// Lowers expressions and calls into bytecode values and control flow.
pub struct ValueCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Shared compiler context used for value lowering.
    context: CompilerContextRef<'borrow, 'table, Spec, Lowering>,

    /// Optional register binding that shadows one local variable name.
    scoped_binding: Option<ScopedVariableBinding>,
}

/// Where an expression result should be written.
#[derive(Clone, Copy)]
pub enum ResultTarget {
    /// Allocate a fresh register for the result.
    Allocate,

    /// Write the result into an existing register.
    Existing(RegisterId),
}

/// One temporary variable binding injected by a higher-level lowering helper.
#[derive(Debug, Clone)]
struct ScopedVariableBinding {
    /// Variable name to shadow during value compilation.
    name: String,

    /// Register that should satisfy reads of `name`.
    register: RegisterId,
}

impl<'borrow, 'table, Spec, Lowering> ValueCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Creates a value compiler over the provided context.
    pub fn new(context: CompilerContextRef<'borrow, 'table, Spec, Lowering>) -> Self {
        Self {
            context,
            scoped_binding: None,
        }
    }

    /// Returns a compiler view where reads of `name` resolve to `register`.
    pub fn with_scoped_binding(mut self, name: impl Into<String>, register: RegisterId) -> Self {
        self.scoped_binding = Some(ScopedVariableBinding {
            name: name.into(),
            register,
        });
        self
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
            ExpressionPlan::List { elements } => self.compile_list_expr(elements, target),
            ExpressionPlan::Dict { entries } => self.compile_dict_expr(entries, target),
            ExpressionPlan::Index { object, index } => {
                self.compile_index_expr(object, index, target)
            }
            ExpressionPlan::Dot { object, attribute } => {
                self.compile_dot_expr(object, attribute, target)
            }
            ExpressionPlan::FunctionCall { call } => match call.global_function {
                Some(GlobalFunction::Len) => self.compile_length_call(call, target),
                Some(_) | None => self.compile_call(self.plan_function_call(call)?, target),
            },
            ExpressionPlan::ActionCall { call } => {
                self.compile_call(self.plan_action_call(call)?, target)
            }
        }
    }

    /// Compiles the `None` literal into a fresh register.
    pub fn compile_none_literal(&mut self) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        self.compile_literal(&Literal::None, ResultTarget::Allocate)
    }

    /// Compiles an action call used as a value expression.
    pub fn compile_action_expr(
        &mut self,
        call: &ActionCall,
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        self.compile_call(self.plan_action_call(call)?, target)
    }

    /// Starts an action call and returns the promise register that holds it.
    pub fn compile_action_start(
        &mut self,
        call: &ActionCall,
        target: ResultTarget,
    ) -> Result<Marked<RegisterHandle, PromiseMarker>, ErrorFor<Spec, Lowering>> {
        self.compile_call_start(self.plan_action_call(call)?, target)
    }

    /// Compiles an action call used as a statement.
    pub fn compile_action_statement(
        &mut self,
        call: &ActionCall,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let _ = self.compile_action_expr(call, ResultTarget::Allocate)?;
        Ok(())
    }

    /// Compiles a sleep statement as a dedicated async suspension point.
    pub fn compile_sleep_statement(
        &mut self,
        duration: &Spanned<Expr>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let promise_register = Marked::mark(self.allocate_result_register(ResultTarget::Allocate));
        let result_register = promise_register.register();

        self.compile_sleep_start(duration, &promise_register)?;
        self.compile_await(result_register, &promise_register);

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

    /// Starts a sleep suspension into the given promise register.
    pub fn compile_sleep_start(
        &mut self,
        duration: &Spanned<Expr>,
        dst: &Marked<RegisterHandle, PromiseMarker>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let duration_register = self.compile_expr(duration, ResultTarget::Allocate)?;

        let resume_state = self.reserve_state();

        self.context
            .emitter
            .emit_sleep(dst.marked(), duration_register.register(), resume_state);

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

    /// Compiles a list literal from recursively evaluated items.
    fn compile_list_expr(
        &mut self,
        elements: &[Spanned<Expr>],
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let item_registers = compile_expr_registers(
            elements,
            |element| element,
            |element| self.compile_expr(element, ResultTarget::Allocate),
        )?;
        let dst = self.allocate_result_register(target);

        self.context.emitter.emit_make_list(
            dst.register(),
            item_registers
                .iter()
                .map(RegisterHandle::register)
                .collect(),
        );

        Ok(dst)
    }

    /// Compiles a dictionary literal from recursively evaluated key-value pairs.
    fn compile_dict_expr(
        &mut self,
        entries: &[DictEntry],
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let mut entry_registers = Vec::with_capacity(entries.len());
        let mut compiled_entries = Vec::with_capacity(entries.len());

        for entry in entries {
            let key = self.compile_expr(&entry.key, ResultTarget::Allocate)?;
            let value = self.compile_expr(&entry.value, ResultTarget::Allocate)?;

            compiled_entries.push(waymark_vm_instructions_pureset::DictEntry {
                key: key.register(),
                value: value.register(),
            });
            entry_registers.push((key, value));
        }

        let dst = self.allocate_result_register(target);
        self.context
            .emitter
            .emit_make_dict(dst.register(), compiled_entries);

        Ok(dst)
    }

    /// Compiles the built-in `len(...)` function into a dedicated pure opcode.
    fn compile_length_call(
        &mut self,
        call: &FunctionCall,
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        if !call.kwargs.is_empty() {
            return Err(Unsupported::FunctionCall {
                name: call.name.clone(),
                reason: UnsupportedFunctionCall::KeywordArguments,
            }
            .into());
        }

        if call.args.len() != 1 {
            return Err(Error::FunctionArityMismatch {
                function: call.name.clone(),
                expected: 1,
                actual: call.args.len(),
            });
        }

        let src = self.compile_expr(&call.args[0], ResultTarget::Allocate)?;
        let dst = self.allocate_result_register(target);
        self.context
            .emitter
            .emit_length(dst.register(), src.register());

        Ok(dst)
    }

    /// Compiles an indexed-access expression from recursively evaluated operands.
    fn compile_index_expr(
        &mut self,
        object: &Spanned<Expr>,
        index: &Spanned<Expr>,
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let object_register = self.compile_expr(object, ResultTarget::Allocate)?;
        let index_register = self.compile_expr(index, ResultTarget::Allocate)?;
        let dst = self.allocate_result_register(target);

        self.context.emitter.emit_index(
            dst.register(),
            object_register.register(),
            index_register.register(),
        );

        Ok(dst)
    }

    /// Compiles an attribute-access expression from a recursively evaluated object.
    fn compile_dot_expr(
        &mut self,
        object: &Spanned<Expr>,
        attribute: &str,
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let object_register = self.compile_expr(object, ResultTarget::Allocate)?;
        let dst = self.allocate_result_register(target);

        self.context.emitter.emit_dot(
            dst.register(),
            object_register.register(),
            attribute.to_owned(),
        );

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
        if let Some(binding) = &self.scoped_binding
            && binding.name == name
        {
            return Ok(RegisterHandle::Existing(binding.register));
        }

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
    use waymark_vm_ast_old::{BinaryOperator, DictEntry, Expr};
    use waymark_vm_ast_old_helpers::{
        action_call, binary_expr, function_call, int, len_expr, spanned, string,
    };
    use waymark_vm_bytecode_core::{FunctionId, StateId};
    use waymark_vm_compiler_for_ast_old_test_support::{
        TestActionRef, TestConstValue, TestLowering, TestSpec,
    };
    use waymark_vm_instructions_coreset::CoreSet;
    use waymark_vm_instructions_extcallset::ExtCallSet;
    use waymark_vm_instructions_fullset::FullSet as InstructionSet;
    use waymark_vm_instructions_pureset::PureSet;
    use waymark_vm_runtime_core::RegisterId;

    use crate::function::compiler::{
        CompilerContextMut,
        bytecode::emitter::FunctionEmitter,
        env::{FlowState, LocalFrame},
        test_helpers::build_function_table,
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

        let mut resume_instructions = states[StateId(1)].instructions.iter();
        assert!(resume_instructions.next().is_none());
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
                        .plan_action_call(&action_call(
                            waymark_action_core::ActionRuntime::Python,
                            "fetch",
                            vec![("value", int(2))],
                        ))
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

        let mut final_instructions = states[StateId(2)].instructions.iter();
        assert!(final_instructions.next().is_none());
    }

    #[test]
    fn sleep_statements_emit_sleep_then_await_in_resume_state() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let duration = int(2);

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
                .compile_sleep_statement(&duration)
                .expect("sleep statement should compile");
        }

        let states = emitter.finish();
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
            Some(InstructionSet::ExtCallSet(ExtCallSet::Sleep {
                dst,
                duration,
                resume,
            })) if *dst == RegisterId(0)
                && *duration == RegisterId(1)
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

        let mut final_instructions = states[StateId(2)].instructions.iter();
        assert!(final_instructions.next().is_none());
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

        let mut resume_instructions = states[StateId(1)].instructions.iter();
        assert!(resume_instructions.next().is_none());
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
                        .plan_action_call(&action_call(
                            waymark_action_core::ActionRuntime::Python,
                            "fetch",
                            vec![("value", int(2))],
                        ))
                        .expect("action call should plan"),
                    ResultTarget::Existing(preferred_dst),
                )
                .expect("action call with preferred dst should compile")
        };

        let states = emitter.finish();
        assert_eq!(dst.register(), preferred_dst);
        assert_eq!(local_frame.num_registers(), 2);
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

        let mut final_instructions = states[StateId(2)].instructions.iter();
        assert!(final_instructions.next().is_none());
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
                .compile_action_statement(&action_call(
                    waymark_action_core::ActionRuntime::Python,
                    "fetch_first",
                    Vec::new(),
                ))
                .expect("first action statement should compile");
            values
                .compile_action_statement(&action_call(
                    waymark_action_core::ActionRuntime::Python,
                    "fetch_second",
                    Vec::new(),
                ))
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
                .compile_action_statement(&action_call(
                    waymark_action_core::ActionRuntime::Python,
                    "fetch_first",
                    vec![("value", int(1))],
                ))
                .expect("first action statement should compile");
            values
                .compile_action_statement(&action_call(
                    waymark_action_core::ActionRuntime::Python,
                    "fetch_second",
                    vec![("value", int(2))],
                ))
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
    fn list_expressions_emit_make_list() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let expr = spanned(Expr::List {
            elements: vec![int(1), int(2)],
        });

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
                .compile_expr(&expr, ResultTarget::Allocate)
                .expect("list expression should compile")
        };

        let states = emitter.finish();
        assert_eq!(dst.register(), RegisterId(2));
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
            Some(InstructionSet::PureSet(PureSet::MakeList { dst, items }))
                if *dst == RegisterId(2) && items == &[RegisterId(0), RegisterId(1)]
        ));
        assert!(instructions.next().is_none());
    }

    #[test]
    fn dict_expressions_emit_make_dict() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let expr = spanned(Expr::Dict {
            entries: vec![DictEntry {
                key: int(1),
                value: int(2),
            }],
        });

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
                .compile_expr(&expr, ResultTarget::Allocate)
                .expect("dict expression should compile")
        };

        let states = emitter.finish();
        assert_eq!(dst.register(), RegisterId(2));
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
            Some(InstructionSet::PureSet(PureSet::MakeDict { dst, entries }))
                if *dst == RegisterId(2)
                    && entries.len() == 1
                    && entries[0].key == RegisterId(0)
                    && entries[0].value == RegisterId(1)
        ));
        assert!(instructions.next().is_none());
    }

    #[test]
    fn global_len_function_calls_emit_length_instruction() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let expr = len_expr(spanned(Expr::List {
            elements: vec![int(1), int(2)],
        }));

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
                .compile_expr(&expr, ResultTarget::Allocate)
                .expect("len expression should compile")
        };

        let result_register = dst.register();
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
            Some(InstructionSet::PureSet(PureSet::MakeList { dst, items }))
                if *dst == RegisterId(2) && items == &[RegisterId(0), RegisterId(1)]
        ));
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::Length { dst, src }))
                if *dst == result_register && *src == RegisterId(2)
        ));
        assert!(instructions.next().is_none());
    }

    #[test]
    fn index_expressions_emit_index() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let expr = spanned(Expr::Index {
            object: Box::new(spanned(Expr::List {
                elements: vec![int(3), int(4)],
            })),
            index: Box::new(int(1)),
        });

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
                .compile_expr(&expr, ResultTarget::Allocate)
                .expect("index expression should compile")
        };

        let states = emitter.finish();

        let mut instructions = states[StateId(0)].instructions.iter();
        let first_item = match instructions.next() {
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(3),
            })) => *dst,
            other => panic!("unexpected first instruction: {other:?}"),
        };
        let second_item = match instructions.next() {
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(4),
            })) => *dst,
            other => panic!("unexpected second instruction: {other:?}"),
        };
        let list_register = match instructions.next() {
            Some(InstructionSet::PureSet(PureSet::MakeList { dst, items }))
                if items == &[first_item, second_item] =>
            {
                *dst
            }
            other => panic!("unexpected make-list instruction: {other:?}"),
        };
        let index_register = match instructions.next() {
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(1),
            })) => *dst,
            other => panic!("unexpected index literal instruction: {other:?}"),
        };
        let result_register = match instructions.next() {
            Some(InstructionSet::PureSet(PureSet::Index { dst, object, index }))
                if *object == list_register && *index == index_register =>
            {
                *dst
            }
            other => panic!("unexpected index instruction: {other:?}"),
        };

        assert_eq!(dst.register(), result_register);
        assert!(instructions.next().is_none());
    }

    #[test]
    fn dot_expressions_emit_dot() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let expr = spanned(Expr::Dot {
            object: Box::new(spanned(Expr::Dict {
                entries: vec![DictEntry {
                    key: string("field"),
                    value: int(9),
                }],
            })),
            attribute: "field".to_owned(),
        });

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
                .compile_expr(&expr, ResultTarget::Allocate)
                .expect("dot expression should compile")
        };

        let states = emitter.finish();

        let mut instructions = states[StateId(0)].instructions.iter();
        let key_register = match instructions.next() {
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::String(value),
            })) if value == "field" => *dst,
            other => panic!("unexpected dict-key instruction: {other:?}"),
        };
        let value_register = match instructions.next() {
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(9),
            })) => *dst,
            other => panic!("unexpected dict-value instruction: {other:?}"),
        };
        let object_register = match instructions.next() {
            Some(InstructionSet::PureSet(PureSet::MakeDict { dst, entries }))
                if entries.len() == 1
                    && entries[0].key == key_register
                    && entries[0].value == value_register =>
            {
                *dst
            }
            other => panic!("unexpected make-dict instruction: {other:?}"),
        };
        let result_register = match instructions.next() {
            Some(InstructionSet::PureSet(PureSet::Dot {
                dst,
                object,
                attribute,
            })) if *object == object_register && attribute == "field" => *dst,
            other => panic!("unexpected dot instruction: {other:?}"),
        };

        assert_eq!(dst.register(), result_register);
        assert!(instructions.next().is_none());
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
