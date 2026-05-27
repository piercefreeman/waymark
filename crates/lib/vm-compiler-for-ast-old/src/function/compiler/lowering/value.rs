//! Value lowering.

mod access;
mod async_value;
mod builtins;
mod collections;
mod operators;

#[cfg(test)]
mod tests;

use waymark_vm_ast_old::{ActionCall, Expr, FunctionCall, GlobalFunction, Literal, Spanned};
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use self::async_value::AsyncValueCompiler;
use super::env::RegisterHandle;
use super::plan::call::{CallPlan, CallPlanFor};
use super::plan::expr::ExpressionPlan;
use super::suspend::PromiseMarker;
use super::{CompilerContextMut, CompilerContextRef};
use super::{Error, ErrorFor};

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

impl<'borrow, 'table, Spec, Lowering> CompilerContextRef<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Converts this shared context into a value compiler.
    pub fn into_value_compiler(self) -> ValueCompiler<'borrow, 'table, Spec, Lowering> {
        ValueCompiler::new(self)
    }
}

impl<'borrow, 'table, Spec, Lowering> CompilerContextMut<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Reborrows the context for value lowering.
    pub fn value_compiler(&mut self) -> ValueCompiler<'_, 'table, Spec, Lowering> {
        self.reborrow_ref().into_value_compiler()
    }

    /// Converts this context into a value compiler.
    pub fn into_value_compiler(self) -> ValueCompiler<'borrow, 'table, Spec, Lowering> {
        self.into_ref().into_value_compiler()
    }
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
                Some(GlobalFunction::IsException) => self.compile_is_exception_call(call, target),
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
        let call = self.plan_action_call(call)?;
        AsyncValueCompiler::new(self).compile_call_start(call, target)
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
        AsyncValueCompiler::new(self).compile_sleep_statement(duration)
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

    /// Starts a call and returns the promise register that holds its result.
    pub fn compile_call_start(
        &mut self,
        call: CallPlanFor<'_, Spec>,
        target: ResultTarget,
    ) -> Result<Marked<RegisterHandle, PromiseMarker>, ErrorFor<Spec, Lowering>> {
        AsyncValueCompiler::new(self).compile_call_start(call, target)
    }

    /// Compiles a call and awaits its result.
    fn compile_call(
        &mut self,
        call: CallPlanFor<'_, Spec>,
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        AsyncValueCompiler::new(self).compile_call(call, target)
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

    /// Emits an await into `target_register` and advances to the resume state.
    pub fn compile_await(
        &mut self,
        target_register: RegisterId,
        promise_register: &Marked<RegisterHandle, PromiseMarker>,
    ) {
        AsyncValueCompiler::new(self).compile_await(target_register, promise_register)
    }
}
