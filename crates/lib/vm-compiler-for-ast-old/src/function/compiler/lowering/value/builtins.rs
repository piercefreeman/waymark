//! Lowering for built-in global functions (`len`, `isexception`).
//!
//! These calls dispatch to dedicated bytecode opcodes rather than going
//! through the generic call path, so they live next to the value compiler
//! but are kept apart from generic expression lowering.

use waymark_vm_ast_old::{Expr, FunctionCall, Literal, Spanned};

use super::super::env::RegisterHandle;
use super::super::plan::call::UnsupportedFunctionCall;
use super::super::{Error, ErrorFor, Unsupported};
use super::{ResultTarget, ValueCompiler};

impl<'borrow, 'table, Spec, Lowering> ValueCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Compiles the built-in `len(...)` function into a dedicated pure opcode.
    pub(super) fn compile_length_call(
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

    /// Compiles the built-in `isexception(...)` function through excset.
    pub(super) fn compile_is_exception_call(
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

        match call.args.as_slice() {
            [] => Err(Error::FunctionArityMismatch {
                function: call.name.clone(),
                expected: 1,
                actual: 0,
            }),
            [value] => self.compile_any_exception_check(value, target),
            [value, exception_types] => {
                self.compile_typed_exception_check(value, exception_types, target)
            }
            _ => Err(Error::FunctionArityMismatch {
                function: call.name.clone(),
                expected: 2,
                actual: call.args.len(),
            }),
        }
    }

    /// Compiles a wildcard exception check against `value`.
    fn compile_any_exception_check(
        &mut self,
        value: &Spanned<Expr>,
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let value_register = self.compile_expr(value, ResultTarget::Allocate)?;
        let dst = self.allocate_result_register(target);

        self.context
            .emitter
            .emit_is_exception(dst.register(), value_register.register(), None);

        Ok(dst)
    }

    /// Compiles a typed exception check against one type id or a list of them.
    fn compile_typed_exception_check(
        &mut self,
        value: &Spanned<Expr>,
        exception_types: &Spanned<Expr>,
        target: ResultTarget,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let value_register = self.compile_expr(value, ResultTarget::Allocate)?;

        let type_exprs: Vec<&Spanned<Expr>> = match &exception_types.value {
            Expr::List { elements } => elements.iter().collect(),
            _ => vec![exception_types],
        };

        let Some((first_type, remaining_types)) = type_exprs.split_first() else {
            let false_literal = Literal::Bool(false);
            return self.compile_literal(&false_literal, target);
        };

        let dst = self.allocate_result_register(target);
        let first_type_register = self.compile_expr(first_type, ResultTarget::Allocate)?;
        self.context.emitter.emit_is_exception(
            dst.register(),
            value_register.register(),
            Some(first_type_register.register()),
        );

        for exception_type in remaining_types {
            let exception_type_register =
                self.compile_expr(exception_type, ResultTarget::Allocate)?;
            let matches = self.allocate_result_register(ResultTarget::Allocate);

            self.context.emitter.emit_is_exception(
                matches.register(),
                value_register.register(),
                Some(exception_type_register.register()),
            );
            self.context.emitter.emit_binary(
                waymark_vm_instructions_pureset::BinaryOpKind::Or,
                dst.register(),
                dst.register(),
                matches.register(),
            );
        }

        Ok(dst)
    }
}
