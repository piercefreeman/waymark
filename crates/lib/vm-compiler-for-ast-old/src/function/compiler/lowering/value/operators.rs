//! Lowering for scalar binary and unary operator expressions.

use waymark_vm_ast_old::{BinaryOperator, Expr, Spanned, UnaryOperator};
use waymark_vm_runtime_core::RegisterId;

use super::super::ErrorFor;
use super::super::env::RegisterHandle;
use super::{ResultTarget, ValueCompiler};

impl<'borrow, 'table, Spec, Lowering> ValueCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Compiles a scalar binary expression and releases any temporary operands.
    pub(super) fn compile_binary_expr(
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
    pub(super) fn compile_unary_expr(
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
}
