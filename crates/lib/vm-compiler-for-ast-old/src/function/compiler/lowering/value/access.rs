//! Lowering for index and attribute access expressions.

use waymark_vm_ast_old::{Expr, Spanned};

use super::super::ErrorFor;
use super::super::env::RegisterHandle;
use super::{ResultTarget, ValueCompiler};

impl<'borrow, 'table, Spec, Lowering> ValueCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Compiles an indexed-access expression from recursively evaluated operands.
    pub(super) fn compile_index_expr(
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
    pub(super) fn compile_dot_expr(
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
}
