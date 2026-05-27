//! Lowering for list and dict literal expressions.

use waymark_vm_ast_old::{DictEntry, Expr, Spanned};

use super::super::ErrorFor;
use super::super::env::RegisterHandle;
use super::super::plan::call::compile_expr_registers;
use super::{ResultTarget, ValueCompiler};

impl<'borrow, 'table, Spec, Lowering> ValueCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Compiles a list literal from recursively evaluated items.
    pub(super) fn compile_list_expr(
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
    pub(super) fn compile_dict_expr(
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
}
