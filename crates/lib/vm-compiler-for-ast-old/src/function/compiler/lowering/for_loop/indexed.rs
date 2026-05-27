//! Indexed-iterable lowering for `for` loops.

use waymark_vm_ast_old::{ActionCall, Block, Expr, Spanned};
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_runtime_core::RegisterId;

use super::super::LoopControlKind;
use super::header::LoopBinding;
use super::{ErrorFor, ForLoopCompiler, IndexedSpreadJoinRegisters};

impl<'borrow, 'table, Spec, Lowering> ForLoopCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Compiles a `for` loop that walks an arbitrary indexable iterable
    /// (lists, tuples, strings, etc.) by stepping through `0..len(iterable)`.
    ///
    /// The lowering reserves three persistent registers up front:
    ///
    /// - `iterable_register` holds the evaluated source so we evaluate the
    ///   iterable expression exactly once, even when the body mutates locals
    ///   that the expression depends on.
    /// - `index_register` is the loop counter, initialized to `0`. It doubles
    ///   as the enumerate index when the binding is `Enumerate`, which is why
    ///   no separate enumerate register is allocated and the continue update
    ///   only increments `index_register`.
    /// - `length_register` snapshots `len(iterable)` once via
    ///   [`emit_length`](Self::context). Snapshotting matches Python's `for`
    ///   semantics and avoids re-emitting the length probe per iteration.
    ///
    /// The body prep allocates a per-iteration temporary `item_register`,
    /// emits `item = iterable[index]`, and routes both `item` and `index`
    /// through [`compile_loop_bindings`](Self::compile_loop_bindings) so the
    /// value/enumerate distinction is handled uniformly.
    pub(super) fn compile_indexed_loop(
        &mut self,
        loop_vars: &[String],
        iterable: &Spanned<Expr>,
        body: &Spanned<Block>,
        binding: LoopBinding,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let iterable_register = self.context.local_frame.allocate_register();
        self.compile_expr_into_register(iterable, iterable_register)?;

        let index_register = self.context.local_frame.allocate_register();
        self.emit_int_literal_into_register(index_register, 0)?;

        let length_register = self.context.local_frame.allocate_register();
        self.context
            .emitter
            .emit_length(length_register, iterable_register);

        self.compile_loop_skeleton(
            body,
            |compiler, for_loop| {
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Lt,
                    index_register,
                    length_register,
                    for_loop.body_state(),
                    for_loop.loop_scope().target(LoopControlKind::Break),
                );
                Ok(())
            },
            |compiler| {
                let item_register = compiler.context.local_frame.allocate_temporary_register();
                compiler.context.emitter.emit_index(
                    item_register.register(),
                    iterable_register,
                    index_register,
                );
                compiler.compile_loop_bindings(
                    loop_vars,
                    binding,
                    item_register.register(),
                    Some(index_register),
                )
            },
            |compiler| compiler.emit_add_assign_immediate(index_register, 1),
        )
    }

    /// Compiles a spread over an indexed iterable.
    pub(super) fn compile_indexed_spread(
        &mut self,
        iterable: &Spanned<Expr>,
        loop_var: &str,
        action: &ActionCall,
        promises_register: RegisterId,
    ) -> Result<IndexedSpreadJoinRegisters, ErrorFor<Spec, Lowering>> {
        let iterable_register = self.context.local_frame.allocate_register();
        self.compile_expr_into_register(iterable, iterable_register)?;

        let index_register = self.context.local_frame.allocate_register();
        self.emit_int_literal_into_register(index_register, 0)?;

        let length_register = self.context.local_frame.allocate_register();
        self.context
            .emitter
            .emit_length(length_register, iterable_register);

        let empty_body = self.empty_block(iterable);

        self.compile_loop_skeleton(
            &empty_body,
            |compiler, for_loop| {
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Lt,
                    index_register,
                    length_register,
                    for_loop.body_state(),
                    for_loop.loop_scope().target(LoopControlKind::Break),
                );
                Ok(())
            },
            |compiler| {
                let item_register = compiler.context.local_frame.allocate_temporary_register();
                compiler.context.emitter.emit_index(
                    item_register.register(),
                    iterable_register,
                    index_register,
                );
                compiler.compile_spread_fanout_iteration(
                    loop_var,
                    item_register.register(),
                    action,
                    promises_register,
                )
            },
            |compiler| compiler.emit_add_assign_immediate(index_register, 1),
        )?;

        Ok(IndexedSpreadJoinRegisters {
            index_register,
            length_register,
        })
    }
}
