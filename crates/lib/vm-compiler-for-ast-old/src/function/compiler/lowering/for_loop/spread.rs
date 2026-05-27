//! Spread-specific lowering for `for` loops.

use waymark_vm_ast_old::{ActionCall, Block, Expr, Spanned};
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use super::super::LoopControlKind;
use super::super::env::RegisterHandle;
use super::super::suspend::PromiseMarker;
use super::super::value::ResultTarget;
use super::header::{RangeLoop, ResolvedForLoop};
use super::{ErrorFor, ForLoopCompiler, IndexedSpreadJoinRegisters};

impl<'borrow, 'table, Spec, Lowering> ForLoopCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Compiles a spread statement as a looped series of action calls.
    pub fn compile_spread_statement(
        &mut self,
        iterable: &Spanned<Expr>,
        loop_var: &str,
        action: &ActionCall,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let promises_register = self.context.local_frame.allocate_register();
        self.context
            .emitter
            .emit_make_list(promises_register, Vec::new());

        let join_registers = match ResolvedForLoop::build::<Spec, Lowering>(iterable)? {
            ResolvedForLoop::Indexed { iterable, .. } => {
                Some(self.compile_indexed_spread(iterable, loop_var, action, promises_register)?)
            }
            ResolvedForLoop::Range {
                range: RangeLoop::Positive { start, end },
                ..
            } => {
                self.compile_positive_range_spread(
                    start,
                    end,
                    loop_var,
                    action,
                    promises_register,
                )?;
                None
            }
            ResolvedForLoop::Range {
                range: RangeLoop::Stepped { start, end, step },
                ..
            } => self
                .compile_stepped_range_spread(start, end, step, loop_var, action, promises_register)
                .map(|()| None)?,
        };

        self.compile_spread_join(promises_register, None, iterable, join_registers)
    }

    /// Compiles a spread expression into `result_register`.
    pub fn compile_spread_expr(
        &mut self,
        iterable: &Spanned<Expr>,
        loop_var: &str,
        action: &ActionCall,
        result_register: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.context
            .emitter
            .emit_make_list(result_register, Vec::new());

        let promises_register = self.context.local_frame.allocate_register();
        self.context
            .emitter
            .emit_make_list(promises_register, Vec::new());

        let join_registers = match ResolvedForLoop::build::<Spec, Lowering>(iterable)? {
            ResolvedForLoop::Indexed { iterable, .. } => {
                Some(self.compile_indexed_spread(iterable, loop_var, action, promises_register)?)
            }
            ResolvedForLoop::Range {
                range: RangeLoop::Positive { start, end },
                ..
            } => {
                self.compile_positive_range_spread(
                    start,
                    end,
                    loop_var,
                    action,
                    promises_register,
                )?;
                None
            }
            ResolvedForLoop::Range {
                range: RangeLoop::Stepped { start, end, step },
                ..
            } => self
                .compile_stepped_range_spread(start, end, step, loop_var, action, promises_register)
                .map(|()| None)?,
        };

        self.compile_spread_join(
            promises_register,
            Some(result_register),
            iterable,
            join_registers,
        )
    }

    /// Awaits all fan-out promises after the spread has started them.
    fn compile_spread_join(
        &mut self,
        promises_register: RegisterId,
        result_register: Option<RegisterId>,
        template: &Spanned<Expr>,
        join_registers: Option<IndexedSpreadJoinRegisters>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let (index_register, length_register) = match join_registers {
            Some(join_registers) => {
                self.emit_int_literal_into_register(join_registers.index_register, 0)?;
                (
                    join_registers.index_register,
                    join_registers.length_register,
                )
            }
            None => {
                let index_register = self.context.local_frame.allocate_register();
                self.emit_int_literal_into_register(index_register, 0)?;

                let length_register = self.context.local_frame.allocate_register();
                self.context
                    .emitter
                    .emit_length(length_register, promises_register);

                (index_register, length_register)
            }
        };

        let empty_body = self.empty_block(template);

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
                compiler.compile_spread_join_iteration(
                    promises_register,
                    index_register,
                    result_register,
                )
            },
            |compiler| compiler.emit_add_assign_immediate(index_register, 1),
        )
    }

    /// Starts one spread action and appends the pending promise to the list.
    pub(super) fn compile_spread_fanout_iteration(
        &mut self,
        loop_var: &str,
        item_register: RegisterId,
        action: &ActionCall,
        promises_register: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let promise_register = self.start_spread_action(loop_var, item_register, action)?;
        self.append_list_item(promises_register, promise_register.register());

        Ok(())
    }

    /// Awaits one previously-started spread promise and optionally collects it.
    fn compile_spread_join_iteration(
        &mut self,
        promises_register: RegisterId,
        index_register: RegisterId,
        result_register: Option<RegisterId>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let promise_register = Marked::mark(RegisterHandle::Temporary(
            self.context.local_frame.allocate_temporary_register(),
        ));
        self.context.emitter.emit_index(
            promise_register.register(),
            promises_register,
            index_register,
        );
        self.context
            .value_compiler()
            .compile_await(promise_register.register(), &promise_register);

        if let Some(result_register) = result_register {
            self.append_list_item(result_register, promise_register.register());
        }

        Ok(())
    }

    /// Starts the spread action call with the current loop item bound.
    fn start_spread_action(
        &mut self,
        loop_var: &str,
        item_register: RegisterId,
        action: &ActionCall,
    ) -> Result<Marked<RegisterHandle, PromiseMarker>, ErrorFor<Spec, Lowering>> {
        let mut value_compiler = self
            .context
            .value_compiler()
            .with_scoped_binding(loop_var, item_register);
        value_compiler.compile_action_start(action, ResultTarget::Allocate)
    }

    /// Appends one item register into a list accumulator in place.
    fn append_list_item(&mut self, list_register: RegisterId, item_register: RegisterId) {
        self.context
            .emitter
            .emit_list_append(list_register, list_register, item_register);
    }

    /// Builds an empty block that lets spread lowering reuse the shared loop skeleton.
    pub(super) fn empty_block(&self, template: &Spanned<Expr>) -> Spanned<Block> {
        Spanned {
            value: Block {
                statements: Vec::new(),
            },
            span: template.span,
        }
    }
}
