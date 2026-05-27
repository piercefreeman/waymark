//! Range-based lowering for `for` loops.

use waymark_vm_ast_old::{ActionCall, Block, Expr, Spanned};
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_runtime_core::RegisterId;

use super::super::LoopControlKind;
use super::header::LoopBinding;
use super::{ErrorFor, ForLoopCompiler};

impl<'borrow, 'table, Spec, Lowering> ForLoopCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Compiles `range(stop)` and `range(start, stop)` loops with implicit
    /// positive step `1`.
    ///
    /// This is kept separate from [`Self::compile_stepped_range_loop`] rather
    /// than dispatched through it with a synthetic `step = 1` because the step
    /// sign is statically known here. That lets us:
    ///
    /// - Skip the runtime sign-classification chain in the condition state
    ///   (two extra comparisons, two `jump_if`s, the step-zero break edge),
    ///   plus the two auxiliary `positive_condition_state` /
    ///   `negative_condition_state` bytecode states they require.
    /// - Avoid materializing a `step` register and the zero literal used to
    ///   classify it.
    /// - Fold the continue-edge update into `emit_add_assign_immediate`, which
    ///   emits a constant `+1` via a temporary instead of an `Add` against a
    ///   persistent step register.
    ///
    /// The stepped variant could produce equivalent semantics, but only after
    /// the bytecode pass eliminated the dead negative branch, and we would
    /// still pay for the extra reserved states.
    pub(super) fn compile_positive_range_loop(
        &mut self,
        loop_vars: &[String],
        start: Option<&Spanned<Expr>>,
        end: &Spanned<Expr>,
        body: &Spanned<Block>,
        binding: LoopBinding,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let current_register = self.context.local_frame.allocate_register();
        match start {
            Some(start) => self.compile_expr_into_register(start, current_register)?,
            None => self.emit_int_literal_into_register(current_register, 0)?,
        }

        let end_register = self.context.local_frame.allocate_register();
        self.compile_expr_into_register(end, end_register)?;

        let enumerate_index_register = self.allocate_enumerate_index_register(binding)?;

        self.compile_loop_skeleton(
            body,
            |compiler, for_loop| {
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Lt,
                    current_register,
                    end_register,
                    for_loop.body_state(),
                    for_loop.loop_scope().target(LoopControlKind::Break),
                );
                Ok(())
            },
            |compiler| {
                compiler.compile_loop_bindings(
                    loop_vars,
                    binding,
                    current_register,
                    enumerate_index_register,
                )
            },
            |compiler| {
                compiler.emit_add_assign_immediate(current_register, 1)?;
                compiler.emit_enumerate_increment(enumerate_index_register)
            },
        )
    }

    /// Compiles a spread over `range(stop)` or `range(start, stop)`.
    pub(super) fn compile_positive_range_spread(
        &mut self,
        start: Option<&Spanned<Expr>>,
        end: &Spanned<Expr>,
        loop_var: &str,
        action: &ActionCall,
        promises_register: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let current_register = self.context.local_frame.allocate_register();
        match start {
            Some(start) => self.compile_expr_into_register(start, current_register)?,
            None => self.emit_int_literal_into_register(current_register, 0)?,
        }

        let end_register = self.context.local_frame.allocate_register();
        self.compile_expr_into_register(end, end_register)?;

        let empty_body = self.empty_block(end);

        self.compile_loop_skeleton(
            &empty_body,
            |compiler, for_loop| {
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Lt,
                    current_register,
                    end_register,
                    for_loop.body_state(),
                    for_loop.loop_scope().target(LoopControlKind::Break),
                );
                Ok(())
            },
            |compiler| {
                compiler.compile_spread_fanout_iteration(
                    loop_var,
                    current_register,
                    action,
                    promises_register,
                )
            },
            |compiler| compiler.emit_add_assign_immediate(current_register, 1),
        )
    }

    /// Compiles `range(start, end, step)` loops where the step direction is
    /// not known until run time.
    ///
    /// Python's `range` uses a strict comparison whose direction depends on
    /// the sign of `step`: positive steps iterate while `current < end`,
    /// negative steps while `current > end`, and a zero step raises
    /// `ValueError` (which we model as an immediate `break`, leaving runtime
    /// validation to the caller). Because `step` is a general expression, we
    /// can't pick the comparison at compile time, so the condition fans out
    /// across three bytecode states:
    ///
    /// 1. The loop's `condition_state` classifies `sign(step)`: it computes
    ///    `step > 0` and `step < 0` against a `zero_register`, jumps to the
    ///    matching bound-check state, and falls through to `break` when the
    ///    step is zero. The two comparisons live in the same state as a
    ///    fall-through chain to minimize jumps.
    /// 2. `positive_condition_state` tests `current < end` and jumps to body
    ///    or break.
    /// 3. `negative_condition_state` tests `current > end` and jumps to body
    ///    or break.
    ///
    /// `current_register`, `end_register`, and `step_register` are persistent
    /// because they are read on every iteration; the continue update mutates
    /// `current_register` in place via `current += step` (a true binary `Add`
    /// rather than an immediate, since the step is a runtime value).
    pub(super) fn compile_stepped_range_loop(
        &mut self,
        loop_vars: &[String],
        start: &Spanned<Expr>,
        end: &Spanned<Expr>,
        step: &Spanned<Expr>,
        body: &Spanned<Block>,
        binding: LoopBinding,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let current_register = self.context.local_frame.allocate_register();
        self.compile_expr_into_register(start, current_register)?;

        let end_register = self.context.local_frame.allocate_register();
        self.compile_expr_into_register(end, end_register)?;

        let step_register = self.context.local_frame.allocate_register();
        self.compile_expr_into_register(step, step_register)?;

        let enumerate_index_register = self.allocate_enumerate_index_register(binding)?;

        let positive_condition_state = self.new_state();
        let negative_condition_state = self.new_state();

        self.compile_loop_skeleton(
            body,
            |compiler, for_loop| {
                let break_target = for_loop.loop_scope().target(LoopControlKind::Break);
                let incoming_flow = for_loop.condition_flow();

                // In the condition state, classify the step sign as a
                // fall-through chain so we route to the matching bound check
                // (or to break when the step is zero) in a single state.
                let zero_register = compiler.compile_temporary_int_literal(0)?;
                let positive_register = compiler.context.local_frame.allocate_temporary_register();
                compiler.context.emitter.emit_binary(
                    BinaryOpKind::Gt,
                    positive_register.register(),
                    step_register,
                    zero_register.register(),
                );
                compiler
                    .context
                    .emitter
                    .emit_jump_if(positive_condition_state, positive_register.register());

                let negative_register = compiler.context.local_frame.allocate_temporary_register();
                compiler.context.emitter.emit_binary(
                    BinaryOpKind::Lt,
                    negative_register.register(),
                    step_register,
                    zero_register.register(),
                );
                compiler
                    .context
                    .emitter
                    .emit_jump_if(negative_condition_state, negative_register.register());
                compiler.context.emitter.emit_jump(break_target);

                compiler.switch_to_with_flow(positive_condition_state, incoming_flow.clone());
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Lt,
                    current_register,
                    end_register,
                    for_loop.body_state(),
                    break_target,
                );

                compiler.switch_to_with_flow(negative_condition_state, incoming_flow);
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Gt,
                    current_register,
                    end_register,
                    for_loop.body_state(),
                    break_target,
                );

                Ok(())
            },
            |compiler| {
                compiler.compile_loop_bindings(
                    loop_vars,
                    binding,
                    current_register,
                    enumerate_index_register,
                )
            },
            |compiler| {
                compiler.context.emitter.emit_binary(
                    BinaryOpKind::Add,
                    current_register,
                    current_register,
                    step_register,
                );
                compiler.emit_enumerate_increment(enumerate_index_register)
            },
        )
    }

    /// Compiles a spread over `range(start, end, step)`.
    pub(super) fn compile_stepped_range_spread(
        &mut self,
        start: &Spanned<Expr>,
        end: &Spanned<Expr>,
        step: &Spanned<Expr>,
        loop_var: &str,
        action: &ActionCall,
        promises_register: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let current_register = self.context.local_frame.allocate_register();
        self.compile_expr_into_register(start, current_register)?;

        let end_register = self.context.local_frame.allocate_register();
        self.compile_expr_into_register(end, end_register)?;

        let step_register = self.context.local_frame.allocate_register();
        self.compile_expr_into_register(step, step_register)?;

        let empty_body = self.empty_block(step);

        self.compile_loop_skeleton(
            &empty_body,
            |compiler, for_loop| {
                let break_target = for_loop.loop_scope().target(LoopControlKind::Break);
                let incoming_flow = for_loop.condition_flow();

                let positive_condition_state = compiler.new_state();
                let negative_condition_state = compiler.new_state();

                let zero_register = compiler.compile_temporary_int_literal(0)?;
                let positive_register = compiler.context.local_frame.allocate_temporary_register();
                compiler.context.emitter.emit_binary(
                    BinaryOpKind::Gt,
                    positive_register.register(),
                    step_register,
                    zero_register.register(),
                );
                compiler
                    .context
                    .emitter
                    .emit_jump_if(positive_condition_state, positive_register.register());

                let negative_register = compiler.context.local_frame.allocate_temporary_register();
                compiler.context.emitter.emit_binary(
                    BinaryOpKind::Lt,
                    negative_register.register(),
                    step_register,
                    zero_register.register(),
                );
                compiler
                    .context
                    .emitter
                    .emit_jump_if(negative_condition_state, negative_register.register());
                compiler.context.emitter.emit_jump(break_target);

                compiler.switch_to_with_flow(positive_condition_state, incoming_flow.clone());
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Lt,
                    current_register,
                    end_register,
                    for_loop.body_state(),
                    break_target,
                );

                compiler.switch_to_with_flow(negative_condition_state, incoming_flow);
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Gt,
                    current_register,
                    end_register,
                    for_loop.body_state(),
                    break_target,
                );

                Ok(())
            },
            |compiler| {
                compiler.compile_spread_fanout_iteration(
                    loop_var,
                    current_register,
                    action,
                    promises_register,
                )
            },
            |compiler| {
                compiler.context.emitter.emit_binary(
                    BinaryOpKind::Add,
                    current_register,
                    current_register,
                    step_register,
                );
                Ok(())
            },
        )
    }
}
