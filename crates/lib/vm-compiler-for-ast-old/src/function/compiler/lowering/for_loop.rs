//! For-loop lowering.
//!
//! # Shared skeleton
//!
//! All `for` loops share one four-state skeleton (condition, body, continue,
//! exit) so that `break`/`continue` resolution and flow-state plumbing stay
//! in one place.
//!
//! # Header variants
//!
//! Header variants exist only where they let cheaper cases skip machinery
//! the expensive ones need. In particular, a runtime-signed `range` step is
//! the only thing that forces extra condition states, so statically-known
//! steps get their own path.
//!
//! # Enumerate
//!
//! `enumerate(...)` is unwrapped during header classification to keep
//! iteration mechanics independent of variable-binding shape.

use waymark_vm_ast_old::{
    ActionCall, Block, Expr, FunctionCall, GlobalFunction, Kwarg, Literal, Spanned,
};
use waymark_vm_bytecode_core::StateId;
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use super::CompilerContextMut;
use super::ErrorFor;
use super::StatementCompiler;
use super::Unsupported;
use super::ValueCompiler;
use super::env::{FlowState, RegisterHandle};
use super::r#loop::LoopControlStack;
use super::plan::call::UnsupportedFunctionCall;
use super::plan::r#loop::ForLoopPlan;
use super::suspend::PromiseMarker;
use super::{Error, LoopControlKind};

/// Lowers `for` loops into bytecode states and register updates.
pub struct ForLoopCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Mutable compiler context for for-loop lowering.
    context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,

    /// Active loop scopes visible to nested statements.
    loop_control: LoopControlStack,

    /// Active exception-handler nesting depth while lowering this loop.
    exception_handler_depth: usize,
}

/// How a `for` loop binds values into its loop variables.
#[derive(Clone, Copy)]
enum LoopBinding {
    /// Bind the current iteration value directly.
    Value,

    /// Bind an `enumerate(...)` pair of `[index, value]`.
    Enumerate,
}

/// Validated loop-source shapes that the lowering path knows how to execute.
enum ResolvedForLoop<'expr> {
    /// Iterate an indexable iterable with an optional enumerate binding.
    Indexed {
        /// Source iterable expression.
        iterable: &'expr Spanned<Expr>,

        /// How iteration values bind to loop variables.
        binding: LoopBinding,
    },

    /// Iterate a validated `range(...)` header.
    Range {
        /// Parsed `range(...)` header.
        range: RangeLoop<'expr>,

        /// How iteration values bind to loop variables.
        binding: LoopBinding,
    },
}

/// Validated `range(...)` header shapes supported by the compiler.
enum RangeLoop<'expr> {
    /// `range(stop)` or `range(start, stop)`.
    Positive {
        /// Optional starting value. When omitted the loop starts at `0`.
        start: Option<&'expr Spanned<Expr>>,

        /// Exclusive loop bound.
        end: &'expr Spanned<Expr>,
    },

    /// `range(start, stop, step)`.
    Stepped {
        /// Starting value.
        start: &'expr Spanned<Expr>,

        /// Exclusive loop bound.
        end: &'expr Spanned<Expr>,

        /// Per-iteration increment or decrement.
        step: &'expr Spanned<Expr>,
    },
}

/// Persistent registers from indexed spread fan-out that the join can reuse.
#[derive(Clone, Copy)]
struct IndexedSpreadJoinRegisters {
    /// Loop counter register from fan-out, reset to `0` before join.
    index_register: RegisterId,

    /// Cached `len(iterable)` register from fan-out, reused as join bound.
    length_register: RegisterId,
}

impl<'expr> ResolvedForLoop<'expr> {
    /// Classifies the `for` loop header into one lowering strategy.
    fn build<Spec, Lowering>(
        iterable: &'expr Spanned<Expr>,
    ) -> Result<Self, ErrorFor<Spec, Lowering>>
    where
        Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
        Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
    {
        match &iterable.value {
            Expr::FunctionCall { call } if call.global_function == Some(GlobalFunction::Range) => {
                Self::range::<Spec, Lowering>(call, LoopBinding::Value)
            }
            Expr::FunctionCall { call }
                if call.global_function == Some(GlobalFunction::Enumerate) =>
            {
                Self::enumerate::<Spec, Lowering>(call)
            }
            _ => Ok(Self::Indexed {
                iterable,
                binding: LoopBinding::Value,
            }),
        }
    }

    /// Validates and classifies a `range(...)` call.
    fn range<Spec, Lowering>(
        call: &'expr FunctionCall,
        binding: LoopBinding,
    ) -> Result<Self, ErrorFor<Spec, Lowering>>
    where
        Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
        Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
    {
        let function = builtin_call_name(call, "range");

        if !call.kwargs.is_empty() {
            return Err(Unsupported::FunctionCall {
                name: function,
                reason: UnsupportedFunctionCall::KeywordArguments,
            }
            .into());
        }

        let range = match call.args.as_slice() {
            [] => {
                return Err(Error::FunctionArityMismatch {
                    function,
                    expected: 1,
                    actual: 0,
                });
            }
            [end] => RangeLoop::Positive { start: None, end },
            [start, end] => RangeLoop::Positive {
                start: Some(start),
                end,
            },
            [start, end, step] => RangeLoop::Stepped { start, end, step },
            _ => {
                return Err(Error::FunctionArityMismatch {
                    function,
                    expected: 3,
                    actual: call.args.len(),
                });
            }
        };

        Ok(Self::Range { range, binding })
    }

    /// Validates and classifies an `enumerate(...)` call.
    fn enumerate<Spec, Lowering>(
        call: &'expr FunctionCall,
    ) -> Result<Self, ErrorFor<Spec, Lowering>>
    where
        Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
        Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
    {
        let function = builtin_call_name(call, "enumerate");

        match (call.args.as_slice(), call.kwargs.as_slice()) {
            ([iterable], []) => {
                Self::bind_iterable::<Spec, Lowering>(iterable, LoopBinding::Enumerate)
            }
            ([], [Kwarg { name, value }]) if name == "items" => {
                Self::bind_iterable::<Spec, Lowering>(value, LoopBinding::Enumerate)
            }
            (args, []) => Err(Error::FunctionArityMismatch {
                function,
                expected: 1,
                actual: args.len(),
            }),
            _ => Err(Unsupported::FunctionCall {
                name: function,
                reason: UnsupportedFunctionCall::KeywordArguments,
            }
            .into()),
        }
    }

    /// Reclassifies the wrapped iterable for `enumerate(...)` loop lowering.
    fn bind_iterable<Spec, Lowering>(
        iterable: &'expr Spanned<Expr>,
        binding: LoopBinding,
    ) -> Result<Self, ErrorFor<Spec, Lowering>>
    where
        Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
        Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
    {
        match &iterable.value {
            Expr::FunctionCall { call } if call.global_function == Some(GlobalFunction::Range) => {
                Self::range::<Spec, Lowering>(call, binding)
            }
            _ => Ok(Self::Indexed { iterable, binding }),
        }
    }
}

impl<'borrow, 'table, Spec, Lowering> ForLoopCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Creates a for-loop compiler over the provided context and loop scope.
    pub fn new(
        context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,
        loop_control: LoopControlStack,
        exception_handler_depth: usize,
    ) -> Self {
        Self {
            context,
            loop_control,
            exception_handler_depth,
        }
    }

    /// Compiles a `for` loop using the appropriate validated header shape.
    pub fn compile(
        &mut self,
        loop_vars: &[String],
        iterable: &Spanned<Expr>,
        body: &Spanned<Block>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        match ResolvedForLoop::build::<Spec, Lowering>(iterable)? {
            ResolvedForLoop::Indexed { iterable, binding } => {
                self.compile_indexed_loop(loop_vars, iterable, body, binding)
            }
            ResolvedForLoop::Range {
                range: RangeLoop::Positive { start, end },
                binding,
            } => self.compile_positive_range_loop(loop_vars, start, end, body, binding),
            ResolvedForLoop::Range {
                range: RangeLoop::Stepped { start, end, step },
                binding,
            } => self.compile_stepped_range_loop(loop_vars, start, end, step, body, binding),
        }
    }

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
    fn compile_indexed_loop(
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
        let exception_handler_depth = self.exception_handler_depth;

        self.compile_loop_skeleton(
            body,
            |compiler, for_loop| {
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Lt,
                    index_register,
                    length_register,
                    for_loop.body_state(),
                    for_loop
                        .loop_scope(exception_handler_depth)
                        .target(LoopControlKind::Break),
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
    fn compile_indexed_spread(
        &mut self,
        iterable: &Spanned<Expr>,
        loop_var: &str,
        action: &ActionCall,
        promises_register: RegisterId,
    ) -> Result<IndexedSpreadJoinRegisters, ErrorFor<Spec, Lowering>> {
        let iterable_register = self.resolve_indexed_spread_iterable_register(iterable)?;

        let index_register = self.context.local_frame.allocate_register();
        self.emit_int_literal_into_register(index_register, 0)?;

        let length_register = self.context.local_frame.allocate_register();
        self.context
            .emitter
            .emit_length(length_register, iterable_register);

        let empty_body = self.empty_block(iterable);
        let exception_handler_depth = self.exception_handler_depth;

        self.compile_loop_skeleton(
            &empty_body,
            |compiler, for_loop| {
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Lt,
                    index_register,
                    length_register,
                    for_loop.body_state(),
                    for_loop
                        .loop_scope(exception_handler_depth)
                        .target(LoopControlKind::Break),
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

    /// Resolves the iterable register for indexed spread fan-out.
    ///
    /// A spread has no user-authored loop body, so reusing a bare
    /// local/parameter register is safe: nothing in the fan-out path can
    /// rebind that local before the loop finishes. Plain `for` loops keep the
    /// explicit copy because their body or loop bindings can overwrite the
    /// source local (for example `for items in items:`).
    fn resolve_indexed_spread_iterable_register(
        &mut self,
        iterable: &Spanned<Expr>,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        if matches!(&iterable.value, Expr::Variable { .. }) {
            return Ok(self
                .value_compiler()
                .compile_expr(iterable, super::value::ResultTarget::Allocate)?
                .register());
        }

        let iterable_register = self.context.local_frame.allocate_register();
        self.compile_expr_into_register(iterable, iterable_register)?;
        Ok(iterable_register)
    }

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
    fn compile_positive_range_loop(
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
        let exception_handler_depth = self.exception_handler_depth;

        self.compile_loop_skeleton(
            body,
            |compiler, for_loop| {
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Lt,
                    current_register,
                    end_register,
                    for_loop.body_state(),
                    for_loop
                        .loop_scope(exception_handler_depth)
                        .target(LoopControlKind::Break),
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
    fn compile_positive_range_spread(
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
        let exception_handler_depth = self.exception_handler_depth;

        self.compile_loop_skeleton(
            &empty_body,
            |compiler, for_loop| {
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Lt,
                    current_register,
                    end_register,
                    for_loop.body_state(),
                    for_loop
                        .loop_scope(exception_handler_depth)
                        .target(LoopControlKind::Break),
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
    fn compile_stepped_range_loop(
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
        let exception_handler_depth = self.exception_handler_depth;

        self.compile_loop_skeleton(
            body,
            |compiler, for_loop| {
                let break_target = for_loop
                    .loop_scope(exception_handler_depth)
                    .target(LoopControlKind::Break);
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
    fn compile_stepped_range_spread(
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
        let exception_handler_depth = self.exception_handler_depth;

        self.compile_loop_skeleton(
            &empty_body,
            |compiler, for_loop| {
                let break_target = for_loop
                    .loop_scope(exception_handler_depth)
                    .target(LoopControlKind::Break);
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
        let exception_handler_depth = self.exception_handler_depth;

        self.compile_loop_skeleton(
            &empty_body,
            |compiler, for_loop| {
                compiler.emit_compare_and_branch(
                    BinaryOpKind::Lt,
                    index_register,
                    length_register,
                    for_loop.body_state(),
                    for_loop
                        .loop_scope(exception_handler_depth)
                        .target(LoopControlKind::Break),
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

    /// Emits the common scaffold shared by every `for` loop lowering.
    ///
    /// The caller supplies three closures:
    /// - `emit_condition` runs starting in the condition state and must end
    ///   with control transferred to either the body state or the loop's break
    ///   target via [`emit_compare_and_branch`].
    /// - `prepare_body` runs at the top of the body state to materialize loop
    ///   variable bindings before the body block compiles.
    /// - `emit_continue_update` runs in the continue state and must advance the
    ///   loop state before the skeleton jumps back to the condition.
    fn compile_loop_skeleton<C, B, U>(
        &mut self,
        body: &Spanned<Block>,
        emit_condition: C,
        prepare_body: B,
        emit_continue_update: U,
    ) -> Result<(), ErrorFor<Spec, Lowering>>
    where
        C: FnOnce(&mut Self, &ForLoopPlan) -> Result<(), ErrorFor<Spec, Lowering>>,
        B: FnOnce(&mut Self) -> Result<(), ErrorFor<Spec, Lowering>>,
        U: FnOnce(&mut Self) -> Result<(), ErrorFor<Spec, Lowering>>,
    {
        let incoming_flow = self.context.flow_state.clone();
        let for_loop = ForLoopPlan::new(
            &incoming_flow,
            self.new_state(),
            self.new_state(),
            self.new_state(),
            self.new_state(),
        );

        self.context.emitter.emit_jump(for_loop.condition_state());

        self.switch_to_with_flow(for_loop.condition_state(), for_loop.condition_flow());
        emit_condition(self, &for_loop)?;

        self.compile_loop_body(&for_loop, body, prepare_body)?;

        self.switch_to_with_flow(for_loop.continue_state(), for_loop.continue_flow());
        emit_continue_update(self)?;
        self.context.emitter.emit_jump(for_loop.condition_state());

        let (exit_state, exit_flow) = for_loop.finish();
        self.switch_to_with_flow(exit_state, exit_flow);

        Ok(())
    }

    /// Emits `if cmp(left, right) jump on_true else jump on_false`.
    ///
    /// Allocates a fresh temporary register for the boolean comparison result
    /// rather than reusing one supplied by the caller, since the value is
    /// consumed immediately by `emit_jump_if` and never read again. The
    /// trailing unconditional `emit_jump(on_false)` closes the current state
    /// so callers do not need to terminate it themselves.
    fn emit_compare_and_branch(
        &mut self,
        op: BinaryOpKind,
        left: RegisterId,
        right: RegisterId,
        on_true: StateId,
        on_false: StateId,
    ) {
        let condition_register = self.context.local_frame.allocate_temporary_register();
        self.context
            .emitter
            .emit_binary(op, condition_register.register(), left, right);
        self.context
            .emitter
            .emit_jump_if(on_true, condition_register.register());
        self.context.emitter.emit_jump(on_false);
    }

    /// Increments the enumerate-index register by one if enumeration is in use.
    fn emit_enumerate_increment(
        &mut self,
        enumerate_index_register: Option<RegisterId>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        if let Some(register) = enumerate_index_register {
            self.emit_add_assign_immediate(register, 1)?;
        }
        Ok(())
    }

    /// Compiles the loop body and routes fallthrough to the `continue` target.
    ///
    /// Pushes the loop's scope onto [`LoopControlStack`] before recursing into
    /// the nested statement compiler so any `break`/`continue` inside the body
    /// resolves to this loop's reserved states. After the body finishes, we
    /// only emit the trailing jump to the continue target when the emitter is
    /// still active (i.e. the body did not already terminate the current
    /// state via `return`, an unconditional `break`, or similar).
    fn compile_loop_body<F>(
        &mut self,
        for_loop: &ForLoopPlan,
        body: &Spanned<Block>,
        prepare_body: F,
    ) -> Result<(), ErrorFor<Spec, Lowering>>
    where
        F: FnOnce(&mut Self) -> Result<(), ErrorFor<Spec, Lowering>>,
    {
        let body_loop_scope = for_loop.loop_scope(self.exception_handler_depth);
        let body_loop_control = self.loop_control.with_loop(body_loop_scope);

        self.switch_to_with_flow(for_loop.body_state(), for_loop.body_flow());
        prepare_body(self)?;

        let mut body_compiler = self.nested_statement_compiler(body_loop_control);
        body_compiler.compile_block(body)?;

        if self.context.emitter.is_active() {
            self.context
                .emitter
                .emit_jump(body_loop_scope.target(LoopControlKind::Continue));
        }

        Ok(())
    }

    /// Compiles loop-variable bindings for one iteration.
    ///
    /// Dispatches on the binding mode, delegating to
    /// [`compile_value_bindings`](Self::compile_value_bindings) for plain
    /// iteration and to
    /// [`compile_enumerate_bindings`](Self::compile_enumerate_bindings) for
    /// `enumerate(...)` headers. The `enumerate` path requires an index
    /// register; the caller is responsible for allocating it via
    /// [`allocate_enumerate_index_register`](Self::allocate_enumerate_index_register)
    /// (or, in the indexed-iteration case, reusing the loop counter) before
    /// invoking this method.
    fn compile_loop_bindings(
        &mut self,
        loop_vars: &[String],
        binding: LoopBinding,
        value_register: RegisterId,
        enumerate_index_register: Option<RegisterId>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        match binding {
            LoopBinding::Value => self.compile_value_bindings(loop_vars, value_register),
            LoopBinding::Enumerate => self.compile_enumerate_bindings(
                loop_vars,
                enumerate_index_register.expect("enumerate loops require an index register"),
                value_register,
            ),
        }
    }

    /// Compiles direct or destructured bindings from the current iteration value.
    ///
    /// Handles three shapes:
    ///
    /// - Zero loop variables (e.g. `for _ in xs` after lowering elides the
    ///   binding): emit nothing.
    /// - A single loop variable: bind the value register straight to the
    ///   local via [`bind_register_to_local`](Self::bind_register_to_local),
    ///   which elides the copy when the local already aliases the register.
    /// - Multiple loop variables (tuple destructuring): index into the value
    ///   register positionally, emitting one `emit_index` per loop local.
    fn compile_value_bindings(
        &mut self,
        loop_vars: &[String],
        value_register: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        if loop_vars.is_empty() {
            return Ok(());
        }

        if loop_vars.len() == 1 {
            self.bind_register_to_local(&loop_vars[0], value_register);
            return Ok(());
        }

        for (index, loop_var) in loop_vars.iter().enumerate() {
            let index_register = self.compile_temporary_int_literal(index as i64)?;
            let target = self
                .context
                .local_frame
                .get_or_declare_local(loop_var, self.context.flow_state);
            self.context.emitter.emit_index(
                target.register(),
                value_register,
                index_register.register(),
            );
            self.context.flow_state.mark_initialized(target);
        }

        Ok(())
    }

    /// Compiles `enumerate(...)` bindings as either a pair value or destructured
    /// index/value locals.
    ///
    /// Mirrors Python's `for x in enumerate(it)` vs `for i, v in enumerate(it)`
    /// distinction:
    ///
    /// - One loop variable: materialize `[index, value]` as a list literal
    ///   into the local so the user observes the same pair object Python
    ///   would.
    /// - Two loop variables: bind `index` and `value` directly, avoiding the
    ///   list allocation.
    /// - Three or more: build the pair list into a temporary and reuse
    ///   [`compile_value_bindings`](Self::compile_value_bindings) to project
    ///   it. This case is unreachable from well-formed Python but keeps the
    ///   lowering total instead of panicking on malformed input.
    fn compile_enumerate_bindings(
        &mut self,
        loop_vars: &[String],
        index_register: RegisterId,
        value_register: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        if loop_vars.is_empty() {
            return Ok(());
        }

        if loop_vars.len() == 1 {
            let target = self
                .context
                .local_frame
                .get_or_declare_local(&loop_vars[0], self.context.flow_state);
            self.context
                .emitter
                .emit_make_list(target.register(), vec![index_register, value_register]);
            self.context.flow_state.mark_initialized(target);
            return Ok(());
        }

        if loop_vars.len() == 2 {
            self.bind_register_to_local(&loop_vars[0], index_register);
            self.bind_register_to_local(&loop_vars[1], value_register);
            return Ok(());
        }

        let pair_register = self.context.local_frame.allocate_temporary_register();
        self.context.emitter.emit_make_list(
            pair_register.register(),
            vec![index_register, value_register],
        );
        self.compile_value_bindings(loop_vars, pair_register.register())
    }

    /// Allocates and initializes the enumerate index register when needed.
    fn allocate_enumerate_index_register(
        &mut self,
        binding: LoopBinding,
    ) -> Result<Option<RegisterId>, ErrorFor<Spec, Lowering>> {
        match binding {
            LoopBinding::Value => Ok(None),
            LoopBinding::Enumerate => {
                let register = self.context.local_frame.allocate_register();
                self.emit_int_literal_into_register(register, 0)?;
                Ok(Some(register))
            }
        }
    }

    /// Binds `source_register` into `name`, copying only when the target local
    /// uses a different register.
    ///
    /// The copy elision matters for hot loop variables: when the local was
    /// freshly declared, the register allocator typically hands back the
    /// source register itself, in which case the explicit `emit_copy` would
    /// be a no-op store-then-load. The flow-state update still runs so
    /// downstream passes see the local as initialized.
    fn bind_register_to_local(&mut self, name: &str, source_register: RegisterId) {
        let target = self
            .context
            .local_frame
            .get_or_declare_local(name, self.context.flow_state);
        if target.register() != source_register {
            self.context
                .emitter
                .emit_copy(target.register(), source_register);
        }
        self.context.flow_state.mark_initialized(target);
    }

    /// Starts one spread action and appends the pending promise to the list.
    fn compile_spread_fanout_iteration(
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
        self.value_compiler()
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
            .value_compiler()
            .with_scoped_binding(loop_var, item_register);
        value_compiler.compile_action_start(action, super::value::ResultTarget::Allocate)
    }

    /// Appends one item register into a list accumulator in place.
    fn append_list_item(&mut self, list_register: RegisterId, item_register: RegisterId) {
        self.context
            .emitter
            .emit_list_append(list_register, list_register, item_register);
    }

    /// Builds an empty block that lets spread lowering reuse the shared loop skeleton.
    fn empty_block(&self, template: &Spanned<Expr>) -> Spanned<Block> {
        Spanned {
            value: Block {
                statements: Vec::new(),
            },
            span: template.span,
        }
    }

    /// Switches the emitter and flow state to a reserved state id.
    fn switch_to_with_flow(&mut self, state_id: StateId, flow_state: FlowState) {
        self.context.emitter.switch_to(state_id);
        *self.context.flow_state = flow_state;
    }

    /// Reserves a new bytecode state id.
    fn new_state(&mut self) -> StateId {
        self.context.emitter.reserve_state()
    }

    /// Compiles an expression into the exact target register.
    ///
    /// Hints the value compiler with [`ResultTarget::Existing`] so it can emit
    /// directly into `target_register` when possible, and falls back to a
    /// trailing `emit_copy` when the compiler had to materialize the value in
    /// a different register (for example, because the expression evaluated to
    /// an existing local that the loop must not overwrite).
    fn compile_expr_into_register(
        &mut self,
        expr: &Spanned<Expr>,
        target_register: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let value_register = self
            .value_compiler()
            .compile_expr(expr, super::value::ResultTarget::Existing(target_register))?;
        if value_register.register() != target_register {
            self.context
                .emitter
                .emit_copy(target_register, value_register.register());
        }
        Ok(())
    }

    /// Emits an integer literal into the provided persistent register.
    fn emit_int_literal_into_register(
        &mut self,
        target_register: RegisterId,
        value: i64,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let literal = self.lower_int_literal(value)?;
        self.context
            .emitter
            .emit_load_const(target_register, literal);
        Ok(())
    }

    /// Compiles an integer literal into a temporary register.
    fn compile_temporary_int_literal(
        &mut self,
        value: i64,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        let register =
            RegisterHandle::Temporary(self.context.local_frame.allocate_temporary_register());
        self.emit_int_literal_into_register(register.register(), value)?;
        Ok(register)
    }

    /// Lowers an integer literal into the target VM constant representation.
    fn lower_int_literal(
        &self,
        value: i64,
    ) -> Result<<Spec as waymark_vm_instructions_pureset::Spec>::ConstValue, ErrorFor<Spec, Lowering>>
    {
        Lowering::lower_literal(&Literal::Int(value)).map_err(Error::LiteralLowering)
    }

    /// Emits `target_register = target_register + immediate`.
    ///
    /// Used for the constant `+1` step of indexed and positive-range loops,
    /// as well as the enumerate-index update. The immediate is materialized
    /// through [`compile_temporary_int_literal`](Self::compile_temporary_int_literal)
    /// so it goes through the same literal-folding path as any other integer
    /// literal in the program.
    fn emit_add_assign_immediate(
        &mut self,
        target_register: RegisterId,
        immediate: i64,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let immediate_register = self.compile_temporary_int_literal(immediate)?;
        self.context.emitter.emit_binary(
            BinaryOpKind::Add,
            target_register,
            target_register,
            immediate_register.register(),
        );
        Ok(())
    }

    /// Creates a value compiler borrowing the current context.
    fn value_compiler(&mut self) -> ValueCompiler<'_, 'table, Spec, Lowering> {
        ValueCompiler::new(self.context.reborrow_ref())
    }

    /// Creates a nested statement compiler with derived loop-control scope.
    fn nested_statement_compiler(
        &mut self,
        loop_control: LoopControlStack,
    ) -> StatementCompiler<'_, 'table, Spec, Lowering> {
        StatementCompiler::new(self.context.reborrow_mut(), loop_control)
            .with_exception_handler_depth(self.exception_handler_depth)
    }
}

/// Returns a stable function name for built-ins parsed without a textual name.
fn builtin_call_name(call: &FunctionCall, fallback: &str) -> String {
    if call.name.is_empty() {
        return fallback.to_owned();
    }

    call.name.clone()
}
