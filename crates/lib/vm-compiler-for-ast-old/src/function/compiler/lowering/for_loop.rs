//! For-loop lowering.

use waymark_vm_ast_old::{
    Block, Expr, FunctionCall, GlobalFunction, Kwarg, Literal, Span, Spanned,
};
use waymark_vm_bytecode_core::StateId;
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_runtime_core::RegisterId;

use super::CompilerContextMut;
use super::ErrorFor;
use super::StatementCompiler;
use super::Unsupported;
use super::ValueCompiler;
use super::env::{FlowState, RegisterHandle};
use super::r#loop::LoopControlStack;
use super::plan::call::UnsupportedFunctionCall;
use super::plan::r#loop::ForLoopPlan;
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
    ) -> Self {
        Self {
            context,
            loop_control,
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

    /// Compiles a `for` loop that walks an indexable iterable.
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

    /// Compiles `range(stop)` and `range(start, stop)` loops with implicit
    /// positive step `1`.
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

    /// Compiles `range(start, end, step)` loops with runtime sign checks.
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

    /// Emits the common scaffold shared by every `for` loop lowering.
    ///
    /// The caller supplies three closures:
    /// * `emit_condition` runs starting in the condition state and must end
    ///   with control transferred to either the body state or the loop's break
    ///   target via [`emit_compare_and_branch`].
    /// * `prepare_body` runs at the top of the body state to materialize loop
    ///   variable bindings before the body block compiles.
    /// * `emit_continue_update` runs in the continue state and must advance the
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
    fn compile_loop_body<F>(
        &mut self,
        for_loop: &ForLoopPlan,
        body: &Spanned<Block>,
        prepare_body: F,
    ) -> Result<(), ErrorFor<Spec, Lowering>>
    where
        F: FnOnce(&mut Self) -> Result<(), ErrorFor<Spec, Lowering>>,
    {
        let body_loop_scope = for_loop.loop_scope();
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
        self.compile_expr_into_register(&synthetic_int_expr(value), target_register)
    }

    /// Compiles an integer literal into a temporary register.
    fn compile_temporary_int_literal(
        &mut self,
        value: i64,
    ) -> Result<RegisterHandle, ErrorFor<Spec, Lowering>> {
        self.value_compiler().compile_expr(
            &synthetic_int_expr(value),
            super::value::ResultTarget::Allocate,
        )
    }

    /// Emits `target_register = target_register + immediate`.
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
    }
}

/// Returns a stable function name for built-ins parsed without a textual name.
fn builtin_call_name(call: &FunctionCall, fallback: &str) -> String {
    if call.name.is_empty() {
        return fallback.to_owned();
    }

    call.name.clone()
}

/// Creates a synthetic integer literal expression for compiler-internal lowering.
fn synthetic_int_expr(value: i64) -> Spanned<Expr> {
    Spanned {
        value: Expr::Literal {
            value: Literal::Int(value),
        },
        span: Span {
            start_line: 0,
            start_col: 0,
            end_line: 0,
            end_col: 0,
        },
    }
}
