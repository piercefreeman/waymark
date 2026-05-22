//! Spread lowering.

use waymark_vm_ast_old::{
    ActionCall, BinaryOperator, Block, DictEntry, Expr, FunctionCall, Kwarg, Span, Spanned,
};
use waymark_vm_bytecode_core::StateId;
use waymark_vm_instructions_pureset::BinaryOpKind;
use waymark_vm_runtime_core::RegisterId;

use crate::Marked;

use super::AssignmentCompiler;
use super::CompilerContextMut;
use super::ErrorFor;
use super::ForLoopCompiler;
use super::LoopControlKind;
use super::ValueCompiler;
use super::env::{AssignmentTargetMarker, LocalSlot, RegisterHandle};
use super::r#loop::LoopControlStack;
use super::plan::call::{ActionCallPlan, CallPlan};
use super::plan::r#loop::ForLoopPlan;
use super::plan::spread::SpreadPlan;

/// Lowers spread expressions and statements into bytecode.
pub struct SpreadCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Mutable compiler context for spread lowering.
    context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,
}

impl<'borrow, 'table, Spec, Lowering> SpreadCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Creates a spread compiler over the provided context.
    pub fn new(context: CompilerContextMut<'borrow, 'table, Spec, Lowering>) -> Self {
        Self { context }
    }

    /// Compiles a spread assignment and materializes the collected results.
    pub fn compile_assignment(
        &mut self,
        target: Marked<LocalSlot, AssignmentTargetMarker>,
        spread: SpreadPlan<'_>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let (collection_register, loop_var_name, rewritten_action) = self.prepare_spread(spread)?;
        let promise_list_register = self.context.local_frame.allocate_register();

        self.emit_empty_list(promise_list_register);
        self.compile_collect_promise_loop(
            collection_register,
            &loop_var_name,
            &rewritten_action,
            promise_list_register,
        )?;

        self.emit_empty_list(target.register());
        self.compile_await_loop(promise_list_register, Some(target.register()))?;

        target.mark_initialized(self.context.flow_state);
        Ok(())
    }

    /// Compiles a spread statement used only for side effects.
    pub fn compile_statement(
        &mut self,
        spread: SpreadPlan<'_>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let (collection_register, loop_var_name, rewritten_action) = self.prepare_spread(spread)?;
        let promise_list_register = self.context.local_frame.allocate_register();

        self.emit_empty_list(promise_list_register);
        self.compile_collect_promise_loop(
            collection_register,
            &loop_var_name,
            &rewritten_action,
            promise_list_register,
        )?;
        self.compile_await_loop(promise_list_register, None)?;

        Ok(())
    }

    /// Materializes the collection and rewrites loop-variable references onto
    /// an internal binding that does not leak outside the spread.
    fn prepare_spread(
        &mut self,
        spread: SpreadPlan<'_>,
    ) -> Result<(RegisterId, String, ActionCall), ErrorFor<Spec, Lowering>> {
        let (collection, loop_var, action) = spread.into_parts();
        let collection_register = self.materialize_collection(collection)?;
        let internal_loop_var = self.synthetic_name("spread_loop_var");
        let rewritten_action = rewrite_action_call_loop_var(action, loop_var, &internal_loop_var);

        Ok((collection_register, internal_loop_var, rewritten_action))
    }

    /// Converts the spread collection into a stable list register using the
    /// existing `for`-loop lowering path.
    fn materialize_collection(
        &mut self,
        collection: &Spanned<Expr>,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        let incoming_flow = self.context.flow_state.clone();
        let collection_name = self.synthetic_name("spread_collection");
        let item_name = self.synthetic_name("spread_source_item");
        let empty_list = synthetic_list_expr(Vec::new());
        let append_value = synthetic_binary_add_expr(
            synthetic_variable_expr(&collection_name),
            synthetic_list_expr(vec![synthetic_variable_expr(&item_name)]),
        );
        let append_body = synthetic_block(vec![synthetic_assignment_statement(
            collection_name.clone(),
            append_value,
        )]);

        self.assignment_compiler()
            .compile_statement(std::slice::from_ref(&collection_name), &empty_list)?;
        self.for_loop_compiler()
            .compile(&[item_name], collection, &append_body)?;

        let register = self
            .context
            .local_frame
            .resolve_initialized_local(&collection_name, self.context.flow_state)
            .expect("internal spread collection should be initialized")
            .register();

        *self.context.flow_state = incoming_flow;
        Ok(register)
    }

    /// Starts one action per collection item and accumulates the produced
    /// promises in source order.
    fn compile_collect_promise_loop(
        &mut self,
        collection_register: RegisterId,
        loop_var_name: &str,
        action: &ActionCall,
        promise_list_register: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let action_plan = ActionCallPlan::lower::<Spec, Lowering, _>(action)?;

        self.compile_indexed_loop(collection_register, move |compiler, item_register| {
            compiler.bind_register_to_local(loop_var_name, item_register);

            let promise_register = compiler.value_compiler().compile_call_start(
                CallPlan::Action(action_plan),
                super::value::ResultTarget::Allocate,
            )?;
            compiler.append_to_list(promise_list_register, promise_register.register());

            Ok(())
        })
    }

    /// Awaits each stored promise in order and optionally accumulates the
    /// resolved values into the provided register.
    fn compile_await_loop(
        &mut self,
        promise_list_register: RegisterId,
        result_register: Option<RegisterId>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.compile_indexed_loop(promise_list_register, move |compiler, promise_register| {
            let promise = Marked::mark(RegisterHandle::Existing(promise_register));
            let awaited_register = compiler.context.local_frame.allocate_temporary_register();

            compiler
                .value_compiler()
                .compile_await(awaited_register.register(), &promise);

            if let Some(result_register) = result_register {
                compiler.append_to_list(result_register, awaited_register.register());
            }

            Ok(())
        })
    }

    /// Compiles a simple indexed loop over a list-valued register.
    fn compile_indexed_loop<F>(
        &mut self,
        collection_register: RegisterId,
        compile_body: F,
    ) -> Result<(), ErrorFor<Spec, Lowering>>
    where
        F: FnOnce(&mut Self, RegisterId) -> Result<(), ErrorFor<Spec, Lowering>>,
    {
        let index_register = self.context.local_frame.allocate_register();
        self.emit_int_literal_into_register(index_register, 0)?;

        let length_register = self.context.local_frame.allocate_register();
        self.context
            .emitter
            .emit_length(length_register, collection_register);

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
        {
            let condition_register = self.context.local_frame.allocate_temporary_register();
            self.context.emitter.emit_binary(
                BinaryOpKind::Lt,
                condition_register.register(),
                index_register,
                length_register,
            );
            self.context
                .emitter
                .emit_jump_if(for_loop.body_state(), condition_register.register());
        }
        self.context
            .emitter
            .emit_jump(for_loop.loop_scope().target(LoopControlKind::Break));

        self.switch_to_with_flow(for_loop.body_state(), for_loop.body_flow());
        {
            let item_register = self.context.local_frame.allocate_temporary_register();
            self.context.emitter.emit_index(
                item_register.register(),
                collection_register,
                index_register,
            );
            compile_body(self, item_register.register())?;
        }

        if self.context.emitter.is_active() {
            self.context.emitter.emit_jump(for_loop.continue_state());
        }

        self.switch_to_with_flow(for_loop.continue_state(), for_loop.continue_flow());
        self.emit_add_assign_immediate(index_register, 1)?;
        self.context.emitter.emit_jump(for_loop.condition_state());

        let (exit_state, exit_flow) = for_loop.finish();
        self.switch_to_with_flow(exit_state, exit_flow);

        Ok(())
    }

    /// Appends one item to a list register using `list + [item]` semantics.
    fn append_to_list(&mut self, list_register: RegisterId, item_register: RegisterId) {
        let singleton_register = self.context.local_frame.allocate_temporary_register();
        self.context
            .emitter
            .emit_make_list(singleton_register.register(), vec![item_register]);
        self.context.emitter.emit_binary(
            BinaryOpKind::Add,
            list_register,
            list_register,
            singleton_register.register(),
        );
    }

    /// Emits an empty list into `target_register`.
    fn emit_empty_list(&mut self, target_register: RegisterId) {
        self.context
            .emitter
            .emit_make_list(target_register, Vec::new());
    }

    /// Binds `source_register` into `name`, copying only when needed.
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

    /// Creates a stable synthetic local name for compiler-internal lowering.
    fn synthetic_name(&self, prefix: &str) -> String {
        format!(
            "__waymark_internal_{prefix}_r{}__",
            self.context.local_frame.num_registers()
        )
    }

    /// Switches the emitter and flow state to a reserved state id.
    fn switch_to_with_flow(&mut self, state_id: StateId, flow_state: super::FlowState) {
        self.context.emitter.switch_to(state_id);
        *self.context.flow_state = flow_state;
    }

    /// Reserves a new bytecode state id.
    fn new_state(&mut self) -> StateId {
        self.context.emitter.reserve_state()
    }

    /// Emits an integer literal into the provided persistent register.
    fn emit_int_literal_into_register(
        &mut self,
        target_register: RegisterId,
        value: i64,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let value_register = self.value_compiler().compile_expr(
            &synthetic_int_expr(value),
            super::value::ResultTarget::Existing(target_register),
        )?;
        if value_register.register() != target_register {
            self.context
                .emitter
                .emit_copy(target_register, value_register.register());
        }
        Ok(())
    }

    /// Emits `target_register = target_register + immediate`.
    fn emit_add_assign_immediate(
        &mut self,
        target_register: RegisterId,
        immediate: i64,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let immediate_register = self.value_compiler().compile_expr(
            &synthetic_int_expr(immediate),
            super::value::ResultTarget::Allocate,
        )?;
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

    /// Creates an assignment compiler borrowing the current context mutably.
    fn assignment_compiler(&mut self) -> AssignmentCompiler<'_, 'table, Spec, Lowering> {
        AssignmentCompiler::new(self.context.reborrow_mut())
    }

    /// Creates a for-loop compiler borrowing the current context mutably.
    fn for_loop_compiler(&mut self) -> ForLoopCompiler<'_, 'table, Spec, Lowering> {
        ForLoopCompiler::new(self.context.reborrow_mut(), LoopControlStack::new())
    }
}

/// Rewrites spread-action kwargs so loop-variable references point at the
/// compiler-internal binding for this spread instance.
fn rewrite_action_call_loop_var(action: &ActionCall, from: &str, to: &str) -> ActionCall {
    ActionCall {
        action_name: action.action_name.clone(),
        kwargs: action
            .kwargs
            .iter()
            .map(|kwarg| Kwarg {
                name: kwarg.name.clone(),
                value: rewrite_expr_loop_var(&kwarg.value, from, to),
            })
            .collect(),
        policies: action.policies.clone(),
        module_name: action.module_name.clone(),
    }
}

/// Rewrites one expression tree by substituting free references to `from`
/// with the compiler-internal `to` binding.
fn rewrite_expr_loop_var(expr: &Spanned<Expr>, from: &str, to: &str) -> Spanned<Expr> {
    let value = match &expr.value {
        Expr::Literal { value } => Expr::Literal {
            value: value.clone(),
        },
        Expr::Variable { name } => Expr::Variable {
            name: if name == from {
                to.to_owned()
            } else {
                name.clone()
            },
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_expr_loop_var(left, from, to)),
            op: op.clone(),
            right: Box::new(rewrite_expr_loop_var(right, from, to)),
        },
        Expr::UnaryOp { op, operand } => Expr::UnaryOp {
            op: op.clone(),
            operand: Box::new(rewrite_expr_loop_var(operand, from, to)),
        },
        Expr::List { elements } => Expr::List {
            elements: elements
                .iter()
                .map(|element| rewrite_expr_loop_var(element, from, to))
                .collect(),
        },
        Expr::Dict { entries } => Expr::Dict {
            entries: entries
                .iter()
                .map(|entry| DictEntry {
                    key: rewrite_expr_loop_var(&entry.key, from, to),
                    value: rewrite_expr_loop_var(&entry.value, from, to),
                })
                .collect(),
        },
        Expr::Index { object, index } => Expr::Index {
            object: Box::new(rewrite_expr_loop_var(object, from, to)),
            index: Box::new(rewrite_expr_loop_var(index, from, to)),
        },
        Expr::Dot { object, attribute } => Expr::Dot {
            object: Box::new(rewrite_expr_loop_var(object, from, to)),
            attribute: attribute.clone(),
        },
        Expr::FunctionCall { call } => Expr::FunctionCall {
            call: FunctionCall {
                name: call.name.clone(),
                args: call
                    .args
                    .iter()
                    .map(|arg| rewrite_expr_loop_var(arg, from, to))
                    .collect(),
                kwargs: call
                    .kwargs
                    .iter()
                    .map(|kwarg| Kwarg {
                        name: kwarg.name.clone(),
                        value: rewrite_expr_loop_var(&kwarg.value, from, to),
                    })
                    .collect(),
                global_function: call.global_function.clone(),
            },
        },
        Expr::ActionCall { call } => Expr::ActionCall {
            call: rewrite_action_call_loop_var(call, from, to),
        },
        Expr::ParallelExpr { calls } => Expr::ParallelExpr {
            calls: calls
                .iter()
                .map(|call| match call {
                    waymark_vm_ast_old::Call::Action(call) => waymark_vm_ast_old::Call::Action(
                        rewrite_action_call_loop_var(call, from, to),
                    ),
                    waymark_vm_ast_old::Call::Function(call) => {
                        waymark_vm_ast_old::Call::Function(FunctionCall {
                            name: call.name.clone(),
                            args: call
                                .args
                                .iter()
                                .map(|arg| rewrite_expr_loop_var(arg, from, to))
                                .collect(),
                            kwargs: call
                                .kwargs
                                .iter()
                                .map(|kwarg| Kwarg {
                                    name: kwarg.name.clone(),
                                    value: rewrite_expr_loop_var(&kwarg.value, from, to),
                                })
                                .collect(),
                            global_function: call.global_function.clone(),
                        })
                    }
                })
                .collect(),
        },
        Expr::SpreadExpr {
            collection,
            loop_var,
            action,
        } => Expr::SpreadExpr {
            collection: Box::new(rewrite_expr_loop_var(collection, from, to)),
            loop_var: loop_var.clone(),
            action: if loop_var == from {
                action.clone()
            } else {
                rewrite_action_call_loop_var(action, from, to)
            },
        },
    };

    Spanned {
        value,
        span: expr.span,
    }
}

/// Builds a synthetic block for internal compiler lowering.
fn synthetic_block(statements: Vec<Spanned<waymark_vm_ast_old::Statement>>) -> Spanned<Block> {
    Spanned {
        value: Block { statements },
        span: synthetic_span(),
    }
}

/// Builds a synthetic assignment statement for internal compiler lowering.
fn synthetic_assignment_statement(
    target: String,
    value: Spanned<Expr>,
) -> Spanned<waymark_vm_ast_old::Statement> {
    Spanned {
        value: waymark_vm_ast_old::Statement::Assignment {
            targets: vec![target],
            value,
        },
        span: synthetic_span(),
    }
}

/// Builds a synthetic `left + right` expression for internal lowering.
fn synthetic_binary_add_expr(left: Spanned<Expr>, right: Spanned<Expr>) -> Spanned<Expr> {
    Spanned {
        value: Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Add,
            right: Box::new(right),
        },
        span: synthetic_span(),
    }
}

/// Builds a synthetic list literal expression for internal lowering.
fn synthetic_list_expr(elements: Vec<Spanned<Expr>>) -> Spanned<Expr> {
    Spanned {
        value: Expr::List { elements },
        span: synthetic_span(),
    }
}

/// Builds a synthetic variable expression for internal lowering.
fn synthetic_variable_expr(name: &str) -> Spanned<Expr> {
    Spanned {
        value: Expr::Variable {
            name: name.to_owned(),
        },
        span: synthetic_span(),
    }
}

/// Builds a synthetic integer literal expression for internal lowering.
fn synthetic_int_expr(value: i64) -> Spanned<Expr> {
    Spanned {
        value: Expr::Literal {
            value: waymark_vm_ast_old::Literal::Int(value),
        },
        span: synthetic_span(),
    }
}

/// Returns the zeroed span used by synthetic AST nodes.
const fn synthetic_span() -> Span {
    Span {
        start_line: 0,
        start_col: 0,
        end_line: 0,
        end_col: 0,
    }
}
