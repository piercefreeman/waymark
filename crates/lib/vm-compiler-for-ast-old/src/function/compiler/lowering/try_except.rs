//! Try/except lowering.

use nonempty_collections::NEVec;
use waymark_vm_ast_old::{Block, ExceptHandler, Literal, Spanned};
use waymark_vm_bytecode_core::StateId;
use waymark_vm_runtime_core::RegisterId;

use super::conditional::{ConditionalJoin, ConditionalJoinFinish};
use super::env::FlowState;
use super::exception::{ExceptionHandlerDispatch, ExceptionScope};
use super::r#loop::LoopControlStack;
use super::{CompilerContextMut, Error, ErrorFor};

/// Lowers `try`/`except` statements and handler dispatch.
pub struct TryExceptCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Mutable compiler context for try/except lowering.
    context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,

    /// Active loop scopes available to nested handler bodies.
    loop_control: LoopControlStack,
}

impl<'borrow, 'table, Spec, Lowering> CompilerContextMut<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Reborrows the context for try/except lowering.
    pub fn try_except_compiler(
        &mut self,
        loop_control: LoopControlStack,
    ) -> TryExceptCompiler<'_, 'table, Spec, Lowering> {
        self.reborrow_mut().into_try_except_compiler(loop_control)
    }

    /// Converts this context into a try/except compiler.
    pub fn into_try_except_compiler(
        self,
        loop_control: LoopControlStack,
    ) -> TryExceptCompiler<'borrow, 'table, Spec, Lowering> {
        TryExceptCompiler::new(self, loop_control)
    }
}

impl<'borrow, 'table, Spec, Lowering> TryExceptCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Creates a try/except compiler over the provided context and loop scope.
    pub fn new(
        context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,
        loop_control: LoopControlStack,
    ) -> Self {
        Self {
            context,
            loop_control,
        }
    }

    /// Compiles a `try`/`except` block by routing awaited exception values into
    /// handler entry states.
    pub fn compile(
        &mut self,
        try_block: &Spanned<Block>,
        handlers: &[Spanned<ExceptHandler>],
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        if handlers.is_empty() {
            let mut try_compiler = self.context.statement_compiler(self.loop_control.clone());
            return try_compiler.compile_block(try_block);
        }

        let incoming_flow = self.context.flow_state.clone();
        let join_state = self.new_state();
        let prepared = self.prepare_exception_handlers(handlers)?;

        let scoped_exceptions = self.context.exception_scope.push(ExceptionScope::new(
            self.context.exception_scope.clone(),
            prepared.exception_register,
            prepared.dispatch,
        ));
        let current_scope = scoped_exceptions
            .current_scope()
            .expect("newly-pushed exception scope should exist");
        let mut join = ConditionalJoin::new(&incoming_flow, join_state);
        let mut handler_continuations: Vec<FlowState> = Vec::new();

        {
            let mut try_compiler = self.context.statement_compiler_with_exception_scope(
                self.loop_control.clone(),
                scoped_exceptions.clone(),
            );
            try_compiler.compile_block(try_block)?;
        }
        if self.context.emitter.is_active() {
            join.record_continuation(self.context.flow_state.clone());
            self.context.emitter.emit_jump(join_state);
        }

        for ((handler, entry_state), entry_flow) in handlers
            .iter()
            .zip(prepared.entry_states)
            .zip(current_scope.handler_entry_flows())
        {
            let Some(entry_flow) = entry_flow else {
                continue;
            };

            self.compile_handler_body(
                handler,
                entry_state,
                entry_flow,
                current_scope.exception_register(),
            )?;

            if self.context.emitter.is_active() {
                let continuation_flow = self.context.flow_state.clone();
                handler_continuations.push(continuation_flow.clone());
                join.record_continuation(continuation_flow);
                self.context.emitter.emit_jump(join_state);
            }
        }

        if let ConditionalJoinFinish::Join {
            join_state,
            merged_flow,
        } = join.finish()
        {
            let merged_flow = merge_flow_with_handlers(merged_flow, handler_continuations);
            self.switch_to_with_flow(join_state, merged_flow);
        }

        Ok(())
    }

    /// Allocates the per-scope exception register, reserves an entry state for
    /// every handler, and lowers each handler's candidate type-id registers.
    fn prepare_exception_handlers(
        &mut self,
        handlers: &[Spanned<ExceptHandler>],
    ) -> Result<PreparedExceptionHandlers, ErrorFor<Spec, Lowering>> {
        let exception_register = self.context.local_frame.allocate_register();
        let mut entry_states = Vec::with_capacity(handlers.len());
        let mut dispatch = Vec::with_capacity(handlers.len());

        for handler in handlers {
            let entry_state = self.new_state();
            let catch_all = handler_is_catch_all(&handler.value.exception_types);
            let mut exception_type_registers = Vec::new();

            if !catch_all {
                for exception_type in &handler.value.exception_types {
                    exception_type_registers
                        .push(self.compile_exception_type_register(exception_type)?);
                }
            }

            entry_states.push(entry_state);
            dispatch.push(ExceptionHandlerDispatch::new(
                entry_state,
                exception_type_registers,
                catch_all,
            ));
        }

        Ok(PreparedExceptionHandlers {
            exception_register,
            entry_states,
            dispatch,
        })
    }

    /// Switches to `entry_state` with `entry_flow`, binds the optional
    /// exception variable, and lowers the handler body.
    fn compile_handler_body(
        &mut self,
        handler: &Spanned<ExceptHandler>,
        entry_state: StateId,
        entry_flow: FlowState,
        exception_register: RegisterId,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        self.switch_to_with_flow(entry_state, entry_flow);

        if let Some(exception_var) = handler.value.exception_var.as_deref() {
            let exception_local = self
                .context
                .local_frame
                .get_or_declare_local(exception_var, self.context.flow_state);
            self.context
                .emitter
                .emit_exception_details(exception_local.register(), exception_register);
            self.context.flow_state.mark_initialized(exception_local);
        }

        let mut handler_compiler = self.context.statement_compiler(self.loop_control.clone());
        handler_compiler.compile_block(&handler.value.body)
    }

    /// Materializes one exception type-id literal into a stable register.
    fn compile_exception_type_register(
        &mut self,
        exception_type: &str,
    ) -> Result<RegisterId, ErrorFor<Spec, Lowering>> {
        let register = self.context.local_frame.allocate_register();
        let value = Lowering::lower_literal(&Literal::String(exception_type.to_owned()))
            .map_err(Error::LiteralLowering)?;
        self.context.emitter.emit_load_const(register, value);
        Ok(register)
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
}

/// Returns whether this handler should match any exception type.
///
/// The canonical catch-all representation in the old AST is an empty
/// `exception_types` list. We also treat a literal `"Exception"` entry as a
/// catch-all here as a compiler-local compatibility shim for the current
/// Python frontend output and existing snapshots. This is not meant to define
/// a broader runtime exception-matching rule.
fn handler_is_catch_all(exception_types: &[String]) -> bool {
    exception_types.is_empty() || exception_types.iter().any(|value| value == "Exception")
}

/// Lowered dispatch state for a `try`/`except` statement, produced before
/// compiling the try and handler bodies.
struct PreparedExceptionHandlers {
    /// Register that stores the active exception while dispatching handlers.
    exception_register: RegisterId,

    /// Entry state reserved for each handler in source order.
    entry_states: Vec<StateId>,

    /// Per-handler dispatch metadata in source order.
    dispatch: Vec<ExceptionHandlerDispatch>,
}

/// Combines the intersected join flow with handler continuations so locals
/// that were only assigned along a handler path remain visible after the join.
fn merge_flow_with_handlers(
    merged_flow: FlowState,
    handler_continuations: Vec<FlowState>,
) -> FlowState {
    if handler_continuations.is_empty() {
        return merged_flow;
    }
    let mut branches = NEVec::new(merged_flow);
    for flow in handler_continuations {
        branches.push(flow);
    }
    FlowState::union_branches(branches)
}
