//! Shared compiler state bundles passed between lowering helpers.

use crate::function::{
    compiler::{FlowState, FunctionEmitter, LocalFrame},
    extras::ExtraFunctions,
    table::FunctionTable,
};

/// Shared compiler state passed between lowering helpers.
pub struct CompilerContext<'borrow, 'table, Spec, Lowering, FlowStateRef>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// Keeps the lowering type in the context without storing a value.
    pub phantom_data: core::marker::PhantomData<Lowering>,

    /// Program-wide function metadata for resolving calls.
    pub function_table: &'table FunctionTable,

    /// Bytecode emitter for the function currently being lowered.
    pub emitter: &'borrow mut FunctionEmitter<Spec>,

    /// Local-variable and register-allocation state.
    pub local_frame: &'borrow mut LocalFrame,

    /// Program-wide extra functions introduced during lowering.
    pub extra_fns: &'borrow mut ExtraFunctions<Spec>,

    /// Current definite-initialization state.
    pub flow_state: FlowStateRef,
}

/// Mutable view capturing compiler context with mutable access to flow state.
pub type CompilerContextMut<'borrow, 'table, Spec, Lowering> =
    CompilerContext<'borrow, 'table, Spec, Lowering, &'borrow mut FlowState>;

/// Mutable view capturing compiler context with read-only access to flow state.
pub type CompilerContextRef<'borrow, 'table, Spec, Lowering> =
    CompilerContext<'borrow, 'table, Spec, Lowering, &'borrow FlowState>;

impl<'borrow, 'table, Spec, Lowering>
    CompilerContext<'borrow, 'table, Spec, Lowering, &'borrow mut FlowState>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// Builds a mutable compiler context from its component parts.
    pub fn new(
        function_table: &'table FunctionTable,
        emitter: &'borrow mut FunctionEmitter<Spec>,
        local_frame: &'borrow mut LocalFrame,
        extra_fns: &'borrow mut ExtraFunctions<Spec>,
        flow_state: &'borrow mut FlowState,
    ) -> Self {
        Self {
            phantom_data: core::marker::PhantomData,
            function_table,
            emitter,
            local_frame,
            extra_fns,
            flow_state,
        }
    }

    /// Reborrows the context mutably for a nested lowering helper.
    pub fn reborrow_mut(&mut self) -> CompilerContextMut<'_, 'table, Spec, Lowering> {
        CompilerContext {
            phantom_data: core::marker::PhantomData,
            function_table: self.function_table,
            emitter: &mut *self.emitter,
            local_frame: &mut *self.local_frame,
            extra_fns: &mut *self.extra_fns,
            flow_state: &mut *self.flow_state,
        }
    }

    /// Reborrows the context while downgrading flow-state access to shared.
    pub fn reborrow_ref(&mut self) -> CompilerContextRef<'_, 'table, Spec, Lowering> {
        CompilerContext {
            phantom_data: core::marker::PhantomData,
            function_table: self.function_table,
            emitter: &mut *self.emitter,
            local_frame: &mut *self.local_frame,
            extra_fns: &mut *self.extra_fns,
            flow_state: &*self.flow_state,
        }
    }

    /// Consumes and convert the context view while downgrading flow-state
    /// access to shared.
    pub fn into_ref(self) -> CompilerContextRef<'borrow, 'table, Spec, Lowering> {
        CompilerContext {
            phantom_data: core::marker::PhantomData,
            function_table: self.function_table,
            emitter: self.emitter,
            local_frame: self.local_frame,
            extra_fns: self.extra_fns,
            flow_state: &*self.flow_state,
        }
    }
}
