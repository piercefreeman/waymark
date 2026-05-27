//! Loop-variable binding helpers for `for` loops.

use waymark_vm_runtime_core::RegisterId;

use super::header::LoopBinding;
use super::{ErrorFor, ForLoopCompiler};

impl<'borrow, 'table, Spec, Lowering> ForLoopCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Increments the enumerate-index register by one if enumeration is in use.
    pub(super) fn emit_enumerate_increment(
        &mut self,
        enumerate_index_register: Option<RegisterId>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        if let Some(register) = enumerate_index_register {
            self.emit_add_assign_immediate(register, 1)?;
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
    pub(super) fn compile_loop_bindings(
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
    pub(super) fn allocate_enumerate_index_register(
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
}
