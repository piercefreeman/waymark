//! Parallel lowering.

use nonempty_collections::NEVec;

use crate::Marked;

use super::CompilerContextMut;
use super::ErrorFor;
use super::ValueCompiler;
use super::env::RegisterHandle;
use super::plan::call::CallPlanFor;
use super::plan::parallel::*;
use super::suspend::PromiseMarker;

/// Lowers parallel blocks and assignments into bytecode.
pub struct ParallelCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Mutable compiler context for parallel lowering.
    context: CompilerContextMut<'borrow, 'table, Spec, Lowering>,
}

impl<'borrow, 'table, Spec, Lowering> ParallelCompiler<'borrow, 'table, Spec, Lowering>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    /// Creates a parallel compiler over the provided context.
    pub fn new(context: CompilerContextMut<'borrow, 'table, Spec, Lowering>) -> Self {
        Self { context }
    }

    /// Compiles a parallel block used only for side effects.
    pub fn compile_block(
        &mut self,
        calls: ParallelCallPlans<'_, Spec>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let promise_registers = calls.try_map(|call| self.compile_call_start(call))?;
        self.compile_execution(ParallelExecutionPlan::Block { promise_registers });
        Ok(())
    }

    /// Compiles a parallel assignment and materializes its results.
    pub fn compile_assignment(
        &mut self,
        assignment: ParallelAssignmentPlan<'_, Spec>,
    ) -> Result<(), ErrorFor<Spec, Lowering>> {
        let assignment = assignment
            .into_items()
            .try_map(|call| self.compile_call_start(call))?;
        let execution_plan = ParallelExecutionPlan::Assignment { assignment };

        self.compile_execution(execution_plan);
        Ok(())
    }

    /// Executes the post-start step for a parallel plan.
    fn compile_execution(&mut self, plan: ParallelExecutionPlan) {
        match &plan {
            ParallelExecutionPlan::Block { promise_registers } => {
                self.await_promise_registers(promise_registers.as_slice());
            }
            ParallelExecutionPlan::Assignment { assignment } => {
                self.compile_assignment_execution(assignment);
            }
        }

        plan.mark_initialized(&mut *self.context.flow_state);
    }

    /// Lowers the materialization step for a parallel assignment.
    fn compile_assignment_execution(
        &mut self,
        assignment: &ParallelAssignmentItems<Marked<RegisterHandle, PromiseMarker>>,
    ) {
        match assignment {
            ParallelAssignmentItems::Aggregate(targeted_items) => {
                let ParallelTargeted {
                    target,
                    payload: registers,
                } = targeted_items;
                self.await_promise_registers(registers.as_slice());
                self.context.emitter.emit_make_list(
                    target.register(),
                    registers
                        .iter()
                        .map(|register| register.register())
                        .collect(),
                );
            }
            ParallelAssignmentItems::Positional(targeted_items) => {
                self.await_targets(targeted_items);
            }
        }
    }

    /// Starts one call and returns the promise register it produces.
    fn compile_call_start(
        &mut self,
        call: CallPlanFor<'_, Spec>,
    ) -> Result<Marked<RegisterHandle, PromiseMarker>, ErrorFor<Spec, Lowering>> {
        self.value_compiler()
            .compile_call_start(call, super::value::ResultTarget::Allocate)
    }

    /// Creates a value compiler borrowing the current context.
    fn value_compiler(&mut self) -> ValueCompiler<'_, 'table, Spec, Lowering> {
        ValueCompiler::new(self.context.reborrow_ref())
    }

    /// Awaits each promise register in order.
    fn await_promise_registers(
        &mut self,
        promise_registers: &[Marked<RegisterHandle, PromiseMarker>],
    ) {
        for promise_register in promise_registers {
            self.value_compiler()
                .compile_await(promise_register.register(), promise_register);
        }
    }

    /// Awaits each promise directly into its final assignment target.
    fn await_targets(
        &mut self,
        awaited_targets: &NEVec<ParallelTargeted<Marked<RegisterHandle, PromiseMarker>>>,
    ) {
        for awaited_target in awaited_targets {
            self.value_compiler().compile_await(
                awaited_target.target().register(),
                awaited_target.promise_register(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use index_type::IndexType;
    use waymark_vm_ast_old::Call;
    use waymark_vm_ast_old_helpers::{function_call, int};
    use waymark_vm_bytecode_core::{FunctionId, StateId};
    use waymark_vm_compiler_for_ast_old_test_support::{TestConstValue, TestLowering, TestSpec};
    use waymark_vm_instructions_coreset::CoreSet;
    use waymark_vm_instructions_fullset::FullSet as InstructionSet;
    use waymark_vm_instructions_pureset::PureSet;
    use waymark_vm_runtime_core::RegisterId;

    use crate::Marked;
    use crate::function::extras::ExtraFunctions;

    use crate::function::compiler::{
        CompilerContextMut,
        bytecode::emitter::FunctionEmitter,
        env::{AssignmentTargetMarker, FlowState, LocalFrame, LocalSlot},
        test_helpers::build_function_table,
    };

    #[test]
    fn empty_parallel_assignment_compiles_into_empty_make_list_and_initializes_target() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(1);
        let assignment = ParallelAssignmentPlan::<TestSpec>::build::<TestLowering, _>(
            nonempty_collections::NESlice::try_from_slice(&["results".to_owned()]).unwrap(),
            &[],
            &function_table,
            |target| {
                Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
                    &mut local_frame,
                    &mut flow_state,
                    target,
                )
            },
        )
        .expect("empty single target should build aggregate plan");

        {
            let mut parallel =
                ParallelCompiler::<TestSpec, TestLowering>::new(CompilerContextMut::new(
                    &function_table,
                    &mut emitter,
                    &mut local_frame,
                    &mut extra_fns,
                    &mut flow_state,
                ));

            parallel
                .compile_assignment(assignment)
                .expect("empty parallel assignment should compile");
        }

        let results_local = local_frame
            .resolve_initialized_local("results", &flow_state)
            .expect("aggregate target should be initialized after compilation");
        assert_eq!(results_local.register(), RegisterId(0));

        let states = emitter.finish();
        assert_eq!(states.len().to_scalar(), 1);

        let mut instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            instructions.next(),
            Some(InstructionSet::PureSet(PureSet::MakeList { dst, items }))
                if *dst == RegisterId(0) && items.is_empty()
        ));
        assert!(instructions.next().is_none());
    }

    #[test]
    fn parallel_assignment_compiles_into_make_list_and_initializes_target() {
        let function_table = build_function_table();
        let mut emitter = FunctionEmitter::<TestSpec>::new();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let mut extra_fns = ExtraFunctions::<TestSpec>::new(1);
        let calls = [
            Call::Function(function_call("child", vec![int(1)])),
            Call::Function(function_call("child", vec![int(2)])),
        ];
        let assignment = ParallelAssignmentPlan::<TestSpec>::build::<TestLowering, _>(
            nonempty_collections::NESlice::try_from_slice(&["results".to_owned()]).unwrap(),
            &calls,
            &function_table,
            |target| {
                Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
                    &mut local_frame,
                    &mut flow_state,
                    target,
                )
            },
        )
        .expect("single target should build aggregate plan");

        {
            let mut parallel =
                ParallelCompiler::<TestSpec, TestLowering>::new(CompilerContextMut::new(
                    &function_table,
                    &mut emitter,
                    &mut local_frame,
                    &mut extra_fns,
                    &mut flow_state,
                ));

            parallel
                .compile_assignment(assignment)
                .expect("parallel assignment should compile");
        }

        let results_local = local_frame
            .resolve_initialized_local("results", &flow_state)
            .expect("aggregate target should be initialized after compilation");
        assert_eq!(results_local.register(), RegisterId(0));

        let states = emitter.finish();
        assert_eq!(states.len().to_scalar(), 3);

        let mut start_instructions = states[StateId(0)].instructions.iter();
        assert!(matches!(
            start_instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(1),
            })) if *dst == RegisterId(2)
        ));
        assert!(matches!(
            start_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Call {
                dst,
                function_id,
                args,
            })) if *dst == RegisterId(1)
                && *function_id == FunctionId(0)
                && args == &[RegisterId(2)]
        ));
        assert!(matches!(
            start_instructions.next(),
            Some(InstructionSet::PureSet(PureSet::LoadConst {
                dst,
                value: TestConstValue::Int(2),
            })) if *dst == RegisterId(3)
        ));
        assert!(matches!(
            start_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Call {
                dst,
                function_id,
                args,
            })) if *dst == RegisterId(2)
                && *function_id == FunctionId(0)
                && args == &[RegisterId(3)]
        ));
        assert!(matches!(
            start_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Await { dst, src, resume }))
                if *dst == RegisterId(1)
                    && *src == RegisterId(1)
                    && *resume == StateId(1)
        ));
        assert!(start_instructions.next().is_none());

        let mut middle_instructions = states[StateId(1)].instructions.iter();
        assert!(matches!(
            middle_instructions.next(),
            Some(InstructionSet::CoreSet(CoreSet::Await { dst, src, resume }))
                if *dst == RegisterId(2)
                    && *src == RegisterId(2)
                    && *resume == StateId(2)
        ));
        assert!(middle_instructions.next().is_none());

        let mut final_instructions = states[StateId(2)].instructions.iter();
        assert!(matches!(
            final_instructions.next(),
            Some(InstructionSet::PureSet(PureSet::MakeList { dst, items }))
                if *dst == RegisterId(0) && items == &[RegisterId(1), RegisterId(2)]
        ));
        assert!(final_instructions.next().is_none());
    }
}
