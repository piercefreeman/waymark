//! Parallel-call planning.

use nonempty_collections::{IntoNonEmptyIterator, NESlice, NEVec, NonEmptyIterator};
use waymark_vm_ast_old::Call;

use crate::function::compiler::env::{AssignmentTargetMarker, LocalSlot, RegisterHandle};
use crate::function::table::FunctionTable;
use crate::{EEVec, Marked};

use super::ErrorFor;
use super::Unsupported;
use super::env::FlowState;
use super::plan::call::{CallPlan, CallPlanFor};
use super::suspend::PromiseMarker;

/// Zero-or-more call plans produced for a parallel block.
pub type ParallelCallPlans<'a, Spec> = EEVec<CallPlanFor<'a, Spec>>;

/// A call plan paired with the local that should receive its result.
pub type TargetedParallelCallPlan<'a, Spec> = ParallelTargeted<CallPlanFor<'a, Spec>>;

/// A validated parallel-assignment shape accepted by the compiler.
#[derive(Debug)]
pub struct ParallelAssignmentPlan<'a, Spec>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// Assignment shape and call plans for the parallel expression.
    assignment: ParallelAssignmentItems<CallPlanFor<'a, Spec>>,
}

/// The supported ways of assigning results from a parallel expression.
#[derive(Debug)]
pub enum ParallelAssignmentItems<T> {
    /// Collect all parallel results into one aggregate target.
    Aggregate(ParallelTargeted<EEVec<T>>),

    /// Assign each parallel result to a matching positional target.
    Positional(NEVec<ParallelTargeted<T>>),
}

/// A payload paired with the local slot that should receive it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParallelTargeted<T> {
    /// Assignment target for the payload.
    pub target: Marked<LocalSlot, AssignmentTargetMarker>,

    /// Value paired with the target.
    pub payload: T,
}

/// Reasons a parallel-expression assignment shape is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedParallelExprAssignment {
    /// The compiler only supports one aggregate target or one target per call.
    TargetCountMustMatchCalls,
}

impl<T> ParallelTargeted<T> {
    /// Creates a targeted payload.
    fn new(target: Marked<LocalSlot, AssignmentTargetMarker>, payload: T) -> Self {
        Self { target, payload }
    }

    /// Returns the assignment target.
    pub fn target(&self) -> Marked<LocalSlot, AssignmentTargetMarker> {
        self.target
    }

    /// Maps the payload while preserving its assignment target.
    fn try_map<U, E, F>(self, mut map_payload: F) -> Result<ParallelTargeted<U>, E>
    where
        F: FnMut(T) -> Result<U, E>,
    {
        Ok(ParallelTargeted::new(
            self.target,
            map_payload(self.payload)?,
        ))
    }
}

impl<T> ParallelAssignmentItems<T> {
    /// Maps each payload while preserving aggregate vs positional structure.
    pub fn try_map<U, E, F>(self, mut map_item: F) -> Result<ParallelAssignmentItems<U>, E>
    where
        F: FnMut(T) -> Result<U, E>,
    {
        match self {
            Self::Aggregate(targeted_items) => {
                let targeted_items =
                    targeted_items.try_map(|items| items.try_map(&mut map_item))?;
                Ok(ParallelAssignmentItems::Aggregate(targeted_items))
            }

            Self::Positional(targeted_items) => {
                let targeted_items = targeted_items
                    .into_nonempty_iter()
                    .map(|item| item.try_map(&mut map_item))
                    .collect::<Result<_, _>>()?;
                Ok(ParallelAssignmentItems::Positional(targeted_items))
            }
        }
    }

    /// Marks every target in the assignment shape as initialized.
    fn mark_initialized(&self, flow_state: &mut FlowState) {
        match self {
            Self::Aggregate(targeted_items) => targeted_items.target().mark_initialized(flow_state),
            Self::Positional(targeted_items) => {
                for targeted_item in targeted_items.iter() {
                    targeted_item.target().mark_initialized(flow_state);
                }
            }
        }
    }
}

impl ParallelTargeted<Marked<RegisterHandle, PromiseMarker>> {
    /// Returns the promise register produced for this targeted parallel call.
    pub fn promise_register(&self) -> &Marked<RegisterHandle, PromiseMarker> {
        &self.payload
    }
}

/// The lowered execution strategy for a parallel block or assignment.
#[derive(Debug)]
pub enum ParallelExecutionPlan {
    /// Execute a parallel block for side effects and await the produced promises.
    Block {
        /// Promise registers returned by starting each call.
        promise_registers: EEVec<Marked<RegisterHandle, PromiseMarker>>,
    },

    /// Execute a parallel assignment and route each promise to its target.
    Assignment {
        /// Targeted promise registers to await and materialize.
        assignment: ParallelAssignmentItems<Marked<RegisterHandle, PromiseMarker>>,
    },
}

/// Builds call plans for a `parallel` block, preserving source order.
pub fn build_parallel_call_plans<'a, Spec, Lowering>(
    calls: &'a [Call],
    function_table: &FunctionTable,
) -> Result<ParallelCallPlans<'a, Spec>, ErrorFor<Spec, Lowering>>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
{
    let Some(calls) = NESlice::try_from_slice(calls) else {
        return Ok(EEVec::Empty);
    };

    let call_plans = CallPlan::build_all::<Spec, Lowering, _>(calls, function_table)?;

    Ok(EEVec::NonEmpty(call_plans))
}

/// Builds positional targeted call plans for a non-empty target and call list.
fn build_targeted_parallel_call_plans<'a, Spec, Lowering, F>(
    targets: NESlice<'_, String>,
    calls: NESlice<'a, Call>,
    function_table: &FunctionTable,
    resolve_target: F,
) -> Result<NEVec<TargetedParallelCallPlan<'a, Spec>>, ErrorFor<Spec, Lowering>>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
    Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
    F: FnMut(&str) -> Marked<LocalSlot, AssignmentTargetMarker>,
{
    Ok(build_targeted_parallel_items(
        targets,
        CallPlan::build_all::<Spec, Lowering, _>(calls, function_table)?,
        resolve_target,
    ))
}

/// Zips positional targets with payloads into targeted items.
fn build_targeted_parallel_items<'t, T, F>(
    targets: impl IntoNonEmptyIterator<IntoNEIter: NonEmptyIterator<Item = &'t String>>,
    items: impl IntoNonEmptyIterator<IntoNEIter: NonEmptyIterator<Item = T>>,
    mut resolve_target: F,
) -> NEVec<ParallelTargeted<T>>
where
    F: FnMut(&str) -> Marked<LocalSlot, AssignmentTargetMarker>,
{
    let targets = targets.into_nonempty_iter();
    let items = items.into_nonempty_iter();

    targets
        .zip(items)
        .map(|(target, item)| ParallelTargeted::new(resolve_target(target), item))
        .collect()
}

impl<'a, Spec> ParallelAssignmentPlan<'a, Spec>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// Returns the stored assignment items.
    pub fn into_items(self) -> ParallelAssignmentItems<CallPlanFor<'a, Spec>> {
        self.assignment
    }

    /// Builds a validated plan for assigning a parallel expression.
    pub fn build<Lowering, F>(
        targets: NESlice<'_, String>,
        calls: &'a [Call],
        function_table: &FunctionTable,
        mut resolve_target: F,
    ) -> Result<Self, ErrorFor<Spec, Lowering>>
    where
        Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
        F: FnMut(&str) -> Marked<LocalSlot, AssignmentTargetMarker>,
    {
        if targets.len().get() == 1 {
            let calls = build_parallel_call_plans::<Spec, Lowering>(calls, function_table)?;
            let (target, _) = targets.into_nonempty_iter().next();

            return Ok(Self {
                assignment: ParallelAssignmentItems::Aggregate(ParallelTargeted {
                    target: resolve_target(target),
                    payload: calls,
                }),
            });
        }

        if targets.len().get() != calls.len() {
            return Err(Unsupported::ParallelExprAssignment {
                target_count: targets.len(),
                call_count: calls.len(),
                reason: UnsupportedParallelExprAssignment::TargetCountMustMatchCalls,
            }
            .into());
        }

        let Some(calls) = NESlice::try_from_slice(calls) else {
            return Err(Unsupported::ParallelExprAssignment {
                target_count: targets.len(),
                call_count: calls.len(),
                reason: UnsupportedParallelExprAssignment::TargetCountMustMatchCalls,
            }
            .into());
        };

        let targeted_calls = build_targeted_parallel_call_plans::<Spec, Lowering, _>(
            targets,
            calls,
            function_table,
            resolve_target,
        )?;

        Ok(Self {
            assignment: ParallelAssignmentItems::Positional(targeted_calls),
        })
    }
}

impl ParallelExecutionPlan {
    /// Marks any targets written by this execution plan as initialized.
    pub fn mark_initialized(&self, flow_state: &mut FlowState) {
        match self {
            Self::Block { .. } => {}
            Self::Assignment { assignment } => assignment.mark_initialized(flow_state),
        }
    }
}

#[cfg(test)]
impl ParallelExecutionPlan {
    /// Builds a parallel-block execution plan for tests.
    pub fn block(promise_registers: EEVec<Marked<RegisterHandle, PromiseMarker>>) -> Self {
        Self::Block { promise_registers }
    }

    /// Builds an assignment execution plan for tests.
    fn assignment(
        assignment: ParallelAssignmentItems<Marked<RegisterHandle, PromiseMarker>>,
    ) -> Self {
        Self::Assignment { assignment }
    }

    /// Builds an aggregate assignment execution plan for tests.
    pub fn aggregate(
        target: Marked<LocalSlot, AssignmentTargetMarker>,
        promise_registers: EEVec<Marked<RegisterHandle, PromiseMarker>>,
    ) -> Self {
        Self::assignment(ParallelAssignmentItems::Aggregate(ParallelTargeted {
            target,
            payload: promise_registers,
        }))
    }

    /// Builds a positional assignment execution plan for tests.
    pub fn positional(
        awaited_targets: NEVec<ParallelTargeted<Marked<RegisterHandle, PromiseMarker>>>,
    ) -> Self {
        Self::assignment(ParallelAssignmentItems::Positional(awaited_targets))
    }
}

impl core::fmt::Display for UnsupportedParallelExprAssignment {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::TargetCountMustMatchCalls => {
                "parallel expressions currently require one assignment target per call"
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nonempty_collections::NEVec;
    use waymark_vm_ast_old::Call;
    use waymark_vm_ast_old_helpers::{function_call, int};
    use waymark_vm_runtime_core::RegisterId;

    use crate::function::compiler::{
        Error, Unsupported,
        env::{AssignmentTargetMarker, FlowState, LocalFrame, LocalSlot},
        test_helpers::{TestLowering, TestSpec, build_function_table},
    };

    #[test]
    fn single_target_builds_aggregate_assignment() {
        let function_table = build_function_table();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let calls = [
            Call::Function(function_call("child", vec![int(1)])),
            Call::Function(function_call("child", vec![int(2)])),
        ];
        let target_register = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "results",
        )
        .register();

        let assignment = ParallelAssignmentPlan::<TestSpec>::build::<TestLowering, _>(
            NESlice::try_from_slice(&["results".to_owned()]).unwrap(),
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

        match assignment.into_items() {
            ParallelAssignmentItems::Aggregate(ParallelTargeted {
                target,
                payload: EEVec::NonEmpty(calls),
            }) => {
                assert_eq!(target.register(), target_register);
                let (_first_call, calls) = calls.into_nonempty_iter().next();
                let call_count = 1 + calls.count();

                assert_eq!(call_count, 2);
            }
            ParallelAssignmentItems::Aggregate(ParallelTargeted {
                payload: EEVec::Empty,
                ..
            }) => panic!("single target should keep non-empty aggregate calls"),
            ParallelAssignmentItems::Positional { .. } => {
                panic!("single target should produce aggregate plan")
            }
        }
    }

    #[test]
    fn empty_single_target_builds_empty_aggregate_assignment() {
        let function_table = build_function_table();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let target_register = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "results",
        )
        .register();

        let assignment = ParallelAssignmentPlan::<TestSpec>::build::<TestLowering, _>(
            NESlice::try_from_slice(&["results".to_owned()]).unwrap(),
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
        .expect("empty single-target parallel expressions should build aggregate plans");

        match assignment.into_items() {
            ParallelAssignmentItems::Aggregate(ParallelTargeted {
                target,
                payload: EEVec::Empty,
            }) => {
                assert_eq!(target.register(), target_register);
            }
            ParallelAssignmentItems::Aggregate(ParallelTargeted {
                payload: EEVec::NonEmpty(_),
                ..
            }) => {
                panic!("empty single-target parallel expressions should keep empty aggregate calls")
            }
            ParallelAssignmentItems::Positional { .. } => {
                panic!("empty single-target parallel expressions should build aggregate plans")
            }
        }
    }

    #[test]
    fn empty_block_builds_empty_call_plans() {
        let function_table = build_function_table();

        let calls = build_parallel_call_plans::<TestSpec, TestLowering>(&[], &function_table)
            .expect("empty blocks should build");

        match calls {
            EEVec::Empty => {}
            EEVec::NonEmpty(_) => {
                panic!("empty blocks should keep empty call plans")
            }
        }
    }

    #[test]
    fn nonempty_block_builds_nonempty_call_plans() {
        let function_table = build_function_table();
        let calls = [
            Call::Function(function_call("child", vec![int(1)])),
            Call::Function(function_call("child", vec![int(2)])),
        ];

        let calls = build_parallel_call_plans::<TestSpec, TestLowering>(&calls, &function_table)
            .expect("non-empty blocks should build");

        match calls {
            EEVec::NonEmpty(calls) => {
                let (_first_call, calls) = calls.into_nonempty_iter().next();
                let call_count = 1 + calls.count();

                assert_eq!(call_count, 2);
            }
            EEVec::Empty => {
                panic!("non-empty blocks should keep non-empty call plans")
            }
        }
    }

    #[test]
    fn positional_plan_marks_every_target_initialized() {
        let function_table = build_function_table();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let calls = [
            Call::Function(function_call("child", vec![int(1)])),
            Call::Function(function_call("child", vec![int(2)])),
        ];

        let assignment = ParallelAssignmentPlan::<TestSpec>::build::<TestLowering, _>(
            NESlice::try_from_slice(&["left".to_owned(), "right".to_owned()]).unwrap(),
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
        .expect("matching targets should build positional plan");

        let ParallelAssignmentItems::Positional(targeted_items) = assignment.into_items() else {
            panic!("matching targets should build positional plan");
        };

        let (first_targeted_call, targeted_calls) = targeted_items.into_nonempty_iter().next();
        let mut awaited_targets = NEVec::new(ParallelTargeted::new(
            first_targeted_call.target(),
            Marked::mark(RegisterHandle::Existing(RegisterId(4))),
        ));

        for targeted_call in targeted_calls {
            awaited_targets.push(ParallelTargeted::new(
                targeted_call.target(),
                Marked::mark(RegisterHandle::Existing(RegisterId(7))),
            ));
        }

        let execution_plan = ParallelExecutionPlan::positional(awaited_targets);

        execution_plan.mark_initialized(&mut flow_state);

        assert!(
            local_frame
                .resolve_initialized_local("left", &flow_state)
                .is_some()
        );
        assert!(
            local_frame
                .resolve_initialized_local("right", &flow_state)
                .is_some()
        );
    }

    #[test]
    fn positional_assignment_builds_targeted_calls_in_order() {
        let function_table = build_function_table();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let calls = [
            Call::Function(function_call("child", vec![int(1)])),
            Call::Function(function_call("child", vec![int(2)])),
        ];
        let left_target_register = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "left",
        )
        .register();
        let right_target_register = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "right",
        )
        .register();

        let assignment = ParallelAssignmentPlan::<TestSpec>::build::<TestLowering, _>(
            NESlice::try_from_slice(&["left".to_owned(), "right".to_owned()]).unwrap(),
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
        .expect("matching targets should build positional plan");

        let ParallelAssignmentItems::Positional(targeted_items) = assignment.into_items() else {
            panic!("matching targets should build positional plan");
        };

        let (first_targeted_call, targeted_calls) = targeted_items.into_nonempty_iter().next();
        let targeted_calls = targeted_calls.collect::<Vec<_>>();

        assert_eq!(
            first_targeted_call.target().register(),
            left_target_register
        );
        assert_eq!(targeted_calls.len(), 1);
        assert_eq!(targeted_calls[0].target().register(), right_target_register);
    }

    #[test]
    fn mismatched_target_count_is_rejected() {
        let function_table = build_function_table();
        let calls = [Call::Function(function_call("only", vec![int(1)]))];
        let error = ParallelAssignmentPlan::<TestSpec>::build::<TestLowering, _>(
            NESlice::try_from_slice(&["left".to_owned(), "right".to_owned()]).unwrap(),
            &calls,
            &function_table,
            |_| panic!("mismatched target counts should fail before resolution"),
        )
        .expect_err("mismatched target counts should fail");

        assert!(matches!(
            error,
            Error::Unsupported(Unsupported::ParallelExprAssignment {
                target_count,
                call_count,
                ..
            }) if target_count.get() == 2 && call_count == 1
        ));
    }

    #[test]
    fn block_execution_plan_preserves_promise_registers() {
        let mut promise_registers =
            NEVec::new(Marked::mark(RegisterHandle::Existing(RegisterId(2))));
        promise_registers.push(Marked::mark(RegisterHandle::Existing(RegisterId(5))));

        let plan = ParallelExecutionPlan::block(EEVec::NonEmpty(promise_registers));

        match plan {
            ParallelExecutionPlan::Block {
                promise_registers: EEVec::NonEmpty(promise_registers),
            } => {
                let (first_register, promise_registers) =
                    promise_registers.into_nonempty_iter().next();
                let promise_registers = promise_registers.collect::<Vec<_>>();

                assert_eq!(first_register.register(), RegisterId(2));
                assert_eq!(promise_registers.len(), 1);
                assert_eq!(promise_registers[0].register(), RegisterId(5));
            }
            other => panic!("unexpected block execution plan {other:?}"),
        }
    }

    #[test]
    fn aggregate_execution_plan_keeps_target_and_promises() {
        let function_table = build_function_table();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let calls = [
            Call::Function(function_call("child", vec![int(1)])),
            Call::Function(function_call("child", vec![int(2)])),
        ];
        let target_register = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "results",
        )
        .register();
        let assignment = ParallelAssignmentPlan::<TestSpec>::build::<TestLowering, _>(
            NESlice::try_from_slice(&["results".to_owned()]).unwrap(),
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

        let ParallelAssignmentItems::Aggregate(ParallelTargeted { target, .. }) =
            assignment.into_items()
        else {
            panic!("single target should build aggregate plan");
        };

        let mut promise_registers =
            NEVec::new(Marked::mark(RegisterHandle::Existing(RegisterId(1))));
        promise_registers.push(Marked::mark(RegisterHandle::Existing(RegisterId(3))));

        let execution_plan =
            ParallelExecutionPlan::aggregate(target, EEVec::NonEmpty(promise_registers));

        match execution_plan {
            ParallelExecutionPlan::Assignment {
                assignment:
                    ParallelAssignmentItems::Aggregate(ParallelTargeted {
                        target,
                        payload: promise_registers,
                    }),
            } => {
                assert_eq!(target.register(), target_register);
                let EEVec::NonEmpty(promise_registers) = promise_registers else {
                    panic!("aggregate execution plan should keep non-empty promises")
                };
                let (first_register, promise_registers) =
                    promise_registers.into_nonempty_iter().next();
                let promise_registers = promise_registers.collect::<Vec<_>>();

                assert_eq!(first_register.register(), RegisterId(1));
                assert_eq!(promise_registers.len(), 1);
                assert_eq!(promise_registers[0].register(), RegisterId(3));
            }
            other => panic!("unexpected aggregate execution plan {other:?}"),
        }
    }

    #[test]
    fn positional_execution_plan_pairs_targets_with_promises() {
        let function_table = build_function_table();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let calls = [
            Call::Function(function_call("child", vec![int(1)])),
            Call::Function(function_call("child", vec![int(2)])),
        ];
        let left_target_register = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "left",
        )
        .register();
        let right_target_register = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "right",
        )
        .register();
        let assignment = ParallelAssignmentPlan::<TestSpec>::build::<TestLowering, _>(
            NESlice::try_from_slice(&["left".to_owned(), "right".to_owned()]).unwrap(),
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
        .expect("matching targets should build positional plan");

        let ParallelAssignmentItems::Positional(targeted_items) = assignment.into_items() else {
            panic!("matching targets should build positional plan");
        };

        let (first_targeted_call, targeted_calls) = targeted_items.into_nonempty_iter().next();
        let mut awaited_targets = NEVec::new(ParallelTargeted::new(
            first_targeted_call.target(),
            Marked::mark(RegisterHandle::Existing(RegisterId(4))),
        ));

        for targeted_call in targeted_calls {
            awaited_targets.push(ParallelTargeted::new(
                targeted_call.target(),
                Marked::mark(RegisterHandle::Existing(RegisterId(9))),
            ));
        }

        let execution_plan = ParallelExecutionPlan::positional(awaited_targets);

        match execution_plan {
            ParallelExecutionPlan::Assignment {
                assignment: ParallelAssignmentItems::Positional(targeted_items),
            } => {
                let (first_targeted_call, awaited_targets) =
                    targeted_items.into_nonempty_iter().next();
                let awaited_targets = awaited_targets.collect::<Vec<_>>();

                assert_eq!(
                    first_targeted_call.target().register(),
                    left_target_register
                );
                assert_eq!(
                    first_targeted_call.promise_register().register(),
                    RegisterId(4)
                );
                assert_eq!(awaited_targets.len(), 1);
                assert_eq!(
                    awaited_targets[0].target().register(),
                    right_target_register
                );
                assert_eq!(
                    awaited_targets[0].promise_register().register(),
                    RegisterId(9)
                );
            }
            other => panic!("unexpected positional execution plan {other:?}"),
        }
    }
}
