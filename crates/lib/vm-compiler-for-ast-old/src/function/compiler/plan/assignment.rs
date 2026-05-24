//! Assignment planning.

use nonempty_collections::NESlice;
use waymark_vm_ast_old::{ActionCall, Expr, Spanned};

use super::ErrorFor;
use super::parallel::ParallelAssignmentPlan;

use crate::Marked;
use crate::function::compiler::env::{AssignmentTargetMarker, LocalSlot};
use crate::function::table::FunctionTable;

/// A validated assignment statement in direct or parallel form.
#[derive(Debug)]
pub enum AssignmentStatementPlan<'a, Spec>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// A single-target assignment lowered directly from an expression.
    Direct {
        /// Assignment target that will receive the computed value.
        target: Marked<LocalSlot, AssignmentTargetMarker>,

        /// Expression to evaluate and assign.
        value: &'a Spanned<Expr>,
    },

    /// A spread expression assignment lowered through looped action fan-out.
    Spread {
        /// Assignment target that will receive the collected list.
        target: Marked<LocalSlot, AssignmentTargetMarker>,

        /// Collection expression evaluated by the spread.
        collection: &'a Spanned<Expr>,

        /// Loop variable bound for each spread item.
        loop_var: &'a str,

        /// Action invoked per collection item.
        action: &'a ActionCall,
    },

    /// An assignment sourced from a parallel expression.
    Parallel {
        /// Validated parallel-assignment plan.
        assignment: ParallelAssignmentPlan<'a, Spec>,
    },
}

impl<'a, Spec> AssignmentStatementPlan<'a, Spec>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// Builds an assignment plan for the given targets and value expression.
    pub fn build<Lowering, F>(
        targets: &'a [String],
        value: &'a Spanned<Expr>,
        function_table: &FunctionTable,
        mut resolve_target: F,
    ) -> Result<Self, ErrorFor<Spec, Lowering>>
    where
        Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
        F: FnMut(&str) -> Marked<LocalSlot, AssignmentTargetMarker>,
    {
        let Some(targets) = NESlice::try_from_slice(targets) else {
            return Err(super::plan::Unsupported::AssignmentNoTargets.into());
        };

        if let Expr::ParallelExpr { calls } = &value.value {
            return Ok(Self::Parallel {
                assignment: ParallelAssignmentPlan::build::<Lowering, _>(
                    targets,
                    calls,
                    function_table,
                    resolve_target,
                )?,
            });
        }

        if targets.len().get() != 1 {
            return Err(super::plan::Unsupported::AssignmentTargetCount {
                count: targets.len(),
            }
            .into());
        }

        if let Expr::SpreadExpr {
            collection,
            loop_var,
            action,
        } = &value.value
        {
            return Ok(Self::Spread {
                target: resolve_target(&targets[0]),
                collection,
                loop_var,
                action,
            });
        }

        Ok(Self::Direct {
            target: resolve_target(&targets[0]),
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nonempty_collections::{IntoNonEmptyIterator as _, NonEmptyIterator as _};
    use waymark_vm_ast_old::Expr;
    use waymark_vm_ast_old_helpers::{action_call, int, parallel_expr, spread_expr, variable};
    use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};
    use waymark_vm_runtime_core::RegisterId;

    use crate::function::compiler::{
        Error,
        env::{AssignmentTargetMarker, FlowState, LocalFrame, LocalSlot},
        plan::parallel::ParallelAssignmentItems,
        test_helpers::build_function_table,
    };

    #[test]
    fn direct_assignment_resolves_a_single_target() {
        let function_table = build_function_table();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let targets = vec!["value".to_owned()];
        let value = int(7);
        let expected_target_register = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "value",
        )
        .register();

        let plan = AssignmentStatementPlan::<TestSpec>::build::<TestLowering, _>(
            &targets,
            &value,
            &function_table,
            |target| {
                Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
                    &mut local_frame,
                    &mut flow_state,
                    target,
                )
            },
        )
        .expect("direct assignment should build");

        match plan {
            AssignmentStatementPlan::Direct { target, value } => {
                assert_eq!(target.register(), expected_target_register);
                assert!(matches!(
                    value.value,
                    Expr::Literal {
                        value: waymark_vm_ast_old::Literal::Int(7),
                    }
                ));
            }
            AssignmentStatementPlan::Parallel { .. } => {
                panic!("non-parallel assignment should build a direct plan")
            }
            AssignmentStatementPlan::Spread { .. } => {
                panic!("literal assignments should not build spread plans")
            }
        }
    }

    #[test]
    fn direct_assignment_target_marks_flow_initialized() {
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let target = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "value",
        );
        let target_register = target.register();

        target.mark_initialized(&mut flow_state);

        let local = local_frame
            .resolve_initialized_local("value", &flow_state)
            .expect("value should resolve after target initialization");
        assert_eq!(local.register(), target_register);
    }

    #[test]
    fn assignment_target_get_or_declare_uses_local_frame_register() {
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();

        let target = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "value",
        );
        let same_target = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "value",
        );

        assert_eq!(target.register(), RegisterId(0));
        assert_eq!(same_target.register(), RegisterId(0));
        assert_eq!(local_frame.num_registers(), 1);
        assert!(
            local_frame
                .resolve_initialized_local("value", &flow_state)
                .is_none()
        );

        target.mark_initialized(&mut flow_state);

        let local = local_frame
            .resolve_initialized_local("value", &flow_state)
            .expect("value should resolve after target initialization");
        assert_eq!(local.register(), RegisterId(0));
    }

    #[test]
    fn parallel_assignment_resolves_plan_and_preserves_calls() {
        let function_table = build_function_table();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let targets = vec!["first".to_owned(), "second".to_owned()];
        let value = parallel_expr(vec![
            waymark_vm_ast_old::Call::Action(action_call("fetch_first", Vec::new())),
            waymark_vm_ast_old::Call::Action(action_call("fetch_second", Vec::new())),
        ]);
        let first_target_register = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "first",
        )
        .register();
        let second_target_register = Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
            &mut local_frame,
            &mut flow_state,
            "second",
        )
        .register();

        let plan = AssignmentStatementPlan::<TestSpec>::build::<TestLowering, _>(
            &targets,
            &value,
            &function_table,
            |target| {
                Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
                    &mut local_frame,
                    &mut flow_state,
                    target,
                )
            },
        )
        .expect("parallel assignment should build");

        match plan {
            AssignmentStatementPlan::Parallel { assignment } => {
                let ParallelAssignmentItems::Positional(targeted_items) = assignment.into_items()
                else {
                    panic!("parallel expressions should build a positional assignment plan");
                };

                let (first_targeted_call, targeted_calls) =
                    targeted_items.into_nonempty_iter().next();
                let targeted_calls = targeted_calls.collect::<Vec<_>>();

                assert_eq!(
                    first_targeted_call.target().register(),
                    first_target_register
                );
                assert_eq!(targeted_calls.len(), 1);
                assert_eq!(
                    targeted_calls[0].target().register(),
                    second_target_register
                );
            }
            AssignmentStatementPlan::Direct { .. } => {
                panic!("parallel expressions should build a parallel assignment plan")
            }
            AssignmentStatementPlan::Spread { .. } => {
                panic!("parallel expressions should not build spread plans")
            }
        }
    }

    #[test]
    fn assignments_reject_zero_targets_before_parallel_planning() {
        let function_table = build_function_table();
        let error = AssignmentStatementPlan::<TestSpec>::build::<TestLowering, _>(
            &[],
            &parallel_expr(Vec::new()),
            &function_table,
            |_| panic!("zero-target assignments should fail before resolution"),
        )
        .expect_err("zero-target assignments should fail");

        assert!(matches!(
            error,
            Error::Unsupported(crate::function::compiler::plan::Unsupported::AssignmentNoTargets)
        ));
    }

    #[test]
    fn spread_assignment_resolves_single_target_and_preserves_payload() {
        let function_table = build_function_table();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let targets = vec!["results".to_owned()];
        let value = spread_expr(
            variable("items"),
            "item",
            action_call("double", vec![("value", variable("item"))]),
        );

        let plan = AssignmentStatementPlan::<TestSpec>::build::<TestLowering, _>(
            &targets,
            &value,
            &function_table,
            |target| {
                Marked::<LocalSlot, AssignmentTargetMarker>::get_or_declare(
                    &mut local_frame,
                    &mut flow_state,
                    target,
                )
            },
        )
        .expect("spread assignment should build");

        match plan {
            AssignmentStatementPlan::Spread {
                target,
                collection,
                loop_var,
                action,
            } => {
                assert_eq!(target.register(), RegisterId(0));
                assert!(matches!(collection.value, Expr::Variable { ref name } if name == "items"));
                assert_eq!(loop_var, "item");
                assert_eq!(action.action_name, "double");
            }
            AssignmentStatementPlan::Direct { .. } => {
                panic!("spread expressions should build spread plans")
            }
            AssignmentStatementPlan::Parallel { .. } => {
                panic!("spread expressions should not build parallel plans")
            }
        }
    }

    #[test]
    fn direct_assignment_rejects_multiple_targets() {
        let function_table = build_function_table();
        let error = AssignmentStatementPlan::<TestSpec>::build::<TestLowering, _>(
            &["left".to_owned(), "right".to_owned()],
            &int(1),
            &function_table,
            |_| panic!("multiple non-parallel targets should fail before resolution"),
        )
        .expect_err("multiple targets should fail");

        assert!(matches!(
            error,
            Error::Unsupported(crate::function::compiler::plan::Unsupported::AssignmentTargetCount { count }) if count.get() == 2
        ));
    }
}
