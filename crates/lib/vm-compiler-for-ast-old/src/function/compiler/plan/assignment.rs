//! Assignment planning.

use nonempty_collections::NESlice;
use waymark_vm_ast_old::{Expr, FunctionCall, GlobalFunction, Spanned};

use super::ErrorFor;
use super::call::{CallPlan, CallPlanFor};
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

    /// A spread expression assignment lowered through looped call fan-out.
    Spread {
        /// Assignment target that will receive the collected list.
        target: Marked<LocalSlot, AssignmentTargetMarker>,

        /// Collection expression evaluated by the spread.
        collection: &'a Spanned<Expr>,

        /// Loop variable bound for each spread item.
        loop_var: &'a str,

        /// Planned action or function call started per collection item.
        call: CallPlanFor<'a, Spec>,
    },

    /// A spread expression evaluated only for its side effects.
    SpreadDiscard {
        /// Collection expression evaluated by the spread.
        collection: &'a Spanned<Expr>,

        /// Loop variable bound for each spread item.
        loop_var: &'a str,

        /// Planned action or function call started per collection item.
        call: CallPlanFor<'a, Spec>,
    },

    /// An assignment sourced from a parallel expression.
    Parallel {
        /// Validated parallel-assignment plan.
        assignment: ParallelAssignmentPlan<'a, Spec>,
    },

    /// A multi-target assignment unpacking a sequence-valued expression.
    Unpack {
        /// Assignment targets in declaration order.
        targets: Vec<Marked<LocalSlot, AssignmentTargetMarker>>,

        /// Expression evaluated once and unpacked into the targets by index.
        value: &'a Spanned<Expr>,
    },

    /// An assignment materializing a `range(...)` call into a list.
    RangeValues {
        /// Assignment target that will receive the materialized list.
        target: Marked<LocalSlot, AssignmentTargetMarker>,

        /// The `range(...)` call to materialize.
        call: &'a FunctionCall,
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
            if let Expr::SpreadExpr {
                collection,
                loop_var,
                call,
            } = &value.value
            {
                return Ok(Self::SpreadDiscard {
                    collection,
                    loop_var,
                    call: CallPlan::build::<Spec, Lowering, _>(call, function_table)?,
                });
            }

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

        if let Expr::SpreadExpr {
            collection,
            loop_var,
            call,
        } = &value.value
        {
            if targets.len().get() != 1 {
                return Err(super::plan::Unsupported::AssignmentTargetCount {
                    count: targets.len(),
                }
                .into());
            }

            let call = CallPlan::build::<Spec, Lowering, _>(call, function_table)?;
            return Ok(Self::Spread {
                target: resolve_target(&targets[0]),
                collection,
                loop_var,
                call,
            });
        }

        if targets.len().get() == 1 {
            if let Expr::FunctionCall { call } = &value.value
                && call.global_function == Some(GlobalFunction::Range)
            {
                return Ok(Self::RangeValues {
                    target: resolve_target(&targets[0]),
                    call,
                });
            }

            return Ok(Self::Direct {
                target: resolve_target(&targets[0]),
                value,
            });
        }

        Ok(Self::Unpack {
            targets: targets
                .iter()
                .map(|target| resolve_target(target))
                .collect(),
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use nonempty_collections::{IntoNonEmptyIterator as _, NonEmptyIterator as _};
    use waymark_vm_ast_old::Expr;
    use waymark_vm_ast_old_helpers::{
        action_call, function_call, int, parallel_expr, spread_expr, variable,
    };
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
            AssignmentStatementPlan::SpreadDiscard { .. } => {
                panic!("literal assignments should not build discard spread plans")
            }
            AssignmentStatementPlan::Unpack { .. } => {
                panic!("single-target assignments should not build unpack plans")
            }
            AssignmentStatementPlan::RangeValues { .. } => {
                panic!("literal assignments should not build range-values plans")
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
            AssignmentStatementPlan::SpreadDiscard { .. } => {
                panic!("parallel expressions should not build discard spread plans")
            }
            AssignmentStatementPlan::Unpack { .. } => {
                panic!("parallel expressions should not build unpack plans")
            }
            AssignmentStatementPlan::RangeValues { .. } => {
                panic!("parallel expressions should not build range-values plans")
            }
        }
    }

    #[test]
    fn non_spread_assignments_reject_zero_targets_before_parallel_planning() {
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
    fn zero_target_spread_assignments_build_discard_plan() {
        let function_table = build_function_table();
        let value = spread_expr(
            variable("items"),
            "item",
            waymark_vm_ast_old::Call::Action(action_call(
                "double",
                vec![("value", variable("item"))],
            )),
        );

        let plan = AssignmentStatementPlan::<TestSpec>::build::<TestLowering, _>(
            &[],
            &value,
            &function_table,
            |_| panic!("discard spreads should not resolve assignment targets"),
        )
        .expect("zero-target spread assignment should build");

        match plan {
            AssignmentStatementPlan::SpreadDiscard {
                collection,
                loop_var,
                call,
            } => {
                assert!(matches!(collection.value, Expr::Variable { ref name } if name == "items"));
                assert_eq!(loop_var, "item");
                let CallPlan::Action(action_plan) = call else {
                    panic!("action spreads should plan action calls");
                };
                let (_, _, action_name, _) = action_plan.into_parts();
                assert_eq!(action_name, "double");
            }
            AssignmentStatementPlan::Direct { .. } => {
                panic!("discard spread should not build a direct plan")
            }
            AssignmentStatementPlan::Spread { .. } => {
                panic!("discard spread should not build a targeted spread plan")
            }
            AssignmentStatementPlan::Parallel { .. } => {
                panic!("discard spread should not build a parallel plan")
            }
            AssignmentStatementPlan::Unpack { .. } => {
                panic!("discard spread should not build an unpack plan")
            }
            AssignmentStatementPlan::RangeValues { .. } => {
                panic!("discard spread should not build a range-values plan")
            }
        }
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
            waymark_vm_ast_old::Call::Action(action_call(
                "double",
                vec![("value", variable("item"))],
            )),
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
                call,
            } => {
                assert_eq!(target.register(), RegisterId(0));
                assert!(matches!(collection.value, Expr::Variable { ref name } if name == "items"));
                assert_eq!(loop_var, "item");
                let CallPlan::Action(action_plan) = call else {
                    panic!("action spreads should plan action calls");
                };
                let (_, _, action_name, _) = action_plan.into_parts();
                assert_eq!(action_name, "double");
            }
            AssignmentStatementPlan::Direct { .. } => {
                panic!("spread expressions should build spread plans")
            }
            AssignmentStatementPlan::Parallel { .. } => {
                panic!("spread expressions should not build parallel plans")
            }
            AssignmentStatementPlan::SpreadDiscard { .. } => {
                panic!("targeted spread expressions should not build discard plans")
            }
            AssignmentStatementPlan::Unpack { .. } => {
                panic!("targeted spread expressions should not build unpack plans")
            }
            AssignmentStatementPlan::RangeValues { .. } => {
                panic!("targeted spread expressions should not build range-values plans")
            }
        }
    }

    #[test]
    fn spread_assignment_plans_function_calls() {
        let function_table = build_function_table();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let targets = vec!["results".to_owned()];
        let value = spread_expr(
            variable("items"),
            "item",
            waymark_vm_ast_old::Call::Function(function_call("child", vec![variable("item")])),
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
        .expect("function spread assignment should build");

        match plan {
            AssignmentStatementPlan::Spread { call, .. } => {
                let CallPlan::Function(function_plan) = call else {
                    panic!("function spreads should plan function calls");
                };
                assert_eq!(function_plan.args().len(), 1);
            }
            other => panic!("function spreads should build spread plans, got {other:?}"),
        }
    }

    #[test]
    fn spread_assignment_rejects_unknown_function_calls() {
        let function_table = build_function_table();
        let value = spread_expr(
            variable("items"),
            "item",
            waymark_vm_ast_old::Call::Function(function_call("missing", vec![variable("item")])),
        );

        let error = AssignmentStatementPlan::<TestSpec>::build::<TestLowering, _>(
            &["results".to_owned()],
            &value,
            &function_table,
            |_| panic!("unknown-function spreads should fail before resolution"),
        )
        .expect_err("unknown-function spreads should fail");

        assert!(matches!(error, Error::UnknownFunction { name } if name == "missing"));
    }

    #[test]
    fn multi_target_assignment_builds_unpack_plan() {
        let function_table = build_function_table();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let targets = vec!["left".to_owned(), "right".to_owned()];
        let value = variable("pair");

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
        .expect("multi-target assignment should build");

        match plan {
            AssignmentStatementPlan::Unpack { targets, value } => {
                assert_eq!(targets.len(), 2);
                assert_eq!(targets[0].register(), RegisterId(0));
                assert_eq!(targets[1].register(), RegisterId(1));
                assert!(matches!(value.value, Expr::Variable { ref name } if name == "pair"));
            }
            other => panic!("multi-target assignments should build unpack plans, got {other:?}"),
        }
    }

    #[test]
    fn single_target_range_assignment_builds_range_values_plan() {
        let function_table = build_function_table();
        let mut local_frame = LocalFrame::new();
        let mut flow_state = FlowState::new();
        let targets = vec!["values".to_owned()];
        let mut call = waymark_vm_ast_old_helpers::function_call("range", vec![int(3)]);
        call.global_function = Some(GlobalFunction::Range);
        let value = waymark_vm_ast_old_helpers::spanned(Expr::FunctionCall { call });

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
        .expect("range assignment should build");

        match plan {
            AssignmentStatementPlan::RangeValues { target, call } => {
                assert_eq!(target.register(), RegisterId(0));
                assert_eq!(call.global_function, Some(GlobalFunction::Range));
                assert_eq!(call.args.len(), 1);
            }
            other => panic!("range assignments should build range-values plans, got {other:?}"),
        }
    }

    #[test]
    fn spread_assignment_rejects_multiple_targets() {
        let function_table = build_function_table();
        let value = spread_expr(
            variable("items"),
            "item",
            waymark_vm_ast_old::Call::Action(action_call(
                "double",
                vec![("value", variable("item"))],
            )),
        );
        let error = AssignmentStatementPlan::<TestSpec>::build::<TestLowering, _>(
            &["left".to_owned(), "right".to_owned()],
            &value,
            &function_table,
            |_| panic!("multi-target spreads should fail before resolution"),
        )
        .expect_err("multi-target spreads should fail");

        assert!(matches!(
            error,
            Error::Unsupported(crate::function::compiler::plan::Unsupported::AssignmentTargetCount { count }) if count.get() == 2
        ));
    }
}
