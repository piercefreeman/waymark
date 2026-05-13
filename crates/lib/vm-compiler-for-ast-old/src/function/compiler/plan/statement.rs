//! Statement planning.

use waymark_vm_ast_old::{
    ActionCall, Block, ElifBranch, ElseBranch, Expr, IfBranch, Spanned, Statement,
};

use crate::function::table::FunctionTable;

use super::ErrorFor;
use super::Unsupported;
use super::parallel::{ParallelCallPlans, build_parallel_call_plans};

/// A normalized statement shape that the statement compiler knows how to lower.
#[derive(Debug)]
pub enum StatementPlan<'a, Spec>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// Assign an expression result into one or more targets.
    Assignment {
        /// Assignment targets in source order.
        targets: &'a [String],

        /// Expression that produces the assigned value.
        value: &'a Spanned<Expr>,
    },

    /// Invoke an action for side effects.
    ActionCall {
        /// Action-call payload from the AST.
        call: &'a ActionCall,
    },

    /// Return from the current function.
    Return {
        /// Optional expression whose value should be returned.
        value: Option<&'a Spanned<Expr>>,
    },

    /// Evaluate an expression statement for its side effects.
    Expr {
        /// Expression to evaluate.
        expr: &'a Spanned<Expr>,
    },

    /// Execute calls in parallel without assignment targets.
    ParallelBlock {
        /// Calls to start in parallel.
        calls: ParallelCallPlans<'a, Spec>,
    },

    /// A `while` loop.
    WhileLoop {
        /// Condition evaluated before each iteration.
        condition: &'a Spanned<Expr>,

        /// Loop body.
        body: &'a Spanned<Block>,
    },

    /// An `if`/`elif`/`else` chain.
    Conditional {
        /// Primary `if` branch.
        if_branch: &'a Spanned<IfBranch>,

        /// Zero or more `elif` branches.
        elif_branches: &'a [Spanned<ElifBranch>],

        /// Optional trailing `else` branch.
        else_branch: Option<&'a Spanned<ElseBranch>>,
    },

    /// Exit the innermost loop.
    Break,

    /// Continue the innermost loop.
    Continue,
}

/// Statement variants that the current lowering pipeline rejects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedStatementKind {
    /// Spread-action statements.
    SpreadAction,

    /// `for` loops.
    ForLoop,

    /// `try`/`except` blocks.
    TryExcept,

    /// Sleep statements.
    Sleep,
}

impl<'a, Spec> StatementPlan<'a, Spec>
where
    Spec: waymark_vm_compiler_for_ast_old_core::SpecRequirements,
{
    /// Builds a statement plan for a supported AST statement.
    pub fn build<Lowering>(
        statement: &'a Statement,
        function_table: &FunctionTable,
    ) -> Result<Self, ErrorFor<Spec, Lowering>>
    where
        Lowering: waymark_vm_compiler_for_ast_old_core::lowering::FullSet<Spec>,
    {
        match statement {
            Statement::Assignment { targets, value } => Ok(Self::Assignment { targets, value }),
            Statement::ActionCall { call } => Ok(Self::ActionCall { call }),
            Statement::Return { value } => Ok(Self::Return {
                value: value.as_ref(),
            }),
            Statement::ExprStmt { expr } => Ok(Self::Expr { expr }),
            Statement::ParallelBlock { calls } => Ok(Self::ParallelBlock {
                calls: build_parallel_call_plans::<Spec, Lowering>(calls, function_table)?,
            }),
            Statement::WhileLoop { condition, body } => Ok(Self::WhileLoop { condition, body }),
            Statement::Conditional {
                if_branch,
                elif_branches,
                else_branch,
            } => Ok(Self::Conditional {
                if_branch,
                elif_branches,
                else_branch: else_branch.as_ref(),
            }),
            Statement::Break => Ok(Self::Break),
            Statement::Continue => Ok(Self::Continue),
            Statement::SpreadAction { .. } => Err(Unsupported::Statement {
                kind: UnsupportedStatementKind::SpreadAction,
            }
            .into()),
            Statement::ForLoop { .. } => Err(Unsupported::Statement {
                kind: UnsupportedStatementKind::ForLoop,
            }
            .into()),
            Statement::TryExcept { .. } => Err(Unsupported::Statement {
                kind: UnsupportedStatementKind::TryExcept,
            }
            .into()),
            Statement::Sleep { .. } => Err(Unsupported::Statement {
                kind: UnsupportedStatementKind::Sleep,
            }
            .into()),
        }
    }
}

impl core::fmt::Display for UnsupportedStatementKind {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::SpreadAction => "SpreadAction",
            Self::ForLoop => "ForLoop",
            Self::TryExcept => "TryExcept",
            Self::Sleep => "Sleep",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use waymark_vm_ast_old::{Spanned, Statement};
    use waymark_vm_ast_old_helpers::{
        action_call, action_stmt, assignment, block, break_stmt, conditional_stmt, continue_stmt,
        int, parallel_stmt, return_stmt, spanned, variable, while_stmt,
    };

    use crate::function::compiler::{
        Error,
        test_helpers::{TestLowering, TestSpec, build_function_table},
    };

    #[test]
    fn builds_simple_statement_plans() {
        let function_table = build_function_table();
        let assignment = assignment("value", int(1));
        let action = action_stmt("notify");
        let returned = return_stmt(Some(variable("value")));
        let expr = spanned(Statement::ExprStmt {
            expr: variable("value"),
        });

        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(&assignment.value, &function_table)
                .expect("assignments should build"),
            StatementPlan::Assignment { targets, .. } if targets == ["value".to_owned()]
        ));
        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(&action.value, &function_table)
                .expect("actions should build"),
            StatementPlan::ActionCall { call } if call.action_name == "notify"
        ));
        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(&returned.value, &function_table)
                .expect("returns should build"),
            StatementPlan::Return { value: Some(_) }
        ));
        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(&expr.value, &function_table)
                .expect("expr statements should build"),
            StatementPlan::Expr { .. }
        ));
    }

    #[test]
    fn builds_control_flow_statement_plans() {
        let function_table = build_function_table();
        let conditional = conditional_stmt(int(1), vec![return_stmt(None)], Vec::new(), None);
        let while_loop = while_stmt(variable("flag"), vec![continue_stmt()]);
        let parallel = parallel_stmt(vec![waymark_vm_ast_old::Call::Action(action_call(
            "notify",
            Vec::new(),
        ))]);

        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(&conditional.value, &function_table)
                .expect("conditionals should build"),
            StatementPlan::Conditional { .. }
        ));
        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(&while_loop.value, &function_table)
                .expect("while loops should build"),
            StatementPlan::WhileLoop { .. }
        ));
        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(&parallel.value, &function_table)
                .expect("parallel blocks should build"),
            StatementPlan::ParallelBlock { .. }
        ));
        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(&break_stmt().value, &function_table)
                .expect("break should build"),
            StatementPlan::<TestSpec>::Break
        ));
        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(
                &continue_stmt().value,
                &function_table,
            )
            .expect("continue should build"),
            StatementPlan::<TestSpec>::Continue
        ));
    }

    #[test]
    fn rejects_unsupported_statement_variants() {
        let function_table = build_function_table();
        let unsupported = [
            (
                spanned(Statement::SpreadAction {
                    collection: variable("items"),
                    loop_var: "item".to_owned(),
                    action: action_call("notify", Vec::new()),
                }),
                UnsupportedStatementKind::SpreadAction,
            ),
            (
                waymark_vm_ast_old_helpers::for_stmt(&["item"], variable("items"), Vec::new()),
                UnsupportedStatementKind::ForLoop,
            ),
            (
                spanned(Statement::TryExcept {
                    handlers: Vec::<Spanned<waymark_vm_ast_old::ExceptHandler>>::new(),
                    try_block: block(Vec::new()),
                }),
                UnsupportedStatementKind::TryExcept,
            ),
            (
                spanned(Statement::Sleep {
                    duration: Some(int(1)),
                }),
                UnsupportedStatementKind::Sleep,
            ),
        ];

        for (statement, kind) in unsupported {
            let error =
                StatementPlan::<TestSpec>::build::<TestLowering>(&statement.value, &function_table)
                    .expect_err("unsupported statements should fail");

            assert!(
                matches!(error, Error::Unsupported(Unsupported::Statement { kind: actual }) if actual == kind)
            );
        }
    }
}
