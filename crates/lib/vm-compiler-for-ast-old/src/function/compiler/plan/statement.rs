//! Statement planning.

use waymark_vm_ast_old::{
    ActionCall, Block, ElifBranch, ElseBranch, ExceptHandler, Expr, IfBranch, Spanned, Statement,
};

use crate::function::table::FunctionTable;

use super::ErrorFor;
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

    /// Fan out one action call across a collection.
    SpreadAction {
        /// Collection expression evaluated by the spread.
        collection: &'a Spanned<Expr>,

        /// Loop variable bound for each collection item.
        loop_var: &'a str,

        /// Action invoked for every item.
        action: &'a ActionCall,
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

    /// Suspend execution until the requested duration elapses.
    Sleep {
        /// Duration expression forwarded to the sleep runtime.
        duration: &'a Spanned<Expr>,
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

    /// A `for` loop.
    ForLoop {
        /// Loop variables bound on each iteration.
        loop_vars: &'a [String],

        /// Iterable expression evaluated by the loop.
        iterable: &'a Spanned<Expr>,

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

    /// A `try`/`except` block.
    TryExcept {
        /// Statements compiled in the protected region.
        try_block: &'a Spanned<Block>,

        /// Exception handlers compiled in source order.
        handlers: &'a [Spanned<ExceptHandler>],
    },

    /// Exit the innermost loop.
    Break,

    /// Continue the innermost loop.
    Continue,
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
            Statement::SpreadAction {
                collection,
                loop_var,
                action,
            } => Ok(Self::SpreadAction {
                collection,
                loop_var,
                action,
            }),
            Statement::Return { value } => Ok(Self::Return {
                value: value.as_ref(),
            }),
            Statement::ExprStmt { expr } => Ok(Self::Expr { expr }),
            Statement::Sleep { duration } => Ok(Self::Sleep { duration }),
            Statement::ParallelBlock { calls } => Ok(Self::ParallelBlock {
                calls: build_parallel_call_plans::<Spec, Lowering>(calls, function_table)?,
            }),
            Statement::WhileLoop { condition, body } => Ok(Self::WhileLoop { condition, body }),
            Statement::ForLoop {
                loop_vars,
                iterable,
                body,
            } => Ok(Self::ForLoop {
                loop_vars,
                iterable,
                body,
            }),
            Statement::Conditional {
                if_branch,
                elif_branches,
                else_branch,
            } => Ok(Self::Conditional {
                if_branch,
                elif_branches,
                else_branch: else_branch.as_ref(),
            }),
            Statement::TryExcept {
                handlers,
                try_block,
            } => Ok(Self::TryExcept {
                try_block,
                handlers,
            }),
            Statement::Break => Ok(Self::Break),
            Statement::Continue => Ok(Self::Continue),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use waymark_vm_ast_old::Statement;
    use waymark_vm_ast_old_helpers::{
        action_call, action_stmt, assignment, block, break_stmt, conditional_stmt, continue_stmt,
        for_stmt, int, parallel_stmt, return_stmt, sleep_stmt, spanned, variable, while_stmt,
    };
    use waymark_vm_compiler_for_ast_old_test_support::{TestLowering, TestSpec};

    use crate::function::compiler::test_helpers::build_function_table;

    #[test]
    fn builds_simple_statement_plans() {
        let function_table = build_function_table();
        let assignment = assignment("value", int(1));
        let action = action_stmt("notify");
        let returned = return_stmt(Some(variable("value")));
        let expr = spanned(Statement::ExprStmt {
            expr: variable("value"),
        });
        let sleep = sleep_stmt(int(1));

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
        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(&sleep.value, &function_table)
                .expect("sleep statements should build"),
            StatementPlan::Sleep { .. }
        ));
    }

    #[test]
    fn builds_control_flow_statement_plans() {
        let function_table = build_function_table();
        let conditional = conditional_stmt(int(1), vec![return_stmt(None)], Vec::new(), None);
        let for_loop = for_stmt(&["item"], variable("items"), vec![continue_stmt()]);
        let while_loop = while_stmt(variable("flag"), vec![continue_stmt()]);
        let parallel = parallel_stmt(vec![waymark_vm_ast_old::Call::Action(action_call(
            "notify",
            Vec::new(),
        ))]);
        let spread = spanned(Statement::SpreadAction {
            collection: variable("items"),
            loop_var: "item".to_owned(),
            action: action_call("notify", vec![("value", variable("item"))]),
        });

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
            StatementPlan::<TestSpec>::build::<TestLowering>(&for_loop.value, &function_table)
                .expect("for loops should build"),
            StatementPlan::ForLoop { loop_vars, .. } if loop_vars == ["item".to_owned()]
        ));
        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(&parallel.value, &function_table)
                .expect("parallel blocks should build"),
            StatementPlan::ParallelBlock { .. }
        ));
        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(&spread.value, &function_table)
                .expect("spread actions should build"),
            StatementPlan::SpreadAction {
                loop_var,
                action,
                ..
            } if loop_var == "item" && action.action_name == "notify"
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
    fn builds_try_except_statement_plans() {
        let function_table = build_function_table();
        let statement = spanned(Statement::TryExcept {
            handlers: vec![spanned(waymark_vm_ast_old::ExceptHandler {
                exception_types: vec!["ValueError".to_owned()],
                exception_var: Some("err".to_owned()),
                body: block(vec![return_stmt(Some(variable("err")))]),
            })],
            try_block: block(vec![return_stmt(Some(variable("value")))]),
        });

        assert!(matches!(
            StatementPlan::<TestSpec>::build::<TestLowering>(&statement.value, &function_table)
                .expect("try/except should build"),
            StatementPlan::TryExcept { handlers, .. }
                if handlers.len() == 1
                    && handlers[0].value.exception_var.as_deref() == Some("err")
        ));
    }
}
