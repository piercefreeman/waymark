//! Expression planning for generic value positions.

use waymark_vm_ast_old::{
    ActionCall, BinaryOperator, DictEntry, Expr, FunctionCall, Literal, Spanned, UnaryOperator,
};

use super::Unsupported;

/// A normalized expression shape that the generic value compiler lowers directly.
///
/// This planner is intentionally narrower than the full AST surface. Some
/// constructs, such as parallel-expression assignments, are handled by
/// dedicated planners that need surrounding statement context.
#[derive(Debug)]
pub enum ExpressionPlan<'a> {
    /// A literal expression.
    Literal {
        /// Literal payload from the AST.
        value: &'a Literal,
    },

    /// A read from a local variable.
    Variable {
        /// Variable name to resolve.
        name: &'a str,
    },

    /// A scalar binary expression.
    BinaryOp {
        /// Left-hand operand.
        left: &'a Spanned<Expr>,

        /// Binary operator to apply.
        op: BinaryOperator,

        /// Right-hand operand.
        right: &'a Spanned<Expr>,
    },

    /// A scalar unary expression.
    UnaryOp {
        /// Unary operator to apply.
        op: UnaryOperator,

        /// Operand to evaluate.
        operand: &'a Spanned<Expr>,
    },

    /// A list literal.
    List {
        /// List items in source order.
        elements: &'a [Spanned<Expr>],
    },

    /// A dictionary literal.
    Dict {
        /// Dictionary entries in source order.
        entries: &'a [DictEntry],
    },

    /// Indexed access such as `items[0]`.
    Index {
        /// Expression that produces the indexed object.
        object: &'a Spanned<Expr>,

        /// Expression that produces the index value.
        index: &'a Spanned<Expr>,
    },

    /// Attribute access such as `record.field`.
    Dot {
        /// Expression that produces the accessed object.
        object: &'a Spanned<Expr>,

        /// Attribute name to look up.
        attribute: &'a str,
    },

    /// A call to another in-VM function.
    FunctionCall {
        /// Function-call payload from the AST.
        call: &'a FunctionCall,
    },

    /// A call to an external action.
    ActionCall {
        /// Action-call payload from the AST.
        call: &'a ActionCall,
    },
}

impl<'a> ExpressionPlan<'a> {
    /// Builds an expression plan for an AST expression used as a generic value.
    ///
    /// Parallel and spread expressions are excluded from this path because the
    /// compiler only lowers them through dedicated assignment planners, which
    /// need surrounding statement context.
    pub fn build(expr: &'a Spanned<Expr>) -> Result<Self, Unsupported> {
        match &expr.value {
            Expr::Literal { value } => Ok(Self::Literal { value }),
            Expr::Variable { name } => Ok(Self::Variable { name }),
            Expr::BinaryOp { left, op, right } => Ok(Self::BinaryOp {
                left,
                op: op.clone(),
                right,
            }),
            Expr::FunctionCall { call } => Ok(Self::FunctionCall { call }),
            Expr::ActionCall { call } => Ok(Self::ActionCall { call }),
            Expr::UnaryOp { op, operand } => Ok(Self::UnaryOp {
                op: op.clone(),
                operand,
            }),
            Expr::List { elements } => Ok(Self::List { elements }),
            Expr::Dict { entries } => Ok(Self::Dict { entries }),
            Expr::Index { object, index } => Ok(Self::Index { object, index }),
            Expr::Dot { object, attribute } => Ok(Self::Dot { object, attribute }),
            Expr::SpreadExpr { .. } => Err(Unsupported::SpreadExprOutsideAssignment),
            Expr::ParallelExpr { .. } => Err(Unsupported::ParallelExprOutsideAssignment),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use waymark_vm_ast_old::{BinaryOperator, Call, DictEntry, Expr, UnaryOperator};
    use waymark_vm_ast_old_helpers::{
        action_call, action_expr, function_expr, int, spanned, variable,
    };

    #[test]
    fn builds_scalar_operator_plans() {
        let supported_binary_ops = [
            BinaryOperator::Add,
            BinaryOperator::Sub,
            BinaryOperator::Mul,
            BinaryOperator::Div,
            BinaryOperator::FloorDiv,
            BinaryOperator::Mod,
            BinaryOperator::Eq,
            BinaryOperator::Ne,
            BinaryOperator::Lt,
            BinaryOperator::Le,
            BinaryOperator::Gt,
            BinaryOperator::Ge,
            BinaryOperator::In,
            BinaryOperator::NotIn,
            BinaryOperator::And,
            BinaryOperator::Or,
        ];

        for op in supported_binary_ops {
            let expr = spanned(Expr::BinaryOp {
                left: Box::new(int(1)),
                op: op.clone(),
                right: Box::new(int(2)),
            });

            let plan = ExpressionPlan::build(&expr).expect("binary expressions should build");

            assert!(matches!(
                plan,
                ExpressionPlan::BinaryOp { op: actual, .. } if actual == op
            ));
        }

        let supported_unary_ops = [UnaryOperator::Neg, UnaryOperator::Not];

        for op in supported_unary_ops {
            let expr = spanned(Expr::UnaryOp {
                op: op.clone(),
                operand: Box::new(int(1)),
            });

            let plan = ExpressionPlan::build(&expr).expect("unary expressions should build");

            assert!(matches!(
                plan,
                ExpressionPlan::UnaryOp { op: actual, .. } if actual == op
            ));
        }
    }

    #[test]
    fn builds_variable_call_collection_and_access_plans() {
        let variable_expr = variable("value");
        let function_expr = function_expr("child", vec![int(1)]);
        let action_expr = action_expr(
            waymark_action_core::ActionRuntime::Python,
            "fetch",
            vec![("value", int(2))],
        );
        let list_expr = spanned(Expr::List {
            elements: vec![int(1), int(2)],
        });
        let dict_expr = spanned(Expr::Dict {
            entries: vec![DictEntry {
                key: int(1),
                value: int(2),
            }],
        });
        let index_expr = spanned(Expr::Index {
            object: Box::new(variable("items")),
            index: Box::new(int(0)),
        });
        let dot_expr = spanned(Expr::Dot {
            object: Box::new(variable("record")),
            attribute: "field".to_owned(),
        });

        assert!(matches!(
            ExpressionPlan::build(&variable_expr).expect("variables should build"),
            ExpressionPlan::Variable { name } if name == "value"
        ));
        assert!(matches!(
            ExpressionPlan::build(&function_expr).expect("function calls should build"),
            ExpressionPlan::FunctionCall { call } if call.name == "child"
        ));
        assert!(matches!(
            ExpressionPlan::build(&action_expr).expect("action calls should build"),
            ExpressionPlan::ActionCall { call } if call.action_name == "fetch"
        ));
        assert!(matches!(
            ExpressionPlan::build(&list_expr).expect("lists should build"),
            ExpressionPlan::List { elements } if elements.len() == 2
        ));
        assert!(matches!(
            ExpressionPlan::build(&dict_expr).expect("dicts should build"),
            ExpressionPlan::Dict { entries } if entries.len() == 1
        ));
        assert!(matches!(
            ExpressionPlan::build(&index_expr).expect("indexes should build"),
            ExpressionPlan::Index { .. }
        ));
        assert!(matches!(
            ExpressionPlan::build(&dot_expr).expect("dots should build"),
            ExpressionPlan::Dot { attribute, .. } if attribute == "field"
        ));
    }

    #[test]
    fn rejects_spread_expressions_outside_assignment_context() {
        let expr = spanned(Expr::SpreadExpr {
            collection: Box::new(variable("items")),
            loop_var: "item".to_owned(),
            action: action_call(
                waymark_action_core::ActionRuntime::Python,
                "notify",
                vec![("value", variable("item"))],
            ),
        });

        let error = ExpressionPlan::build(&expr)
            .expect_err("spread expressions should stay assignment-specific");

        assert!(matches!(error, Unsupported::SpreadExprOutsideAssignment));
        assert_eq!(
            error.to_string(),
            "spread expressions are only supported on the right-hand side of assignments"
        );
    }

    #[test]
    fn rejects_parallel_expressions_outside_assignment_context() {
        let expr = spanned(Expr::ParallelExpr {
            calls: vec![Call::Action(action_call(
                waymark_action_core::ActionRuntime::Python,
                "notify",
                Vec::new(),
            ))],
        });

        let error =
            ExpressionPlan::build(&expr).expect_err("parallel expressions should stay special");

        assert!(matches!(error, Unsupported::ParallelExprOutsideAssignment));
        assert_eq!(
            error.to_string(),
            "parallel expressions are only supported on the right-hand side of assignments"
        );
    }
}
