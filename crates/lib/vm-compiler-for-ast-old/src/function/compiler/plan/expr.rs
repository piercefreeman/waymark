//! Expression planning for generic value positions.

use waymark_vm_ast_old::{ActionCall, BinaryOperator, Expr, FunctionCall, Literal, Spanned};

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

    /// An addition expression.
    Add {
        /// Left-hand operand.
        left: &'a Spanned<Expr>,

        /// Right-hand operand.
        right: &'a Spanned<Expr>,
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

/// Expression variants that the generic value-lowering path rejects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedExpressionKind {
    /// Unary operations such as `not value`.
    UnaryOp,

    /// List literals.
    List,

    /// Dictionary literals.
    Dict,

    /// Indexing expressions such as `items[0]`.
    Index,

    /// Attribute access such as `record.field`.
    Dot,

    /// Spread expressions.
    SpreadExpr,
}

impl<'a> ExpressionPlan<'a> {
    /// Builds an expression plan for an AST expression used as a generic value.
    ///
    /// Parallel expressions are excluded from this path because the compiler
    /// only lowers them through the dedicated assignment planner, which needs
    /// access to the assignment targets.
    pub fn build(expr: &'a Spanned<Expr>) -> Result<Self, Unsupported> {
        match &expr.value {
            Expr::Literal { value } => Ok(Self::Literal { value }),
            Expr::Variable { name } => Ok(Self::Variable { name }),
            Expr::BinaryOp { left, op, right } => {
                if !matches!(op, BinaryOperator::Add) {
                    return Err(Unsupported::BinaryOperator { op: op.clone() });
                }

                Ok(Self::Add { left, right })
            }
            Expr::FunctionCall { call } => Ok(Self::FunctionCall { call }),
            Expr::ActionCall { call } => Ok(Self::ActionCall { call }),
            Expr::UnaryOp { .. } => Err(Unsupported::Expression {
                kind: UnsupportedExpressionKind::UnaryOp,
            }),
            Expr::List { .. } => Err(Unsupported::Expression {
                kind: UnsupportedExpressionKind::List,
            }),
            Expr::Dict { .. } => Err(Unsupported::Expression {
                kind: UnsupportedExpressionKind::Dict,
            }),
            Expr::Index { .. } => Err(Unsupported::Expression {
                kind: UnsupportedExpressionKind::Index,
            }),
            Expr::Dot { .. } => Err(Unsupported::Expression {
                kind: UnsupportedExpressionKind::Dot,
            }),
            Expr::ParallelExpr { .. } => Err(Unsupported::ParallelExprOutsideAssignment),
            Expr::SpreadExpr { .. } => Err(Unsupported::Expression {
                kind: UnsupportedExpressionKind::SpreadExpr,
            }),
        }
    }
}

impl core::fmt::Display for UnsupportedExpressionKind {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self {
            Self::UnaryOp => "UnaryOp",
            Self::List => "List",
            Self::Dict => "Dict",
            Self::Index => "Index",
            Self::Dot => "Dot",
            Self::SpreadExpr => "SpreadExpr",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use waymark_vm_ast_old::{BinaryOperator, Call, DictEntry, Expr, Literal, UnaryOperator};
    use waymark_vm_ast_old_helpers::{
        action_call, action_expr, add, function_expr, int, spanned, variable,
    };

    #[test]
    fn builds_add_plan_for_supported_binary_ops() {
        let expr = add(int(1), int(2));

        let plan = ExpressionPlan::build(&expr).expect("add expressions should build");

        match plan {
            ExpressionPlan::Add { left, right } => {
                assert!(matches!(
                    left.value,
                    Expr::Literal {
                        value: Literal::Int(1),
                    }
                ));
                assert!(matches!(
                    right.value,
                    Expr::Literal {
                        value: Literal::Int(2),
                    }
                ));
            }
            other => panic!("unexpected expression plan {other:?}"),
        }
    }

    #[test]
    fn builds_variable_and_call_plans() {
        let variable_expr = variable("value");
        let function_expr = function_expr("child", vec![int(1)]);
        let action_expr = action_expr("fetch", vec![("value", int(2))]);

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
    }

    #[test]
    fn rejects_unsupported_binary_ops() {
        let expr = spanned(Expr::BinaryOp {
            left: Box::new(int(1)),
            op: BinaryOperator::Ne,
            right: Box::new(int(2)),
        });

        let error = ExpressionPlan::build(&expr).expect_err("unsupported binary ops should fail");

        assert!(matches!(
            error,
            Unsupported::BinaryOperator {
                op: BinaryOperator::Ne,
            }
        ));
    }

    #[test]
    fn rejects_unsupported_expression_variants() {
        let unsupported = [
            (
                spanned(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    operand: Box::new(int(1)),
                }),
                UnsupportedExpressionKind::UnaryOp,
            ),
            (
                spanned(Expr::List {
                    elements: vec![int(1)],
                }),
                UnsupportedExpressionKind::List,
            ),
            (
                spanned(Expr::Dict {
                    entries: vec![DictEntry {
                        key: int(1),
                        value: int(2),
                    }],
                }),
                UnsupportedExpressionKind::Dict,
            ),
            (
                spanned(Expr::Index {
                    object: Box::new(variable("items")),
                    index: Box::new(int(0)),
                }),
                UnsupportedExpressionKind::Index,
            ),
            (
                spanned(Expr::Dot {
                    object: Box::new(variable("record")),
                    attribute: "field".to_owned(),
                }),
                UnsupportedExpressionKind::Dot,
            ),
            (
                spanned(Expr::SpreadExpr {
                    collection: Box::new(variable("items")),
                    loop_var: "item".to_owned(),
                    action: action_call("notify", vec![("value", variable("item"))]),
                }),
                UnsupportedExpressionKind::SpreadExpr,
            ),
        ];

        for (expr, kind) in unsupported {
            let error =
                ExpressionPlan::build(&expr).expect_err("unsupported expressions should fail");

            assert!(matches!(
                error,
                Unsupported::Expression { kind: actual } if actual == kind
            ));
        }
    }

    #[test]
    fn rejects_parallel_expressions_outside_assignment_context() {
        let expr = spanned(Expr::ParallelExpr {
            calls: vec![Call::Action(action_call("notify", Vec::new()))],
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
