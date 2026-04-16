//! Shared ValueExpr visitors for traversal, resolution, and evaluation.

use std::collections::{HashMap, HashSet};

use crate::ValueExpr;

use crate::state::{
    ActionCallSpec, BinaryOpValue, DictEntryValue, DictValue, DotValue, FunctionCallValue,
    IndexValue, ListValue, SpreadValue, UnaryOpValue,
};

/// Resolve variables inside a ValueExpr tree without executing actions.
///
/// Example IR:
/// - y = x + 1 (where x -> LiteralValue(2))
///   Produces BinaryOpValue(LiteralValue(2), +, LiteralValue(1)).
struct ValueExprResolver<'a> {
    resolve_variable: &'a dyn Fn(&str, &mut HashSet<String>) -> ValueExpr,
    seen: &'a mut HashSet<String>,
}

impl<'a> ValueExprResolver<'a> {
    pub fn new(
        resolve_variable: &'a dyn Fn(&str, &mut HashSet<String>) -> ValueExpr,
        seen: &'a mut HashSet<String>,
    ) -> Self {
        Self {
            resolve_variable,
            seen,
        }
    }

    pub fn visit(&mut self, expr: &ValueExpr) -> ValueExpr {
        match expr {
            ValueExpr::Literal(value) => ValueExpr::Literal(value.clone()),
            ValueExpr::Variable(value) => (self.resolve_variable)(&value.name, self.seen),
            ValueExpr::ActionResult(value) => ValueExpr::ActionResult(value.clone()),
            ValueExpr::BinaryOp(value) => ValueExpr::BinaryOp(BinaryOpValue {
                left: Box::new(self.visit(&value.left)),
                op: value.op,
                right: Box::new(self.visit(&value.right)),
            }),
            ValueExpr::UnaryOp(value) => ValueExpr::UnaryOp(UnaryOpValue {
                op: value.op,
                operand: Box::new(self.visit(&value.operand)),
            }),
            ValueExpr::List(value) => ValueExpr::List(ListValue {
                elements: value.elements.iter().map(|item| self.visit(item)).collect(),
            }),
            ValueExpr::Dict(value) => ValueExpr::Dict(DictValue {
                entries: value
                    .entries
                    .iter()
                    .map(|entry| DictEntryValue {
                        key: self.visit(&entry.key),
                        value: self.visit(&entry.value),
                    })
                    .collect(),
            }),
            ValueExpr::Index(value) => ValueExpr::Index(IndexValue {
                object: Box::new(self.visit(&value.object)),
                index: Box::new(self.visit(&value.index)),
            }),
            ValueExpr::Dot(value) => ValueExpr::Dot(DotValue {
                object: Box::new(self.visit(&value.object)),
                attribute: value.attribute.clone(),
            }),
            ValueExpr::FunctionCall(value) => ValueExpr::FunctionCall(FunctionCallValue {
                name: value.name.clone(),
                args: value.args.iter().map(|arg| self.visit(arg)).collect(),
                kwargs: value
                    .kwargs
                    .iter()
                    .map(|(name, arg)| (name.clone(), self.visit(arg)))
                    .collect(),
                global_function: value.global_function,
            }),
            ValueExpr::Spread(value) => {
                let kwargs = value
                    .action
                    .kwargs
                    .iter()
                    .map(|(name, arg)| (name.clone(), self.visit(arg)))
                    .collect::<HashMap<_, _>>();
                let action = ActionCallSpec {
                    action_name: value.action.action_name.clone(),
                    module_name: value.action.module_name.clone(),
                    kwargs,
                };
                ValueExpr::Spread(SpreadValue {
                    collection: Box::new(self.visit(&value.collection)),
                    loop_var: value.loop_var.clone(),
                    action,
                })
            }
        }
    }
}

/// Recursively resolve variable references throughout a value tree.
///
/// Use this as the core materialization step before assignment storage.
///
/// Example IR:
/// - z = (x + y) * 2
///   The tree walk replaces VariableValue("x")/("y") with their latest
///   symbolic definitions before storing z.
pub fn resolve_value_tree(
    value: &ValueExpr,
    resolve_variable: &dyn Fn(&str, &mut HashSet<String>) -> ValueExpr,
) -> ValueExpr {
    let mut seen = HashSet::new();
    let mut resolver = ValueExprResolver::new(resolve_variable, &mut seen);
    resolver.visit(value)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use serde_json::Value;
    use waymark_runner_expr::{LiteralValue, VariableValue};

    use crate::ValueExpr;

    use super::*;
    use waymark_proto::ast as ir;

    fn literal_int(value: i64) -> ValueExpr {
        ValueExpr::Literal(LiteralValue {
            value: Value::Number(value.into()),
        })
    }

    #[test]
    fn test_value_expr_resolver_visit_happy_path() {
        let mut seen = HashSet::new();
        let resolve = |name: &str, _: &mut HashSet<String>| {
            if name == "x" {
                literal_int(3)
            } else {
                literal_int(0)
            }
        };
        let mut resolver = ValueExprResolver::new(&resolve, &mut seen);
        let expr = ValueExpr::BinaryOp(BinaryOpValue {
            left: Box::new(ValueExpr::Variable(VariableValue {
                name: "x".to_string(),
            })),
            op: ir::BinaryOperator::BinaryOpAdd as i32,
            right: Box::new(literal_int(1)),
        });

        let resolved = resolver.visit(&expr);
        match resolved {
            ValueExpr::BinaryOp(value) => {
                assert!(matches!(*value.left, ValueExpr::Literal(_)));
                assert!(matches!(*value.right, ValueExpr::Literal(_)));
            }
            other => panic!("expected binary value, got {other:?}"),
        }
    }

    #[test]
    fn test_resolve_value_tree_happy_path() {
        let expr = ValueExpr::List(ListValue {
            elements: vec![ValueExpr::Variable(VariableValue {
                name: "user_id".to_string(),
            })],
        });
        let resolve = |name: &str, _seen: &mut HashSet<String>| {
            if name == "user_id" {
                ValueExpr::Literal(LiteralValue {
                    value: Value::String("abc".to_string()),
                })
            } else {
                ValueExpr::Literal(LiteralValue { value: Value::Null })
            }
        };

        let resolved = resolve_value_tree(&expr, &resolve);
        match resolved {
            ValueExpr::List(list) => {
                assert_eq!(list.elements.len(), 1);
                assert!(matches!(list.elements[0], ValueExpr::Literal(_)));
            }
            other => panic!("expected list value, got {other:?}"),
        }
    }
}
