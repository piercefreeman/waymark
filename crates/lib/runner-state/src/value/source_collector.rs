//! Shared ValueExpr visitors for traversal, resolution, and evaluation.

use std::collections::HashSet;

use waymark_ids::ExecutionId;

use crate::ValueExpr;

/// Collect execution node ids that supply data to a ValueExpr tree.
///
/// Example IR:
/// - total = a + @sum(values)
///   Returns the node ids that last defined `a` and the action node for sum().
struct ValueExprSourceCollector<'a> {
    resolve_variable: &'a dyn Fn(&str) -> Option<ExecutionId>,
}

impl<'a> ValueExprSourceCollector<'a> {
    pub fn new(resolve_variable: &'a dyn Fn(&str) -> Option<ExecutionId>) -> Self {
        Self { resolve_variable }
    }

    pub fn visit(&self, expr: &ValueExpr) -> HashSet<ExecutionId> {
        match expr {
            ValueExpr::Literal(_) => HashSet::new(),
            ValueExpr::Variable(value) => {
                (self.resolve_variable)(&value.name).into_iter().collect()
            }
            ValueExpr::ActionResult(value) => [value.node_id].into_iter().collect(),
            ValueExpr::BinaryOp(value) => {
                let mut sources = self.visit(&value.left);
                sources.extend(self.visit(&value.right));
                sources
            }
            ValueExpr::UnaryOp(value) => self.visit(&value.operand),
            ValueExpr::List(value) => {
                let mut sources = HashSet::new();
                for item in &value.elements {
                    sources.extend(self.visit(item));
                }
                sources
            }
            ValueExpr::Dict(value) => {
                let mut sources = HashSet::new();
                for entry in &value.entries {
                    sources.extend(self.visit(&entry.key));
                    sources.extend(self.visit(&entry.value));
                }
                sources
            }
            ValueExpr::Index(value) => {
                let mut sources = self.visit(&value.object);
                sources.extend(self.visit(&value.index));
                sources
            }
            ValueExpr::Dot(value) => self.visit(&value.object),
            ValueExpr::FunctionCall(value) => {
                let mut sources = HashSet::new();
                for arg in &value.args {
                    sources.extend(self.visit(arg));
                }
                for arg in value.kwargs.values() {
                    sources.extend(self.visit(arg));
                }
                sources
            }
            ValueExpr::Spread(value) => {
                let mut sources = self.visit(&value.collection);
                for arg in value.action.kwargs.values() {
                    sources.extend(self.visit(arg));
                }
                sources
            }
        }
    }
}

/// Find execution node ids that supply data to the given value.
///
/// Example IR:
/// - total = a + @sum(values)
///   Returns the latest assignment node for a and the action node for sum().
pub fn collect_value_sources(
    value: &ValueExpr,
    resolve_variable: &dyn Fn(&str) -> Option<ExecutionId>,
) -> HashSet<ExecutionId> {
    let collector = ValueExprSourceCollector::new(resolve_variable);
    collector.visit(value)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use waymark_runner_expr::{BinaryOpValue, FunctionCallValue, VariableValue};

    use crate::{ActionResultValue, ValueExpr};

    use super::*;
    use waymark_proto::ast as ir;

    #[test]
    fn test_value_expr_source_collector_visit_happy_path() {
        let variable_source = ExecutionId::new_uuid_v4();
        let action_source = ExecutionId::new_uuid_v4();
        let resolve = |name: &str| {
            if name == "x" {
                Some(variable_source)
            } else {
                None
            }
        };
        let collector = ValueExprSourceCollector::new(&resolve);
        let expr = ValueExpr::BinaryOp(BinaryOpValue {
            left: Box::new(ValueExpr::Variable(VariableValue {
                name: "x".to_string(),
            })),
            op: ir::BinaryOperator::BinaryOpAdd as i32,
            right: Box::new(ValueExpr::ActionResult(ActionResultValue {
                node_id: action_source,
                action_name: "fetch".to_string(),
                iteration_index: None,
                result_index: None,
            })),
        });

        let sources = collector.visit(&expr);
        assert!(sources.contains(&variable_source));
        assert!(sources.contains(&action_source));
    }

    #[test]
    fn test_collect_value_sources_happy_path() {
        let source_a = ExecutionId::new_uuid_v4();
        let source_b = ExecutionId::new_uuid_v4();
        let expr = ValueExpr::FunctionCall(FunctionCallValue {
            name: "sum".to_string(),
            args: vec![ValueExpr::Variable(VariableValue {
                name: "a".to_string(),
            })],
            kwargs: HashMap::from([(
                "other".to_string(),
                ValueExpr::ActionResult(ActionResultValue {
                    node_id: source_b,
                    action_name: "compute".to_string(),
                    iteration_index: None,
                    result_index: None,
                }),
            )]),
            global_function: None,
        });
        let resolve = |name: &str| if name == "a" { Some(source_a) } else { None };

        let sources = collect_value_sources(&expr, &resolve);
        assert_eq!(sources.len(), 2);
        assert!(sources.contains(&source_a));
        assert!(sources.contains(&source_b));
    }
}
