//! Shared ValueExpr visitors for traversal, resolution, and evaluation.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::state::{
    ActionCallSpec, ActionResultValue, BinaryOpValue, DictEntryValue, DictValue, DotValue,
    FunctionCallValue, IndexValue, ListValue, LiteralValue, SpreadValue, UnaryOpValue,
    VariableValue,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ValueExpr {
    Literal(LiteralValue),
    Variable(VariableValue),
    ActionResult(ActionResultValue),
    BinaryOp(BinaryOpValue),
    UnaryOp(UnaryOpValue),
    List(ListValue),
    Dict(DictValue),
    Index(IndexValue),
    Dot(DotValue),
    FunctionCall(FunctionCallValue),
    Spread(SpreadValue),
}

/// Resolve variables inside a ValueExpr tree without executing actions.
///
/// Example IR:
/// - y = x + 1 (where x -> LiteralValue(2))
///   Produces BinaryOpValue(LiteralValue(2), +, LiteralValue(1)).
pub struct ValueExprResolver<'a> {
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

/// Collect execution node ids that supply data to a ValueExpr tree.
///
/// Example IR:
/// - total = a + @sum(values)
///   Returns the node ids that last defined `a` and the action node for sum().
pub struct ValueExprSourceCollector<'a> {
    resolve_variable: &'a dyn Fn(&str) -> Option<Uuid>,
}

impl<'a> ValueExprSourceCollector<'a> {
    pub fn new(resolve_variable: &'a dyn Fn(&str) -> Option<Uuid>) -> Self {
        Self { resolve_variable }
    }

    pub fn visit(&self, expr: &ValueExpr) -> HashSet<Uuid> {
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

/// Evaluate ValueExpr nodes into concrete Python values.
///
/// Example:
/// - BinaryOpValue(VariableValue("a"), +, LiteralValue(1)) becomes the
///   current value of a plus 1.
pub struct ValueExprEvaluator<'a, E> {
    resolve_variable: &'a dyn Fn(&str) -> Result<serde_json::Value, E>,
    resolve_action_result: &'a dyn Fn(&ActionResultValue) -> Result<serde_json::Value, E>,
    resolve_function_call: &'a ResolveFunctionCall<'a, E>,
    apply_binary:
        &'a dyn Fn(i32, serde_json::Value, serde_json::Value) -> Result<serde_json::Value, E>,
    apply_unary: &'a dyn Fn(i32, serde_json::Value) -> Result<serde_json::Value, E>,
    error_factory: &'a dyn Fn(&str) -> E,
}

type ResolveFunctionCall<'a, E> = dyn Fn(
        &FunctionCallValue,
        Vec<serde_json::Value>,
        HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value, E>
    + 'a;

impl<'a, E> ValueExprEvaluator<'a, E> {
    pub fn new(
        resolve_variable: &'a dyn Fn(&str) -> Result<serde_json::Value, E>,
        resolve_action_result: &'a dyn Fn(&ActionResultValue) -> Result<serde_json::Value, E>,
        resolve_function_call: &'a ResolveFunctionCall<'a, E>,
        apply_binary: &'a dyn Fn(
            i32,
            serde_json::Value,
            serde_json::Value,
        ) -> Result<serde_json::Value, E>,
        apply_unary: &'a dyn Fn(i32, serde_json::Value) -> Result<serde_json::Value, E>,
        error_factory: &'a dyn Fn(&str) -> E,
    ) -> Self {
        Self {
            resolve_variable,
            resolve_action_result,
            resolve_function_call,
            apply_binary,
            apply_unary,
            error_factory,
        }
    }

    pub fn visit(&self, expr: &ValueExpr) -> Result<serde_json::Value, E> {
        match expr {
            ValueExpr::Literal(value) => Ok(value.value.clone()),
            ValueExpr::Variable(value) => (self.resolve_variable)(&value.name),
            ValueExpr::ActionResult(value) => (self.resolve_action_result)(value),
            ValueExpr::BinaryOp(value) => {
                let left = self.visit(&value.left)?;
                let right = self.visit(&value.right)?;
                (self.apply_binary)(value.op, left, right)
            }
            ValueExpr::UnaryOp(value) => {
                let operand = self.visit(&value.operand)?;
                (self.apply_unary)(value.op, operand)
            }
            ValueExpr::List(value) => {
                let mut items = Vec::with_capacity(value.elements.len());
                for item in &value.elements {
                    items.push(self.visit(item)?);
                }
                Ok(serde_json::Value::Array(items))
            }
            ValueExpr::Dict(value) => {
                let mut map = serde_json::Map::with_capacity(value.entries.len());
                for entry in &value.entries {
                    let key_value = self.visit(&entry.key)?;
                    let key = key_value
                        .as_str()
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| key_value.to_string());
                    let entry_value = self.visit(&entry.value)?;
                    map.insert(key, entry_value);
                }
                Ok(serde_json::Value::Object(map))
            }
            ValueExpr::Index(value) => {
                let object = self.visit(&value.object)?;
                let index = self.visit(&value.index)?;
                match (object, index) {
                    (serde_json::Value::Array(items), serde_json::Value::Number(idx)) => {
                        let idx = idx.as_i64().unwrap_or(-1);
                        if idx < 0 || idx as usize >= items.len() {
                            return Err((self.error_factory)("index out of range"));
                        }
                        Ok(items[idx as usize].clone())
                    }
                    (serde_json::Value::Object(map), serde_json::Value::String(key)) => map
                        .get(&key)
                        .cloned()
                        .or_else(|| lookup_exception_value(&map, &key))
                        .ok_or_else(|| (self.error_factory)("dict has no key")),
                    _ => Err((self.error_factory)("unsupported index operation")),
                }
            }
            ValueExpr::Dot(value) => {
                let object = self.visit(&value.object)?;
                if let serde_json::Value::Object(map) = object {
                    return map
                        .get(&value.attribute)
                        .cloned()
                        .or_else(|| lookup_exception_value(&map, &value.attribute))
                        .ok_or_else(|| (self.error_factory)("dict has no key"));
                }
                Err((self.error_factory)("attribute not found"))
            }
            ValueExpr::FunctionCall(value) => {
                let mut args = Vec::with_capacity(value.args.len());
                for arg in &value.args {
                    args.push(self.visit(arg)?);
                }
                let mut kwargs = HashMap::new();
                for (name, arg) in &value.kwargs {
                    kwargs.insert(name.clone(), self.visit(arg)?);
                }
                (self.resolve_function_call)(value, args, kwargs)
            }
            ValueExpr::Spread(_) => Err((self.error_factory)(
                "cannot replay unresolved spread expression",
            )),
        }
    }
}

fn lookup_exception_value(
    map: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<serde_json::Value> {
    if !(map.contains_key("type") && map.contains_key("message")) {
        return None;
    }
    map.get("values")
        .and_then(|value| value.as_object())
        .and_then(|values| values.get(key))
        .cloned()
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

/// Find execution node ids that supply data to the given value.
///
/// Example IR:
/// - total = a + @sum(values)
///   Returns the latest assignment node for a and the action node for sum().
pub fn collect_value_sources(
    value: &ValueExpr,
    resolve_variable: &dyn Fn(&str) -> Option<Uuid>,
) -> HashSet<Uuid> {
    let collector = ValueExprSourceCollector::new(resolve_variable);
    collector.visit(value)
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use serde_json::Value;
    use uuid::Uuid;

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
    fn test_value_expr_source_collector_visit_happy_path() {
        let variable_source = Uuid::new_v4();
        let action_source = Uuid::new_v4();
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
    fn test_value_expr_evaluator_visit_happy_path() {
        let resolve_variable = |name: &str| -> Result<Value, String> {
            if name == "x" {
                Ok(Value::Number(2.into()))
            } else {
                Err(format!("unknown variable: {name}"))
            }
        };
        let resolve_action_result =
            |_value: &ActionResultValue| -> Result<Value, String> { Ok(Value::Number(0.into())) };
        let resolve_function_call =
            |_call: &FunctionCallValue,
             args: Vec<Value>,
             _kwargs: HashMap<String, Value>|
             -> Result<Value, String> { Ok(Value::Number((args.len() as i64).into())) };
        let apply_binary = |_op: i32, left: Value, right: Value| -> Result<Value, String> {
            match (left.as_i64(), right.as_i64()) {
                (Some(left), Some(right)) => Ok(Value::Number((left + right).into())),
                _ => Err("bad operands".to_string()),
            }
        };
        let apply_unary = |_op: i32, value: Value| -> Result<Value, String> {
            Ok(Value::Bool(!value.as_bool().unwrap_or(false)))
        };
        let error_factory = |message: &str| message.to_string();

        let evaluator = ValueExprEvaluator::new(
            &resolve_variable,
            &resolve_action_result,
            &resolve_function_call,
            &apply_binary,
            &apply_unary,
            &error_factory,
        );
        let expr = ValueExpr::BinaryOp(BinaryOpValue {
            left: Box::new(ValueExpr::Variable(VariableValue {
                name: "x".to_string(),
            })),
            op: ir::BinaryOperator::BinaryOpAdd as i32,
            right: Box::new(literal_int(5)),
        });

        let value = evaluator.visit(&expr).expect("evaluate expression");
        assert_eq!(value, Value::Number(7.into()));
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

    #[test]
    fn test_collect_value_sources_happy_path() {
        let source_a = Uuid::new_v4();
        let source_b = Uuid::new_v4();
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
