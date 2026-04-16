//! Shared ValueExpr visitors for traversal, resolution, and evaluation.

use std::collections::HashMap;

use crate::ValueExpr;

use crate::state::{ActionResultValue, FunctionCallValue};

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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::Value;
    use waymark_runner_expr::{BinaryOpValue, LiteralValue, VariableValue};

    use crate::ValueExpr;

    use super::*;
    use waymark_proto::ast as ir;

    fn literal_int(value: i64) -> ValueExpr {
        ValueExpr::Literal(LiteralValue {
            value: Value::Number(value.into()),
        })
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
}
