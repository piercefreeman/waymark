//! AST Expression Evaluator
//!
//! Evaluates AST expressions (boolean logic, arithmetic, comparisons, etc.)
//! at runtime against a variable scope.
//!
//! This module provides the core expression evaluation logic used by the DAG runner
//! to evaluate guard expressions, assignments, and inline expressions.

use std::collections::HashMap;

use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::parser::ast;

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during expression evaluation.
#[derive(Debug, Error)]
pub enum EvaluationError {
    #[error("Expression evaluation error: {0}")]
    Evaluation(String),

    #[error("Variable not found: {0}")]
    VariableNotFound(String),

    #[error("Function not found: {0}")]
    FunctionNotFound(String),
}

pub type EvaluationResult<T> = Result<T, EvaluationError>;

// ============================================================================
// Scope
// ============================================================================

/// Scope for inline expression evaluation.
/// This is a simple in-memory variable map used during inline node execution.
pub type Scope = HashMap<String, JsonValue>;

// ============================================================================
// Expression Evaluator
// ============================================================================

/// Evaluates AST expressions in a given context.
pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    /// Evaluate an expression to a runtime value.
    pub fn evaluate(expr: &ast::Expr, scope: &Scope) -> EvaluationResult<JsonValue> {
        let kind = expr
            .kind
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Empty expression".to_string()))?;

        match kind {
            ast::expr::Kind::Literal(lit) => Self::eval_literal(lit),
            ast::expr::Kind::Variable(var) => Self::eval_variable(var, scope),
            ast::expr::Kind::BinaryOp(op) => Self::eval_binary_op(op, scope),
            ast::expr::Kind::UnaryOp(op) => Self::eval_unary_op(op, scope),
            ast::expr::Kind::List(list) => Self::eval_list(list, scope),
            ast::expr::Kind::Dict(dict) => Self::eval_dict(dict, scope),
            ast::expr::Kind::Index(idx) => Self::eval_index(idx, scope),
            ast::expr::Kind::Dot(dot) => Self::eval_dot(dot, scope),
            ast::expr::Kind::FunctionCall(call) => Self::eval_function_call(call, scope),
            ast::expr::Kind::ActionCall(_) => Err(EvaluationError::Evaluation(
                "Action calls cannot be evaluated inline".to_string(),
            )),
        }
    }

    fn eval_literal(lit: &ast::Literal) -> EvaluationResult<JsonValue> {
        let value = lit
            .value
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Empty literal".to_string()))?;

        Ok(match value {
            ast::literal::Value::IntValue(i) => JsonValue::Number((*i).into()),
            ast::literal::Value::FloatValue(f) => JsonValue::Number(
                serde_json::Number::from_f64(*f).unwrap_or_else(|| serde_json::Number::from(0)),
            ),
            ast::literal::Value::StringValue(s) => JsonValue::String(s.clone()),
            ast::literal::Value::BoolValue(b) => JsonValue::Bool(*b),
            ast::literal::Value::IsNone(true) => JsonValue::Null,
            ast::literal::Value::IsNone(false) => JsonValue::Null,
        })
    }

    fn eval_variable(var: &ast::Variable, scope: &Scope) -> EvaluationResult<JsonValue> {
        scope
            .get(&var.name)
            .cloned()
            .ok_or_else(|| EvaluationError::VariableNotFound(var.name.clone()))
    }

    fn eval_binary_op(op: &ast::BinaryOp, scope: &Scope) -> EvaluationResult<JsonValue> {
        let left_expr = op
            .left
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Missing left operand".to_string()))?;
        let right_expr = op
            .right
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Missing right operand".to_string()))?;

        let left = Self::evaluate(left_expr, scope)?;
        let right = Self::evaluate(right_expr, scope)?;

        match ast::BinaryOperator::try_from(op.op)
            .unwrap_or(ast::BinaryOperator::BinaryOpUnspecified)
        {
            ast::BinaryOperator::BinaryOpAdd => Self::apply_add(&left, &right),
            ast::BinaryOperator::BinaryOpSub => Self::apply_sub(&left, &right),
            ast::BinaryOperator::BinaryOpMul => Self::apply_mul(&left, &right),
            ast::BinaryOperator::BinaryOpDiv => Self::apply_div(&left, &right),
            ast::BinaryOperator::BinaryOpEq => Ok(JsonValue::Bool(left == right)),
            ast::BinaryOperator::BinaryOpNe => Ok(JsonValue::Bool(left != right)),
            ast::BinaryOperator::BinaryOpLt => Self::apply_lt(&left, &right),
            ast::BinaryOperator::BinaryOpLe => Self::apply_le(&left, &right),
            ast::BinaryOperator::BinaryOpGt => Self::apply_gt(&left, &right),
            ast::BinaryOperator::BinaryOpGe => Self::apply_ge(&left, &right),
            ast::BinaryOperator::BinaryOpAnd => Self::apply_and(&left, &right),
            ast::BinaryOperator::BinaryOpOr => Self::apply_or(&left, &right),
            ast::BinaryOperator::BinaryOpIn => Self::apply_in(&left, &right),
            ast::BinaryOperator::BinaryOpNotIn => {
                let result = Self::apply_in(&left, &right)?;
                Ok(JsonValue::Bool(!result.as_bool().unwrap_or(false)))
            }
            _ => Err(EvaluationError::Evaluation(
                "Unknown binary operator".to_string(),
            )),
        }
    }

    fn eval_unary_op(op: &ast::UnaryOp, scope: &Scope) -> EvaluationResult<JsonValue> {
        let operand_expr = op
            .operand
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Missing operand".to_string()))?;

        let operand = Self::evaluate(operand_expr, scope)?;

        match ast::UnaryOperator::try_from(op.op).unwrap_or(ast::UnaryOperator::UnaryOpUnspecified)
        {
            ast::UnaryOperator::UnaryOpNeg => match &operand {
                JsonValue::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Ok(JsonValue::Number((-i).into()))
                    } else if let Some(f) = n.as_f64() {
                        Ok(JsonValue::Number(
                            serde_json::Number::from_f64(-f)
                                .unwrap_or_else(|| serde_json::Number::from(0)),
                        ))
                    } else {
                        Err(EvaluationError::Evaluation(
                            "Cannot negate non-numeric value".to_string(),
                        ))
                    }
                }
                _ => Err(EvaluationError::Evaluation(
                    "Cannot negate non-numeric value".to_string(),
                )),
            },
            ast::UnaryOperator::UnaryOpNot => Ok(JsonValue::Bool(!Self::is_truthy(&operand))),
            _ => Err(EvaluationError::Evaluation(
                "Unknown unary operator".to_string(),
            )),
        }
    }

    fn eval_list(list: &ast::ListExpr, scope: &Scope) -> EvaluationResult<JsonValue> {
        let elements: Result<Vec<JsonValue>, _> = list
            .elements
            .iter()
            .map(|e| Self::evaluate(e, scope))
            .collect();
        Ok(JsonValue::Array(elements?))
    }

    fn eval_dict(dict: &ast::DictExpr, scope: &Scope) -> EvaluationResult<JsonValue> {
        let mut map = serde_json::Map::new();
        for entry in &dict.entries {
            let key_expr = entry
                .key
                .as_ref()
                .ok_or_else(|| EvaluationError::Evaluation("Missing dict key".to_string()))?;
            let val_expr = entry
                .value
                .as_ref()
                .ok_or_else(|| EvaluationError::Evaluation("Missing dict value".to_string()))?;

            let key = Self::evaluate(key_expr, scope)?;
            let val = Self::evaluate(val_expr, scope)?;

            let key_str = match key {
                JsonValue::String(s) => s,
                other => other.to_string(),
            };
            map.insert(key_str, val);
        }
        Ok(JsonValue::Object(map))
    }

    fn eval_index(idx: &ast::IndexAccess, scope: &Scope) -> EvaluationResult<JsonValue> {
        let obj_expr = idx
            .object
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Missing index object".to_string()))?;
        let idx_expr = idx
            .index
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Missing index".to_string()))?;

        let obj = Self::evaluate(obj_expr, scope)?;
        let index = Self::evaluate(idx_expr, scope)?;

        match (&obj, &index) {
            (JsonValue::Array(arr), JsonValue::Number(n)) => {
                let i = n.as_i64().unwrap_or(0) as usize;
                arr.get(i)
                    .cloned()
                    .ok_or_else(|| EvaluationError::Evaluation(format!("Index {} out of bounds", i)))
            }
            (JsonValue::Object(map), JsonValue::String(key)) => map
                .get(key)
                .cloned()
                .ok_or_else(|| EvaluationError::Evaluation(format!("Key '{}' not found", key))),
            (JsonValue::String(s), JsonValue::Number(n)) => {
                let i = n.as_i64().unwrap_or(0) as usize;
                s.chars()
                    .nth(i)
                    .map(|c| JsonValue::String(c.to_string()))
                    .ok_or_else(|| EvaluationError::Evaluation(format!("Index {} out of bounds", i)))
            }
            _ => Err(EvaluationError::Evaluation(
                "Invalid index operation".to_string(),
            )),
        }
    }

    fn eval_dot(dot: &ast::DotAccess, scope: &Scope) -> EvaluationResult<JsonValue> {
        let obj_expr = dot
            .object
            .as_ref()
            .ok_or_else(|| EvaluationError::Evaluation("Missing dot object".to_string()))?;

        let obj = Self::evaluate(obj_expr, scope)?;

        match &obj {
            JsonValue::Object(map) => map.get(&dot.attribute).cloned().ok_or_else(|| {
                EvaluationError::Evaluation(format!("Attribute '{}' not found", dot.attribute))
            }),
            _ => Err(EvaluationError::Evaluation(
                "Dot access on non-object".to_string(),
            )),
        }
    }

    fn eval_function_call(call: &ast::FunctionCall, scope: &Scope) -> EvaluationResult<JsonValue> {
        // Evaluate kwargs
        let mut kwargs = HashMap::new();
        for kwarg in &call.kwargs {
            let val_expr = kwarg
                .value
                .as_ref()
                .ok_or_else(|| EvaluationError::Evaluation("Missing kwarg value".to_string()))?;
            let val = Self::evaluate(val_expr, scope)?;
            kwargs.insert(kwarg.name.clone(), val);
        }

        // Built-in functions
        match call.name.as_str() {
            "range" => Self::builtin_range(&kwargs),
            "len" => Self::builtin_len(&kwargs),
            "enumerate" => Self::builtin_enumerate(&kwargs),
            _ => Err(EvaluationError::FunctionNotFound(call.name.clone())),
        }
    }

    // Helper methods for operators
    fn apply_add(left: &JsonValue, right: &JsonValue) -> EvaluationResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                    Ok(JsonValue::Number((ai + bi).into()))
                } else if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                    Ok(JsonValue::Number(
                        serde_json::Number::from_f64(af + bf)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                } else {
                    Err(EvaluationError::Evaluation(
                        "Cannot add incompatible numbers".to_string(),
                    ))
                }
            }
            (JsonValue::String(a), JsonValue::String(b)) => {
                Ok(JsonValue::String(format!("{}{}", a, b)))
            }
            (JsonValue::Array(a), JsonValue::Array(b)) => {
                let mut result = a.clone();
                result.extend(b.clone());
                Ok(JsonValue::Array(result))
            }
            _ => Err(EvaluationError::Evaluation(
                "Cannot add incompatible types".to_string(),
            )),
        }
    }

    fn apply_sub(left: &JsonValue, right: &JsonValue) -> EvaluationResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                    Ok(JsonValue::Number((ai - bi).into()))
                } else if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                    Ok(JsonValue::Number(
                        serde_json::Number::from_f64(af - bf)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                } else {
                    Err(EvaluationError::Evaluation(
                        "Cannot subtract incompatible numbers".to_string(),
                    ))
                }
            }
            _ => Err(EvaluationError::Evaluation(
                "Cannot subtract non-numbers".to_string(),
            )),
        }
    }

    fn apply_mul(left: &JsonValue, right: &JsonValue) -> EvaluationResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
                    Ok(JsonValue::Number((ai * bi).into()))
                } else if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
                    Ok(JsonValue::Number(
                        serde_json::Number::from_f64(af * bf)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                } else {
                    Err(EvaluationError::Evaluation(
                        "Cannot multiply incompatible numbers".to_string(),
                    ))
                }
            }
            _ => Err(EvaluationError::Evaluation(
                "Cannot multiply non-numbers".to_string(),
            )),
        }
    }

    fn apply_div(left: &JsonValue, right: &JsonValue) -> EvaluationResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let af = a.as_f64().unwrap_or(0.0);
                let bf = b.as_f64().unwrap_or(1.0);
                if bf == 0.0 {
                    Err(EvaluationError::Evaluation("Division by zero".to_string()))
                } else {
                    Ok(JsonValue::Number(
                        serde_json::Number::from_f64(af / bf)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    ))
                }
            }
            _ => Err(EvaluationError::Evaluation(
                "Cannot divide non-numbers".to_string(),
            )),
        }
    }

    fn apply_lt(left: &JsonValue, right: &JsonValue) -> EvaluationResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let af = a.as_f64().unwrap_or(0.0);
                let bf = b.as_f64().unwrap_or(0.0);
                Ok(JsonValue::Bool(af < bf))
            }
            (JsonValue::String(a), JsonValue::String(b)) => Ok(JsonValue::Bool(a < b)),
            _ => Err(EvaluationError::Evaluation(
                "Cannot compare incompatible types".to_string(),
            )),
        }
    }

    fn apply_le(left: &JsonValue, right: &JsonValue) -> EvaluationResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let af = a.as_f64().unwrap_or(0.0);
                let bf = b.as_f64().unwrap_or(0.0);
                Ok(JsonValue::Bool(af <= bf))
            }
            (JsonValue::String(a), JsonValue::String(b)) => Ok(JsonValue::Bool(a <= b)),
            _ => Err(EvaluationError::Evaluation(
                "Cannot compare incompatible types".to_string(),
            )),
        }
    }

    fn apply_gt(left: &JsonValue, right: &JsonValue) -> EvaluationResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let af = a.as_f64().unwrap_or(0.0);
                let bf = b.as_f64().unwrap_or(0.0);
                Ok(JsonValue::Bool(af > bf))
            }
            (JsonValue::String(a), JsonValue::String(b)) => Ok(JsonValue::Bool(a > b)),
            _ => Err(EvaluationError::Evaluation(
                "Cannot compare incompatible types".to_string(),
            )),
        }
    }

    fn apply_ge(left: &JsonValue, right: &JsonValue) -> EvaluationResult<JsonValue> {
        match (left, right) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let af = a.as_f64().unwrap_or(0.0);
                let bf = b.as_f64().unwrap_or(0.0);
                Ok(JsonValue::Bool(af >= bf))
            }
            (JsonValue::String(a), JsonValue::String(b)) => Ok(JsonValue::Bool(a >= b)),
            _ => Err(EvaluationError::Evaluation(
                "Cannot compare incompatible types".to_string(),
            )),
        }
    }

    fn apply_and(left: &JsonValue, right: &JsonValue) -> EvaluationResult<JsonValue> {
        Ok(JsonValue::Bool(
            Self::is_truthy(left) && Self::is_truthy(right),
        ))
    }

    fn apply_or(left: &JsonValue, right: &JsonValue) -> EvaluationResult<JsonValue> {
        Ok(JsonValue::Bool(
            Self::is_truthy(left) || Self::is_truthy(right),
        ))
    }

    fn apply_in(left: &JsonValue, right: &JsonValue) -> EvaluationResult<JsonValue> {
        match right {
            JsonValue::Array(arr) => Ok(JsonValue::Bool(arr.contains(left))),
            JsonValue::Object(map) => {
                let key = match left {
                    JsonValue::String(s) => s.clone(),
                    other => other.to_string(),
                };
                Ok(JsonValue::Bool(map.contains_key(&key)))
            }
            JsonValue::String(s) => {
                let needle = match left {
                    JsonValue::String(n) => n.clone(),
                    other => other.to_string(),
                };
                Ok(JsonValue::Bool(s.contains(&needle)))
            }
            _ => Err(EvaluationError::Evaluation(
                "'in' requires array, object, or string".to_string(),
            )),
        }
    }

    /// Check if a value is truthy (Python-style semantics).
    pub fn is_truthy(value: &JsonValue) -> bool {
        match value {
            JsonValue::Null => false,
            JsonValue::Bool(b) => *b,
            JsonValue::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(false),
            JsonValue::String(s) => !s.is_empty(),
            JsonValue::Array(a) => !a.is_empty(),
            JsonValue::Object(o) => !o.is_empty(),
        }
    }

    // Built-in functions
    fn builtin_range(kwargs: &HashMap<String, JsonValue>) -> EvaluationResult<JsonValue> {
        let start = kwargs.get("start").and_then(|v| v.as_i64()).unwrap_or(0);
        let stop = kwargs.get("stop").and_then(|v| v.as_i64()).ok_or_else(|| {
            EvaluationError::Evaluation("range() requires 'stop' argument".to_string())
        })?;
        let step = kwargs.get("step").and_then(|v| v.as_i64()).unwrap_or(1);

        if step == 0 {
            return Err(EvaluationError::Evaluation(
                "range() step cannot be zero".to_string(),
            ));
        }

        let mut result = Vec::new();
        let mut i = start;
        while (step > 0 && i < stop) || (step < 0 && i > stop) {
            result.push(JsonValue::Number(i.into()));
            i += step;
        }

        Ok(JsonValue::Array(result))
    }

    fn builtin_len(kwargs: &HashMap<String, JsonValue>) -> EvaluationResult<JsonValue> {
        let items = kwargs.get("items").ok_or_else(|| {
            EvaluationError::Evaluation("len() requires 'items' argument".to_string())
        })?;

        let len = match items {
            JsonValue::Array(a) => a.len(),
            JsonValue::Object(o) => o.len(),
            JsonValue::String(s) => s.len(),
            _ => {
                return Err(EvaluationError::Evaluation(
                    "len() requires array, object, or string".to_string(),
                ));
            }
        };

        Ok(JsonValue::Number((len as i64).into()))
    }

    fn builtin_enumerate(kwargs: &HashMap<String, JsonValue>) -> EvaluationResult<JsonValue> {
        let items = kwargs.get("items").ok_or_else(|| {
            EvaluationError::Evaluation("enumerate() requires 'items' argument".to_string())
        })?;

        let arr = match items {
            JsonValue::Array(a) => a,
            _ => {
                return Err(EvaluationError::Evaluation(
                    "enumerate() requires array".to_string(),
                ));
            }
        };

        let result: Vec<JsonValue> = arr
            .iter()
            .enumerate()
            .map(|(i, v)| JsonValue::Array(vec![JsonValue::Number((i as i64).into()), v.clone()]))
            .collect();

        Ok(JsonValue::Array(result))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a literal expression
    fn int_literal(value: i64) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::IntValue(value)),
            })),
            span: None,
        }
    }

    fn float_literal(value: f64) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::FloatValue(value)),
            })),
            span: None,
        }
    }

    fn string_literal(value: &str) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::StringValue(value.to_string())),
            })),
            span: None,
        }
    }

    fn bool_literal(value: bool) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::BoolValue(value)),
            })),
            span: None,
        }
    }

    fn none_literal() -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Literal(ast::Literal {
                value: Some(ast::literal::Value::IsNone(true)),
            })),
            span: None,
        }
    }

    fn variable(name: &str) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Variable(ast::Variable {
                name: name.to_string(),
            })),
            span: None,
        }
    }

    fn binary_op(left: ast::Expr, op: ast::BinaryOperator, right: ast::Expr) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::BinaryOp(Box::new(ast::BinaryOp {
                left: Some(Box::new(left)),
                op: op as i32,
                right: Some(Box::new(right)),
            }))),
            span: None,
        }
    }

    fn unary_op(op: ast::UnaryOperator, operand: ast::Expr) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::UnaryOp(Box::new(ast::UnaryOp {
                op: op as i32,
                operand: Some(Box::new(operand)),
            }))),
            span: None,
        }
    }

    fn list_expr(elements: Vec<ast::Expr>) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::List(ast::ListExpr { elements })),
            span: None,
        }
    }

    fn dict_expr(entries: Vec<(ast::Expr, ast::Expr)>) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Dict(ast::DictExpr {
                entries: entries
                    .into_iter()
                    .map(|(k, v)| ast::DictEntry {
                        key: Some(k),
                        value: Some(v),
                    })
                    .collect(),
            })),
            span: None,
        }
    }

    fn index_access(object: ast::Expr, index: ast::Expr) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Index(Box::new(ast::IndexAccess {
                object: Some(Box::new(object)),
                index: Some(Box::new(index)),
            }))),
            span: None,
        }
    }

    fn dot_access(object: ast::Expr, attribute: &str) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::Dot(Box::new(ast::DotAccess {
                object: Some(Box::new(object)),
                attribute: attribute.to_string(),
            }))),
            span: None,
        }
    }

    fn function_call(name: &str, kwargs: Vec<(&str, ast::Expr)>) -> ast::Expr {
        ast::Expr {
            kind: Some(ast::expr::Kind::FunctionCall(ast::FunctionCall {
                name: name.to_string(),
                args: vec![],
                kwargs: kwargs
                    .into_iter()
                    .map(|(k, v)| ast::Kwarg {
                        name: k.to_string(),
                        value: Some(v),
                    })
                    .collect(),
            })),
            span: None,
        }
    }

    // ========================================================================
    // Literal Tests
    // ========================================================================

    #[test]
    fn test_eval_int_literal() {
        let scope = Scope::new();
        let expr = int_literal(42);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Number(42.into()));
    }

    #[test]
    fn test_eval_float_literal() {
        let scope = Scope::new();
        let expr = float_literal(3.14);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            JsonValue::Number(serde_json::Number::from_f64(3.14).unwrap())
        );
    }

    #[test]
    fn test_eval_string_literal() {
        let scope = Scope::new();
        let expr = string_literal("hello");
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::String("hello".to_string()));
    }

    #[test]
    fn test_eval_bool_literal() {
        let scope = Scope::new();
        let expr = bool_literal(true);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_none_literal() {
        let scope = Scope::new();
        let expr = none_literal();
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Null);
    }

    // ========================================================================
    // Variable Tests
    // ========================================================================

    #[test]
    fn test_eval_variable_found() {
        let mut scope = Scope::new();
        scope.insert("x".to_string(), JsonValue::Number(10.into()));
        let expr = variable("x");
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Number(10.into()));
    }

    #[test]
    fn test_eval_variable_not_found() {
        let scope = Scope::new();
        let expr = variable("x");
        let result = ExpressionEvaluator::evaluate(&expr, &scope);
        assert!(matches!(result, Err(EvaluationError::VariableNotFound(_))));
    }

    // ========================================================================
    // Arithmetic Operator Tests
    // ========================================================================

    #[test]
    fn test_eval_add_integers() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(10),
            ast::BinaryOperator::BinaryOpAdd,
            int_literal(5),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Number(15.into()));
    }

    #[test]
    fn test_eval_add_floats() {
        let scope = Scope::new();
        let expr = binary_op(
            float_literal(1.5),
            ast::BinaryOperator::BinaryOpAdd,
            float_literal(2.5),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            JsonValue::Number(serde_json::Number::from_f64(4.0).unwrap())
        );
    }

    #[test]
    fn test_eval_add_strings() {
        let scope = Scope::new();
        let expr = binary_op(
            string_literal("hello"),
            ast::BinaryOperator::BinaryOpAdd,
            string_literal(" world"),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::String("hello world".to_string()));
    }

    #[test]
    fn test_eval_add_arrays() {
        let scope = Scope::new();
        let expr = binary_op(
            list_expr(vec![int_literal(1), int_literal(2)]),
            ast::BinaryOperator::BinaryOpAdd,
            list_expr(vec![int_literal(3)]),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into()),
            ])
        );
    }

    #[test]
    fn test_eval_subtract() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(10),
            ast::BinaryOperator::BinaryOpSub,
            int_literal(3),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Number(7.into()));
    }

    #[test]
    fn test_eval_multiply() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(6),
            ast::BinaryOperator::BinaryOpMul,
            int_literal(7),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Number(42.into()));
    }

    #[test]
    fn test_eval_divide() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(10),
            ast::BinaryOperator::BinaryOpDiv,
            int_literal(4),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            JsonValue::Number(serde_json::Number::from_f64(2.5).unwrap())
        );
    }

    #[test]
    fn test_eval_divide_by_zero() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(10),
            ast::BinaryOperator::BinaryOpDiv,
            int_literal(0),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope);
        assert!(matches!(result, Err(EvaluationError::Evaluation(_))));
    }

    // ========================================================================
    // Comparison Operator Tests
    // ========================================================================

    #[test]
    fn test_eval_equal() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(5),
            ast::BinaryOperator::BinaryOpEq,
            int_literal(5),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_not_equal() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(5),
            ast::BinaryOperator::BinaryOpNe,
            int_literal(3),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_less_than() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(3),
            ast::BinaryOperator::BinaryOpLt,
            int_literal(5),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_less_than_or_equal() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(5),
            ast::BinaryOperator::BinaryOpLe,
            int_literal(5),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_greater_than() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(5),
            ast::BinaryOperator::BinaryOpGt,
            int_literal(3),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_greater_than_or_equal() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(5),
            ast::BinaryOperator::BinaryOpGe,
            int_literal(5),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_string_comparison() {
        let scope = Scope::new();
        let expr = binary_op(
            string_literal("apple"),
            ast::BinaryOperator::BinaryOpLt,
            string_literal("banana"),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    // ========================================================================
    // Boolean Operator Tests
    // ========================================================================

    #[test]
    fn test_eval_and_true_true() {
        let scope = Scope::new();
        let expr = binary_op(
            bool_literal(true),
            ast::BinaryOperator::BinaryOpAnd,
            bool_literal(true),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_and_true_false() {
        let scope = Scope::new();
        let expr = binary_op(
            bool_literal(true),
            ast::BinaryOperator::BinaryOpAnd,
            bool_literal(false),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(false));
    }

    #[test]
    fn test_eval_or_false_true() {
        let scope = Scope::new();
        let expr = binary_op(
            bool_literal(false),
            ast::BinaryOperator::BinaryOpOr,
            bool_literal(true),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_or_false_false() {
        let scope = Scope::new();
        let expr = binary_op(
            bool_literal(false),
            ast::BinaryOperator::BinaryOpOr,
            bool_literal(false),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(false));
    }

    #[test]
    fn test_eval_not_true() {
        let scope = Scope::new();
        let expr = unary_op(ast::UnaryOperator::UnaryOpNot, bool_literal(true));
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(false));
    }

    #[test]
    fn test_eval_not_false() {
        let scope = Scope::new();
        let expr = unary_op(ast::UnaryOperator::UnaryOpNot, bool_literal(false));
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_unary_negate() {
        let scope = Scope::new();
        let expr = unary_op(ast::UnaryOperator::UnaryOpNeg, int_literal(42));
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Number((-42).into()));
    }

    // ========================================================================
    // Membership Tests (in / not in)
    // ========================================================================

    #[test]
    fn test_eval_in_array() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(2),
            ast::BinaryOperator::BinaryOpIn,
            list_expr(vec![int_literal(1), int_literal(2), int_literal(3)]),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_not_in_array() {
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(5),
            ast::BinaryOperator::BinaryOpNotIn,
            list_expr(vec![int_literal(1), int_literal(2), int_literal(3)]),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_in_string() {
        let scope = Scope::new();
        let expr = binary_op(
            string_literal("ell"),
            ast::BinaryOperator::BinaryOpIn,
            string_literal("hello"),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_eval_in_dict() {
        let scope = Scope::new();
        let expr = binary_op(
            string_literal("key"),
            ast::BinaryOperator::BinaryOpIn,
            dict_expr(vec![(string_literal("key"), int_literal(42))]),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    // ========================================================================
    // Truthiness Tests
    // ========================================================================

    #[test]
    fn test_is_truthy_bool() {
        assert!(ExpressionEvaluator::is_truthy(&JsonValue::Bool(true)));
        assert!(!ExpressionEvaluator::is_truthy(&JsonValue::Bool(false)));
    }

    #[test]
    fn test_is_truthy_number() {
        assert!(ExpressionEvaluator::is_truthy(&JsonValue::Number(1.into())));
        assert!(!ExpressionEvaluator::is_truthy(&JsonValue::Number(0.into())));
    }

    #[test]
    fn test_is_truthy_string() {
        assert!(ExpressionEvaluator::is_truthy(&JsonValue::String(
            "hello".to_string()
        )));
        assert!(!ExpressionEvaluator::is_truthy(&JsonValue::String(
            "".to_string()
        )));
    }

    #[test]
    fn test_is_truthy_array() {
        assert!(ExpressionEvaluator::is_truthy(&JsonValue::Array(vec![
            JsonValue::Number(1.into())
        ])));
        assert!(!ExpressionEvaluator::is_truthy(&JsonValue::Array(vec![])));
    }

    #[test]
    fn test_is_truthy_null() {
        assert!(!ExpressionEvaluator::is_truthy(&JsonValue::Null));
    }

    // ========================================================================
    // Collection Tests
    // ========================================================================

    #[test]
    fn test_eval_list() {
        let scope = Scope::new();
        let expr = list_expr(vec![int_literal(1), int_literal(2), int_literal(3)]);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            JsonValue::Array(vec![
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into()),
            ])
        );
    }

    #[test]
    fn test_eval_dict() {
        let scope = Scope::new();
        let expr = dict_expr(vec![
            (string_literal("a"), int_literal(1)),
            (string_literal("b"), int_literal(2)),
        ]);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        let expected: serde_json::Map<String, JsonValue> = [
            ("a".to_string(), JsonValue::Number(1.into())),
            ("b".to_string(), JsonValue::Number(2.into())),
        ]
        .into_iter()
        .collect();
        assert_eq!(result, JsonValue::Object(expected));
    }

    #[test]
    fn test_eval_index_array() {
        let scope = Scope::new();
        let expr = index_access(
            list_expr(vec![int_literal(10), int_literal(20), int_literal(30)]),
            int_literal(1),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Number(20.into()));
    }

    #[test]
    fn test_eval_index_dict() {
        let scope = Scope::new();
        let expr = index_access(
            dict_expr(vec![(string_literal("key"), int_literal(42))]),
            string_literal("key"),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Number(42.into()));
    }

    #[test]
    fn test_eval_index_string() {
        let scope = Scope::new();
        let expr = index_access(string_literal("hello"), int_literal(1));
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::String("e".to_string()));
    }

    #[test]
    fn test_eval_dot_access() {
        let mut scope = Scope::new();
        let obj: serde_json::Map<String, JsonValue> =
            [("x".to_string(), JsonValue::Number(10.into()))]
                .into_iter()
                .collect();
        scope.insert("obj".to_string(), JsonValue::Object(obj));

        let expr = dot_access(variable("obj"), "x");
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Number(10.into()));
    }

    // ========================================================================
    // Built-in Function Tests
    // ========================================================================

    #[test]
    fn test_builtin_range() {
        let scope = Scope::new();
        let expr = function_call("range", vec![("stop", int_literal(5))]);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            JsonValue::Array(vec![
                JsonValue::Number(0.into()),
                JsonValue::Number(1.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into()),
                JsonValue::Number(4.into()),
            ])
        );
    }

    #[test]
    fn test_builtin_range_with_start() {
        let scope = Scope::new();
        let expr = function_call(
            "range",
            vec![("start", int_literal(2)), ("stop", int_literal(5))],
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            JsonValue::Array(vec![
                JsonValue::Number(2.into()),
                JsonValue::Number(3.into()),
                JsonValue::Number(4.into()),
            ])
        );
    }

    #[test]
    fn test_builtin_range_with_step() {
        let scope = Scope::new();
        let expr = function_call(
            "range",
            vec![
                ("start", int_literal(0)),
                ("stop", int_literal(10)),
                ("step", int_literal(2)),
            ],
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            JsonValue::Array(vec![
                JsonValue::Number(0.into()),
                JsonValue::Number(2.into()),
                JsonValue::Number(4.into()),
                JsonValue::Number(6.into()),
                JsonValue::Number(8.into()),
            ])
        );
    }

    #[test]
    fn test_builtin_len_array() {
        let scope = Scope::new();
        let expr = function_call(
            "len",
            vec![(
                "items",
                list_expr(vec![int_literal(1), int_literal(2), int_literal(3)]),
            )],
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Number(3.into()));
    }

    #[test]
    fn test_builtin_len_string() {
        let scope = Scope::new();
        let expr = function_call("len", vec![("items", string_literal("hello"))]);
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Number(5.into()));
    }

    #[test]
    fn test_builtin_enumerate() {
        let scope = Scope::new();
        let expr = function_call(
            "enumerate",
            vec![(
                "items",
                list_expr(vec![
                    string_literal("a"),
                    string_literal("b"),
                    string_literal("c"),
                ]),
            )],
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(
            result,
            JsonValue::Array(vec![
                JsonValue::Array(vec![
                    JsonValue::Number(0.into()),
                    JsonValue::String("a".to_string())
                ]),
                JsonValue::Array(vec![
                    JsonValue::Number(1.into()),
                    JsonValue::String("b".to_string())
                ]),
                JsonValue::Array(vec![
                    JsonValue::Number(2.into()),
                    JsonValue::String("c".to_string())
                ]),
            ])
        );
    }

    #[test]
    fn test_unknown_function() {
        let scope = Scope::new();
        let expr = function_call("unknown", vec![]);
        let result = ExpressionEvaluator::evaluate(&expr, &scope);
        assert!(matches!(result, Err(EvaluationError::FunctionNotFound(_))));
    }

    // ========================================================================
    // Complex Expression Tests
    // ========================================================================

    #[test]
    fn test_complex_expression() {
        // Test: (x + y) * 2 == 10 and z
        let mut scope = Scope::new();
        scope.insert("x".to_string(), JsonValue::Number(2.into()));
        scope.insert("y".to_string(), JsonValue::Number(3.into()));
        scope.insert("z".to_string(), JsonValue::Bool(true));

        let add = binary_op(variable("x"), ast::BinaryOperator::BinaryOpAdd, variable("y"));
        let mul = binary_op(add, ast::BinaryOperator::BinaryOpMul, int_literal(2));
        let eq = binary_op(mul, ast::BinaryOperator::BinaryOpEq, int_literal(10));
        let expr = binary_op(eq, ast::BinaryOperator::BinaryOpAnd, variable("z"));

        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_nested_collections() {
        let scope = Scope::new();
        // [[1, 2], [3, 4]][0][1] == 2
        let nested_list = list_expr(vec![
            list_expr(vec![int_literal(1), int_literal(2)]),
            list_expr(vec![int_literal(3), int_literal(4)]),
        ]);
        let first_access = index_access(nested_list, int_literal(0));
        let second_access = index_access(first_access, int_literal(1));

        let result = ExpressionEvaluator::evaluate(&second_access, &scope).unwrap();
        assert_eq!(result, JsonValue::Number(2.into()));
    }

    #[test]
    fn test_conditional_with_truthiness() {
        // Test truthy values in boolean context: 1 and "hello" -> true
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(1),
            ast::BinaryOperator::BinaryOpAnd,
            string_literal("hello"),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(true));
    }

    #[test]
    fn test_conditional_with_falsy() {
        // Test falsy values: 0 or "" -> false
        let scope = Scope::new();
        let expr = binary_op(
            int_literal(0),
            ast::BinaryOperator::BinaryOpOr,
            string_literal(""),
        );
        let result = ExpressionEvaluator::evaluate(&expr, &scope).unwrap();
        assert_eq!(result, JsonValue::Bool(false));
    }
}
