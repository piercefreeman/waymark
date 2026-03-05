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
