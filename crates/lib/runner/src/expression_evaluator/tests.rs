use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use uuid::Uuid;

use super::*;
use waymark_dag::{DAG, DAGEdge};
use waymark_ir_parser::IRParser;
use waymark_proto::ast as ir;
use waymark_runner_state::{
    ActionCallSpec, ActionResultValue, BinaryOpValue, FunctionCallValue, LiteralValue, RunnerState,
    VariableValue, value_visitor::ValueExpr,
};

fn parse_expr(source: &str) -> ir::Expr {
    IRParser::new("    ")
        .parse_expr(source)
        .expect("parse expression")
}

fn literal_int(value: i64) -> ValueExpr {
    ValueExpr::Literal(LiteralValue {
        value: Value::Number(value.into()),
    })
}

fn empty_executor() -> RunnerExecutor {
    let dag = Arc::new(DAG::default());
    let state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
    RunnerExecutor::new(dag, state, HashMap::new(), None)
}

fn executor_with_assignment(name: &str, value: ValueExpr) -> RunnerExecutor {
    let dag = Arc::new(DAG::default());
    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
    state
        .record_assignment_value(
            vec![name.to_string()],
            value,
            None,
            Some("test assignment".to_string()),
        )
        .expect("record assignment");
    RunnerExecutor::new(dag, state, HashMap::new(), None)
}

#[test]
fn test_expr_to_value_happy_path() {
    let expr = parse_expr("x + 2");
    let value = RunnerExecutor::expr_to_value(&expr).expect("convert expression");
    match value {
        ValueExpr::BinaryOp(binary) => {
            assert!(matches!(*binary.left, ValueExpr::Variable(_)));
            assert!(matches!(*binary.right, ValueExpr::Literal(_)));
        }
        other => panic!("expected binary op, got {other:?}"),
    }
}

#[test]
fn test_evaluate_guard_happy_path() {
    let executor = executor_with_assignment("x", literal_int(2));
    let guard = parse_expr("x > 1");
    let result = executor
        .evaluate_guard(Some(&guard))
        .expect("evaluate guard");
    assert!(result);
}

#[test]
fn test_resolve_action_kwargs_happy_path() {
    let executor = executor_with_assignment("x", literal_int(10));
    let action = ActionCallSpec {
        action_name: "double".to_string(),
        module_name: Some("tests".to_string()),
        kwargs: HashMap::from([(
            "value".to_string(),
            ValueExpr::Variable(VariableValue {
                name: "x".to_string(),
            }),
        )]),
    };
    let resolved = executor
        .resolve_action_kwargs(Uuid::new_v4(), &action)
        .expect("resolve kwargs");
    assert_eq!(resolved.get("value"), Some(&Value::Number(10.into())));
}

#[test]
fn test_resolve_action_kwargs_uses_data_flow_for_self_referential_targets() {
    let dag = Arc::new(DAG::default());
    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
    state
        .record_assignment_value(
            vec!["current".to_string()],
            literal_int(0),
            None,
            Some("current = 0".to_string()),
        )
        .expect("record current");
    let action_result = state
        .queue_action(
            "increment",
            Some(vec!["current".to_string()]),
            Some(HashMap::from([(
                "value".to_string(),
                ValueExpr::Variable(VariableValue {
                    name: "current".to_string(),
                }),
            )])),
            None,
            None,
        )
        .expect("queue increment");
    let action_node = state
        .nodes
        .get(&action_result.node_id)
        .expect("action node")
        .clone();
    let action_spec = action_node.action.expect("action spec");

    let executor = RunnerExecutor::new(dag, state, HashMap::new(), None);
    let resolved = executor
        .resolve_action_kwargs(action_result.node_id, &action_spec)
        .expect("resolve kwargs");
    assert_eq!(resolved.get("value"), Some(&Value::Number(0.into())));
}

#[test]
fn test_evaluate_value_expr_happy_path() {
    let executor = executor_with_assignment("x", literal_int(3));
    let expr = ValueExpr::BinaryOp(waymark_runner_state::BinaryOpValue {
        left: Box::new(ValueExpr::Variable(VariableValue {
            name: "x".to_string(),
        })),
        op: ir::BinaryOperator::BinaryOpAdd as i32,
        right: Box::new(literal_int(1)),
    });
    let value = executor
        .evaluate_value_expr(&expr)
        .expect("evaluate value expression");
    assert_eq!(value, Value::Number(4.into()));
}

#[test]
fn test_evaluate_variable_happy_path() {
    let executor = executor_with_assignment("value", literal_int(5));
    let stack = Rc::new(RefCell::new(HashSet::new()));
    let value = executor
        .evaluate_variable_with_context(None, "value", stack)
        .expect("evaluate variable");
    assert_eq!(value, Value::Number(5.into()));
}

#[test]
fn test_evaluate_assignment_happy_path() {
    let executor = executor_with_assignment("value", literal_int(9));
    let node_id = executor
        .state()
        .latest_assignment("value")
        .expect("latest assignment");
    let stack = Rc::new(RefCell::new(HashSet::new()));
    let value = executor
        .evaluate_assignment(node_id, "value", stack)
        .expect("evaluate assignment");
    assert_eq!(value, Value::Number(9.into()));
}

#[test]
fn test_evaluate_assignment_uses_data_flow_for_self_referential_updates() {
    let dag = Arc::new(DAG::default());
    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
    state
        .record_assignment_value(
            vec!["count".to_string()],
            literal_int(0),
            None,
            Some("count = 0".to_string()),
        )
        .expect("record initial count");
    state
        .record_assignment_value(
            vec!["count".to_string()],
            ValueExpr::BinaryOp(BinaryOpValue {
                left: Box::new(ValueExpr::Variable(VariableValue {
                    name: "count".to_string(),
                })),
                op: ir::BinaryOperator::BinaryOpAdd as i32,
                right: Box::new(literal_int(1)),
            }),
            None,
            Some("count = count + 1".to_string()),
        )
        .expect("record updated count");

    let executor = RunnerExecutor::new(dag, state, HashMap::new(), None);
    let node_id = executor
        .state()
        .latest_assignment("count")
        .expect("latest assignment");
    let stack = Rc::new(RefCell::new(HashSet::new()));
    let value = executor
        .evaluate_assignment(node_id, "count", stack)
        .expect("evaluate self-referential assignment");
    assert_eq!(value, Value::Number(1.into()));
}

#[test]
fn test_evaluate_assignment_handles_deep_dependency_chain() {
    let dag = Arc::new(DAG::default());
    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);

    state
        .record_assignment_value(
            vec!["count".to_string()],
            literal_int(0),
            None,
            Some("count = 0".to_string()),
        )
        .expect("record initial count");

    for idx in 0..5000 {
        state
            .record_assignment_value(
                vec!["count".to_string()],
                ValueExpr::BinaryOp(BinaryOpValue {
                    left: Box::new(ValueExpr::Variable(VariableValue {
                        name: "count".to_string(),
                    })),
                    op: ir::BinaryOperator::BinaryOpAdd as i32,
                    right: Box::new(literal_int(1)),
                }),
                None,
                Some(format!("count = count + 1 #{idx}")),
            )
            .expect("record updated count");
    }

    let executor = RunnerExecutor::new(dag, state, HashMap::new(), None);
    let node_id = executor
        .state()
        .latest_assignment("count")
        .expect("latest assignment");
    let stack = Rc::new(RefCell::new(HashSet::new()));
    let value = executor
        .evaluate_assignment(node_id, "count", stack)
        .expect("evaluate deep dependency chain");

    assert_eq!(value, Value::Number(5000.into()));
}

#[test]
fn test_resolve_action_result_happy_path() {
    let mut executor = empty_executor();
    let action_id = Uuid::new_v4();
    executor.set_action_result(
        action_id,
        Value::Array(vec![Value::Number(7.into()), Value::Number(8.into())]),
    );
    let result = executor
        .resolve_action_result(&ActionResultValue {
            node_id: action_id,
            action_name: "fetch".to_string(),
            iteration_index: None,
            result_index: Some(1),
        })
        .expect("resolve action result");
    assert_eq!(result, Value::Number(8.into()));
}

#[test]
fn test_evaluate_function_call_happy_path() {
    let executor = empty_executor();
    let value = executor
        .evaluate_function_call(
            &FunctionCallValue {
                name: "len".to_string(),
                args: Vec::new(),
                kwargs: HashMap::new(),
                global_function: Some(ir::GlobalFunction::Len as i32),
            },
            vec![Value::Array(vec![Value::Null, Value::Null])],
            HashMap::new(),
        )
        .expect("evaluate function call");
    assert_eq!(value, Value::Number(2.into()));
}

#[test]
fn test_evaluate_global_function_happy_path() {
    let executor = empty_executor();
    let value = executor
        .evaluate_global_function(
            ir::GlobalFunction::Range as i32,
            vec![Value::Number(1.into()), Value::Number(4.into())],
            HashMap::new(),
        )
        .expect("evaluate global function");
    assert_eq!(
        value,
        Value::Array(vec![
            Value::Number(1.into()),
            Value::Number(2.into()),
            Value::Number(3.into())
        ])
    );
}

#[test]
fn test_apply_binary_happy_path() {
    let value = RunnerExecutor::apply_binary(
        ir::BinaryOperator::BinaryOpAdd as i32,
        Value::Number(2.into()),
        Value::Number(3.into()),
    )
    .expect("apply binary");
    assert_eq!(value, Value::Number(5.into()));
}

#[test]
fn test_apply_unary_happy_path() {
    let value =
        RunnerExecutor::apply_unary(ir::UnaryOperator::UnaryOpNot as i32, Value::Bool(true))
            .expect("apply unary");
    assert_eq!(value, Value::Bool(false));
}

#[test]
fn test_exception_matches_happy_path() {
    let executor = empty_executor();
    let edge = DAGEdge::state_machine_with_exception("a", "b", vec!["ValueError".to_string()]);
    let exception = serde_json::json!({
        "type": "ValueError",
        "message": "boom",
    });
    assert!(executor.exception_matches(&edge, &exception));
}

#[test]
fn test_executor_error_happy_path() {
    let error = executor_error("hello");
    assert_eq!(error.0, "hello");
}

#[test]
fn test_int_value_happy_path() {
    let value = Value::Number(7_u64.into());
    assert_eq!(int_value(&value), Some(7));
}

#[test]
fn test_numeric_op_happy_path() {
    let value = numeric_op(
        Value::Number(10.into()),
        Value::Number(3.into()),
        |a, b| a + b,
        true,
        executor_error,
    )
    .expect("numeric op");
    assert_eq!(value, Value::Number(13.into()));
}

#[test]
fn test_add_values_happy_path() {
    let value = add_values(
        Value::String("hello ".to_string()),
        Value::String("world".to_string()),
        executor_error,
    )
    .expect("add values");
    assert_eq!(value, Value::String("hello world".to_string()));
}

#[test]
fn test_compare_values_happy_path() {
    let value = compare_values(
        Value::Number(3.into()),
        Value::Number(5.into()),
        |a, b| a < b,
        executor_error,
    )
    .expect("compare values");
    assert_eq!(value, Value::Bool(true));
}

#[test]
fn test_value_in_happy_path() {
    let container = Value::Array(vec![Value::Number(1.into()), Value::Number(2.into())]);
    assert!(value_in(&Value::Number(2.into()), &container));
}

#[test]
fn test_is_truthy_happy_path() {
    assert!(is_truthy(&Value::String("non-empty".to_string())));
}

#[test]
fn test_is_exception_value_happy_path() {
    let value = serde_json::json!({
        "type": "RuntimeError",
        "message": "bad",
    });
    assert!(is_exception_value(&value));
}

#[test]
fn test_len_of_value_happy_path() {
    let value = Value::Array(vec![Value::Null, Value::Null, Value::Null]);
    let len = len_of_value(&value, executor_error).expect("length");
    assert_eq!(len.as_i64(), Some(3));
}

#[test]
fn test_range_from_args_happy_path() {
    let values = range_from_args(&[
        Value::Number(0.into()),
        Value::Number(5.into()),
        Value::Number(2.into()),
    ]);
    assert_eq!(
        values,
        vec![
            Value::Number(0.into()),
            Value::Number(2.into()),
            Value::Number(4.into())
        ]
    );
}
