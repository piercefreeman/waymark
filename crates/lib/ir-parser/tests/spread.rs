//! Tests for spread-expression parsing.

use waymark_proto::ast as ir;

/// Extracts the spread expression assigned in the only statement of the
/// program's only function.
fn parse_single_spread_expr(source: &str) -> ir::SpreadExpr {
    let program = waymark_ir_parser::parse_program(source.trim()).expect("program should parse");
    let function = program.functions.first().expect("one function");
    let body = function.body.as_ref().expect("function body");
    let statement = body.statements.first().expect("one statement");
    let Some(ir::statement::Kind::Assignment(assignment)) = &statement.kind else {
        panic!("expected an assignment statement");
    };
    let value = assignment.value.as_ref().expect("assignment value");
    let Some(ir::expr::Kind::SpreadExpr(spread)) = &value.kind else {
        panic!("expected a spread expression value");
    };
    (**spread).clone()
}

const ACTION_SPREAD_SOURCE: &str = r#"
fn main(input: [items], output: []):
    results = spread items:item -> @notify(value=item)
"#;

const FUNCTION_SPREAD_SOURCE: &str = r#"
fn main(input: [items], output: []):
    results = spread items:item -> helper(item)
"#;

#[test]
fn parses_spread_expressions_over_action_calls() {
    let spread = parse_single_spread_expr(ACTION_SPREAD_SOURCE);

    assert_eq!(spread.loop_var, "item");
    let call = spread.call.expect("spread call");
    let Some(ir::call::Kind::Action(action)) = call.kind else {
        panic!("expected an action call");
    };
    assert_eq!(action.action_name, "notify");
}

#[test]
fn parses_spread_expressions_over_function_calls() {
    let spread = parse_single_spread_expr(FUNCTION_SPREAD_SOURCE);

    assert_eq!(spread.loop_var, "item");
    let call = spread.call.expect("spread call");
    let Some(ir::call::Kind::Function(function)) = call.kind else {
        panic!("expected a function call");
    };
    assert_eq!(function.name, "helper");
    assert_eq!(function.args.len(), 1);
}
