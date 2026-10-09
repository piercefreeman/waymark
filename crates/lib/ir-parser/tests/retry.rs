//! Tests for retry and timeout policy bracket parsing.

use waymark_proto::ast as ir;

/// Parses a program whose only statement is a bare action call and returns
/// that call's policy brackets.
fn parse_single_action_policies(source: &str) -> Vec<ir::PolicyBracket> {
    let program = waymark_ir_parser::parse_program(source.trim()).expect("program should parse");
    let function = program.functions.first().expect("one function");
    let body = function.body.as_ref().expect("function body");
    let statement = body.statements.first().expect("one statement");
    let Some(ir::statement::Kind::ActionCall(action)) = &statement.kind else {
        panic!("expected an action call statement");
    };
    action.policies.clone()
}

/// The classes a retry bracket lists.
fn retry_exception_types(bracket: &ir::PolicyBracket) -> Vec<String> {
    let Some(ir::policy_bracket::Kind::Retry(retry)) = &bracket.kind else {
        panic!("expected a retry bracket");
    };
    retry.exception_types.clone()
}

const LISTED_HEADER_SOURCE: &str = r#"
fn main(input: [], output: []):
    @notify()[ValueError, KeyError -> retry: 2]
"#;

const EMPTY_HEADER_SOURCE: &str = r#"
fn main(input: [], output: []):
    @notify()[() -> retry: 2]
"#;

const NO_HEADER_SOURCE: &str = r#"
fn main(input: [], output: []):
    @notify()[retry: 2]
"#;

const TIMEOUT_WITH_HEADER_SOURCE: &str = r#"
fn main(input: [], output: []):
    @notify()[() -> timeout: 30 s]
"#;

const FRACTIONAL_DURATIONS_SOURCE: &str = r#"
fn main(input: [], output: []):
    @notify()[Exception -> retry: 2, backoff: 0.5 s][timeout: 1.5 s]
"#;

const OVERFLOWING_DURATION_SOURCE: &str = r#"
fn main(input: [], output: []):
    @notify()[timeout: 1e305 h]
"#;

#[test]
fn parses_the_classes_a_retry_header_lists() {
    let policies = parse_single_action_policies(LISTED_HEADER_SOURCE);

    assert_eq!(policies.len(), 1);
    assert_eq!(
        retry_exception_types(&policies[0]),
        ["ValueError", "KeyError"]
    );
}

#[test]
fn parses_an_empty_retry_header_as_no_classes() {
    let policies = parse_single_action_policies(EMPTY_HEADER_SOURCE);

    assert_eq!(policies.len(), 1);
    assert!(retry_exception_types(&policies[0]).is_empty());
}

#[test]
fn rejects_a_retry_bracket_without_a_header() {
    let error = waymark_ir_parser::parse_program(NO_HEADER_SOURCE.trim())
        .expect_err("a headerless retry bracket should not parse");

    assert!(
        error.to_string().contains("exception header"),
        "unexpected error: {error}"
    );
}

#[test]
fn parses_fractional_durations() {
    let policies = parse_single_action_policies(FRACTIONAL_DURATIONS_SOURCE);

    assert_eq!(policies.len(), 2);
    let Some(ir::policy_bracket::Kind::Retry(retry)) = &policies[0].kind else {
        panic!("expected a retry bracket");
    };
    assert_eq!(
        retry.backoff.as_ref().map(|backoff| backoff.seconds),
        Some(0.5)
    );
    let Some(ir::policy_bracket::Kind::Timeout(timeout)) = &policies[1].kind else {
        panic!("expected a timeout bracket");
    };
    assert_eq!(
        timeout.timeout.as_ref().map(|timeout| timeout.seconds),
        Some(1.5)
    );
}

#[test]
fn rejects_a_duration_whose_unit_multiply_overflows() {
    let error = waymark_ir_parser::parse_program(OVERFLOWING_DURATION_SOURCE.trim())
        .expect_err("a finite value that overflows in hours should not parse");

    assert!(
        error.to_string().contains("finite duration"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_a_timeout_bracket_with_a_header() {
    let error = waymark_ir_parser::parse_program(TIMEOUT_WITH_HEADER_SOURCE.trim())
        .expect_err("a timeout bracket with a header should not parse");

    assert!(
        error.to_string().contains("Timeout policy"),
        "unexpected error: {error}"
    );
}
