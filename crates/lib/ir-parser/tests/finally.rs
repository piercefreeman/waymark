use waymark_ir_parser::parse_program;

#[test]
fn parses_finally_block() {
    let program = parse_program(
        "fn main(input: [], output: []):\n    try:\n        value = 1\n    finally:\n        value = 2\n",
    )
    .expect("finally should parse");
    let statement = &program.functions[0]
        .body
        .as_ref()
        .expect("function body should exist")
        .statements[0];
    let Some(waymark_proto::ast::statement::Kind::TryExcept(try_except)) = &statement.kind else {
        panic!("expected try statement");
    };

    assert!(try_except.handlers.is_empty());
    assert_eq!(
        try_except
            .finally_block
            .as_ref()
            .expect("finally block should exist")
            .statements
            .len(),
        1
    );
}
