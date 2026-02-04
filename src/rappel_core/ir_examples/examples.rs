//! Collection of complex IR example programs.

use crate::messages::ast as ir;
use crate::rappel_core::ir_parser::IRParser;

fn parse_source(source: &str) -> Result<ir::Program, String> {
    let mut parser = IRParser::new("    ");
    parser
        .parse_program(source.trim())
        .map_err(|err| err.to_string())
}

const CONTROL_FLOW_SOURCE: &str = r#"
fn main(input: [base], output: [summary]):
    payload = {"items": [1, 2, 3, 4], "limit": base}
    items = payload.items
    first_item = items[0]
    limit = payload.limit
    results = []
    for idx, item in enumerate(items):
        if item % 2 == 0:
            doubled = @double(value=item)
            results = results + [doubled]
            continue
        elif item > limit:
            break
        else:
            results = results + [item]
    count = len(results)
    summary = {"count": count, "first": first_item, "results": results}
    return summary
"#;

pub fn build_control_flow_program() -> Result<ir::Program, String> {
    parse_source(CONTROL_FLOW_SOURCE)
}

const PARALLEL_SPREAD_SOURCE: &str = r#"
fn main(input: [base], output: [final]):
    values = range(1, base + 1)
    doubles = spread values:item -> @double(value=item)
    a, b = parallel:
        @double(value=base)
        @double(value=base + 1)
    pair_sum = a + b
    total = @sum(values=doubles)
    final = pair_sum + total
    return final
"#;

pub fn build_parallel_spread_program() -> Result<ir::Program, String> {
    parse_source(PARALLEL_SPREAD_SOURCE)
}

const TRY_EXCEPT_SOURCE: &str = r#"
fn risky(input: [numerator, denominator], output: [result]):
    try:
        result = numerator / denominator
    except ZeroDivisionError as err:
        result = 0
    return result

fn main(input: [values], output: [total]):
    total = 0
    for item in values:
        denom = item - 2
        part = risky(numerator=item, denominator=denom)
        total = total + part
    return total
"#;

pub fn build_try_except_program() -> Result<ir::Program, String> {
    parse_source(TRY_EXCEPT_SOURCE)
}

const WHILE_LOOP_SOURCE: &str = r#"
fn main(input: [limit], output: [accum]):
    index = 0
    accum = []
    while index < limit:
        accum = accum + [index]
        if index == 2:
            index = index + 1
            continue
        if index == 4:
            break
        index = index + 1
    return accum
"#;

pub fn build_while_loop_program() -> Result<ir::Program, String> {
    parse_source(WHILE_LOOP_SOURCE)
}

pub fn list_examples() -> Vec<&'static str> {
    let mut names = vec![
        "control_flow",
        "parallel_spread",
        "try_except",
        "while_loop",
    ];
    names.sort();
    names
}

pub fn get_example(name: &str) -> Result<ir::Program, String> {
    match name {
        "control_flow" => build_control_flow_program(),
        "parallel_spread" => build_parallel_spread_program(),
        "try_except" => build_try_except_program(),
        "while_loop" => build_while_loop_program(),
        _ => Err(format!("unknown example: {name}")),
    }
}
