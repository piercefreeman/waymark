//! Shared utils for smoke checks.

use waymark_ir_parser::parse_program;
use waymark_proto::ast as ir;

fn parse_program_source(source: &str) -> Result<ir::Program, String> {
    parse_program(source.trim()).map_err(|err| err.to_string())
}

const SMOKE_SOURCE: &str = r#"
fn main(input: [base], output: [final]):
    values = [1, 2, 3]
    doubles = spread values:item -> @tests.fixtures.test_actions.double(value=item)
    a, b = parallel:
        @tests.fixtures.test_actions.double(value=base)
        @tests.fixtures.test_actions.double(value=base + 1)
    pair_sum = a + b
    total = @tests.fixtures.test_actions.sum(values=doubles)
    final = pair_sum + total
    return final
"#;

const CONTROL_FLOW_SOURCE: &str = r#"
fn main(input: [base], output: [summary]):
    payload = {"items": [1, 2, 3, 4], "limit": base}
    items = payload.items
    first_item = items[0]
    limit = payload.limit
    results = []
    for idx, item in enumerate(items):
        if item % 2 == 0:
            doubled = @tests.fixtures.test_actions.double(value=item)
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

const PARALLEL_SPREAD_SOURCE: &str = r#"
fn main(input: [base], output: [final]):
    values = range(1, base + 1)
    doubles = spread values:item -> @tests.fixtures.test_actions.double(value=item)
    a, b = parallel:
        @tests.fixtures.test_actions.double(value=base)
        @tests.fixtures.test_actions.double(value=base + 1)
    pair_sum = a + b
    total = @tests.fixtures.test_actions.sum(values=doubles)
    final = pair_sum + total
    return final
"#;

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

pub fn build_program() -> ir::Program {
    parse_program_source(SMOKE_SOURCE).expect("smoke program")
}

pub fn build_control_flow_program() -> Result<ir::Program, String> {
    parse_program_source(CONTROL_FLOW_SOURCE)
}

pub fn build_parallel_spread_program() -> Result<ir::Program, String> {
    parse_program_source(PARALLEL_SPREAD_SOURCE)
}

pub fn build_try_except_program() -> Result<ir::Program, String> {
    parse_program_source(TRY_EXCEPT_SOURCE)
}

pub fn build_while_loop_program() -> Result<ir::Program, String> {
    parse_program_source(WHILE_LOOP_SOURCE)
}
