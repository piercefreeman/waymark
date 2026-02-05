//! CLI smoke check for core-python components.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use clap::Parser;
use serde_json::Value;
use uuid::Uuid;

use crate::backends::{MemoryBackend, QueuedInstance};
use crate::messages::ast as ir;
use crate::rappel_core::dag::convert_to_dag;
use crate::rappel_core::dag_viz::render_dag_image;
use crate::rappel_core::ir_format::format_program;
use crate::rappel_core::ir_parser::parse_program;
use crate::rappel_core::runloop::RunLoop;
use crate::rappel_core::runner::RunnerState;
use crate::workers::{PythonWorkerConfig, RemoteWorkerPool};

#[derive(Parser, Debug)]
#[command(name = "rappel-smoke", about = "Smoke check core-python components.")]
struct SmokeArgs {
    #[arg(long, default_value_t = 5)]
    base: i64,
}

pub(crate) fn literal_from_value(value: &Value) -> ir::Expr {
    match value {
        Value::Bool(value) => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::BoolValue(*value)),
            })),
            span: None,
        },
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IntValue(value)),
                    })),
                    span: None,
                }
            } else {
                ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::FloatValue(
                            number.as_f64().unwrap_or(0.0),
                        )),
                    })),
                    span: None,
                }
            }
        }
        Value::String(value) => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::StringValue(value.clone())),
            })),
            span: None,
        },
        Value::Array(items) => ir::Expr {
            kind: Some(ir::expr::Kind::List(ir::ListExpr {
                elements: items.iter().map(literal_from_value).collect(),
            })),
            span: None,
        },
        Value::Object(map) => {
            let entries = map
                .iter()
                .map(|(key, value)| ir::DictEntry {
                    key: Some(literal_from_value(&Value::String(key.clone()))),
                    value: Some(literal_from_value(value)),
                })
                .collect();
            ir::Expr {
                kind: Some(ir::expr::Kind::Dict(ir::DictExpr { entries })),
                span: None,
            }
        }
        Value::Null => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::IsNone(true)),
            })),
            span: None,
        },
    }
}

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

pub(crate) fn build_program() -> ir::Program {
    parse_program_source(SMOKE_SOURCE).expect("smoke program")
}

pub(crate) fn build_control_flow_program() -> Result<ir::Program, String> {
    parse_program_source(CONTROL_FLOW_SOURCE)
}

pub(crate) fn build_parallel_spread_program() -> Result<ir::Program, String> {
    parse_program_source(PARALLEL_SPREAD_SOURCE)
}

pub(crate) fn build_try_except_program() -> Result<ir::Program, String> {
    parse_program_source(TRY_EXCEPT_SOURCE)
}

pub(crate) fn build_while_loop_program() -> Result<ir::Program, String> {
    parse_program_source(WHILE_LOOP_SOURCE)
}

pub(crate) fn list_examples() -> Vec<&'static str> {
    let mut names = vec![
        "control_flow",
        "parallel_spread",
        "try_except",
        "while_loop",
    ];
    names.sort();
    names
}

#[derive(Clone)]
struct SmokeCase {
    name: String,
    program: ir::Program,
    inputs: HashMap<String, Value>,
}

fn slugify(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

async fn run_program_smoke(case: &SmokeCase, worker_pool: RemoteWorkerPool) -> Result<()> {
    println!("\nIR program ({})", case.name);
    println!("{}", format_program(&case.program));
    println!("IR inputs ({}): {:?}", case.name, case.inputs);
    let dag = convert_to_dag(&case.program).map_err(|err| anyhow!(err.to_string()))?;
    let slug = slugify(&case.name);
    let output_path = PathBuf::from(format!("dag_smoke_{slug}.png"));
    let output_path =
        render_dag_image(&dag, &output_path).map_err(|err| anyhow!(err.to_string()))?;
    println!(
        "DAG image ({}) written to {}",
        case.name,
        output_path.display()
    );

    let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    for (name, value) in &case.inputs {
        let expr = literal_from_value(value);
        let label = format!("input {name} = {value}");
        let _ = state
            .record_assignment(vec![name.clone()], &expr, None, Some(label))
            .expect("record assignment");
    }

    let entry_node = dag
        .entry_node
        .clone()
        .ok_or_else(|| anyhow!("DAG entry node not found"))?;
    let entry_exec = state
        .queue_template_node(&entry_node, None)
        .map_err(|err| anyhow!(err.0))?;

    let mut runloop = RunLoop::new(worker_pool, backend, 25, None, 0.05, 0.1);
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        dag: dag.clone(),
        entry_node: entry_exec.node_id,
        state: Some(state),
        action_results: HashMap::new(),
        instance_id: Uuid::new_v4(),
    });
    let result = runloop
        .run()
        .await
        .map_err(|err| anyhow!(err.to_string()))?;
    let executed = result
        .completed_actions
        .values()
        .next()
        .cloned()
        .unwrap_or_default();
    let labels: Vec<String> = executed.iter().map(|node| node.label.clone()).collect();
    println!("Runner executor actions: {labels:?}");
    Ok(())
}

async fn run_smoke(base: i64) -> i32 {
    let config = PythonWorkerConfig::new().with_user_module("tests.fixtures.test_actions");
    let worker_pool = match RemoteWorkerPool::new_with_config(config, 2, None, None, 10).await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Failed to start python worker pool: {err}");
            return 1;
        }
    };

    let mut cases = Vec::new();
    cases.push(SmokeCase {
        name: "smoke".to_string(),
        program: build_program(),
        inputs: HashMap::from([("base".to_string(), Value::Number(base.into()))]),
    });
    let examples = vec![
        ("control_flow", build_control_flow_program()),
        ("parallel_spread", build_parallel_spread_program()),
        ("try_except", build_try_except_program()),
        ("while_loop", build_while_loop_program()),
    ];
    for (name, program) in examples {
        let program = match program {
            Ok(value) => value,
            Err(err) => {
                println!("Failed to build {name} program: {err}");
                continue;
            }
        };
        let inputs = match name {
            "control_flow" => HashMap::from([("base".to_string(), Value::Number(2.into()))]),
            "parallel_spread" => HashMap::from([("base".to_string(), Value::Number(3.into()))]),
            "try_except" => HashMap::from([(
                "values".to_string(),
                Value::Array(vec![1.into(), 2.into(), 3.into()]),
            )]),
            "while_loop" => HashMap::from([("limit".to_string(), Value::Number(6.into()))]),
            _ => HashMap::new(),
        };
        cases.push(SmokeCase {
            name: name.to_string(),
            program,
            inputs,
        });
    }

    let mut failures = 0;
    for case in &cases {
        if let Err(err) = run_program_smoke(case, worker_pool.clone()).await {
            failures += 1;
            println!("Smoke case '{}' failed: {}", case.name, err);
        }
    }

    if let Err(err) = worker_pool.shutdown().await {
        println!("Failed to shut down worker pool: {err}");
    }

    println!("Examples available: {:?}", list_examples());
    if failures > 0 { 1 } else { 0 }
}

pub fn main() {
    let args = SmokeArgs::parse();
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let code = runtime.block_on(run_smoke(args.base));
    std::process::exit(code);
}
