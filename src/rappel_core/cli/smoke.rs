//! CLI smoke check for core-python components.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use clap::Parser;
use serde_json::Value;
use uuid::Uuid;

use crate::messages::ast as ir;
use crate::rappel_core::backends::{MemoryBackend, QueuedInstance};
use crate::rappel_core::dag::{DAG, convert_to_dag};
use crate::rappel_core::dag_viz::render_dag_image;
use crate::rappel_core::ir_examples::{binary, list_expr, variable};
use crate::rappel_core::ir_examples::{
    build_control_flow_program, build_parallel_spread_program, build_try_except_program,
    build_while_loop_program, list_examples,
};
use crate::rappel_core::ir_executor::{ExecutionError, StatementExecutor};
use crate::rappel_core::ir_format::format_program;
use crate::rappel_core::runloop::RunLoop;
use crate::rappel_core::runner::state::LiteralValue;
use crate::rappel_core::runner::value_visitor::ValueExpr;
use crate::rappel_core::runner::{RunnerState, replay_variables};
use crate::rappel_core::workers::{ActionCallable, InlineWorkerPool, WorkerPoolError};

#[derive(Parser, Debug)]
#[command(name = "rappel-smoke", about = "Smoke check core-python components.")]
struct SmokeArgs {
    #[arg(long, default_value_t = 5)]
    base: i64,
}

fn literal_int(value: i64) -> ir::Expr {
    ir::Expr {
        kind: Some(ir::expr::Kind::Literal(ir::Literal {
            value: Some(ir::literal::Value::IntValue(value)),
        })),
        span: None,
    }
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

pub(crate) fn build_program() -> ir::Program {
    let values_expr = list_expr(vec![literal_int(1), literal_int(2), literal_int(3)]);
    let doubles_expr = ir::Expr {
        kind: Some(ir::expr::Kind::SpreadExpr(Box::new(ir::SpreadExpr {
            collection: Some(Box::new(variable("values"))),
            loop_var: "item".to_string(),
            action: Some(ir::ActionCall {
                action_name: "double".to_string(),
                kwargs: vec![ir::Kwarg {
                    name: "value".to_string(),
                    value: Some(variable("item")),
                }],
                policies: Vec::new(),
                module_name: None,
            }),
        }))),
        span: None,
    };
    let parallel_expr = ir::Expr {
        kind: Some(ir::expr::Kind::ParallelExpr(ir::ParallelExpr {
            calls: vec![
                ir::Call {
                    kind: Some(ir::call::Kind::Action(ir::ActionCall {
                        action_name: "double".to_string(),
                        kwargs: vec![ir::Kwarg {
                            name: "value".to_string(),
                            value: Some(variable("base")),
                        }],
                        policies: Vec::new(),
                        module_name: None,
                    })),
                },
                ir::Call {
                    kind: Some(ir::call::Kind::Action(ir::ActionCall {
                        action_name: "double".to_string(),
                        kwargs: vec![ir::Kwarg {
                            name: "value".to_string(),
                            value: Some(binary(
                                variable("base"),
                                ir::BinaryOperator::BinaryOpAdd,
                                literal_int(1),
                            )),
                        }],
                        policies: Vec::new(),
                        module_name: None,
                    })),
                },
            ],
        })),
        span: None,
    };

    let statements = vec![
        ir::Statement {
            kind: Some(ir::statement::Kind::Assignment(ir::Assignment {
                targets: vec!["values".to_string()],
                value: Some(values_expr),
            })),
            span: None,
        },
        ir::Statement {
            kind: Some(ir::statement::Kind::Assignment(ir::Assignment {
                targets: vec!["doubles".to_string()],
                value: Some(doubles_expr),
            })),
            span: None,
        },
        ir::Statement {
            kind: Some(ir::statement::Kind::Assignment(ir::Assignment {
                targets: vec!["a".to_string(), "b".to_string()],
                value: Some(parallel_expr),
            })),
            span: None,
        },
        ir::Statement {
            kind: Some(ir::statement::Kind::Assignment(ir::Assignment {
                targets: vec!["pair_sum".to_string()],
                value: Some(binary(
                    variable("a"),
                    ir::BinaryOperator::BinaryOpAdd,
                    variable("b"),
                )),
            })),
            span: None,
        },
        ir::Statement {
            kind: Some(ir::statement::Kind::Assignment(ir::Assignment {
                targets: vec!["total".to_string()],
                value: Some(ir::Expr {
                    kind: Some(ir::expr::Kind::ActionCall(ir::ActionCall {
                        action_name: "sum".to_string(),
                        kwargs: vec![ir::Kwarg {
                            name: "values".to_string(),
                            value: Some(variable("doubles")),
                        }],
                        policies: Vec::new(),
                        module_name: None,
                    })),
                    span: None,
                }),
            })),
            span: None,
        },
        ir::Statement {
            kind: Some(ir::statement::Kind::Assignment(ir::Assignment {
                targets: vec!["final".to_string()],
                value: Some(binary(
                    variable("pair_sum"),
                    ir::BinaryOperator::BinaryOpAdd,
                    variable("total"),
                )),
            })),
            span: None,
        },
        ir::Statement {
            kind: Some(ir::statement::Kind::ReturnStmt(ir::ReturnStmt {
                value: Some(variable("final")),
            })),
            span: None,
        },
    ];

    let main_block = ir::Block {
        statements,
        span: None,
    };
    let main_fn = ir::FunctionDef {
        name: "main".to_string(),
        io: Some(ir::IoDecl {
            inputs: vec!["base".to_string()],
            outputs: vec!["final".to_string()],
            span: None,
        }),
        body: Some(main_block),
        span: None,
    };
    ir::Program {
        functions: vec![main_fn],
    }
}

async fn action_double(kwargs: HashMap<String, Value>) -> Result<Value, WorkerPoolError> {
    let value = kwargs
        .get("value")
        .and_then(|value| value.as_i64())
        .ok_or_else(|| WorkerPoolError::new("ActionError", "double expects integer value"))?;
    Ok(Value::Number((value * 2).into()))
}

async fn action_sum(kwargs: HashMap<String, Value>) -> Result<Value, WorkerPoolError> {
    let values = kwargs
        .get("values")
        .and_then(|value| value.as_array())
        .ok_or_else(|| WorkerPoolError::new("ActionError", "sum expects list of integers"))?;
    let mut total = 0i64;
    for item in values {
        total += item.as_i64().unwrap_or(0);
    }
    Ok(Value::Number(total.into()))
}

fn action_registry() -> HashMap<String, ActionCallable> {
    let mut actions: HashMap<String, ActionCallable> = HashMap::new();
    actions.insert(
        "double".to_string(),
        Arc::new(|kwargs| Box::pin(action_double(kwargs))),
    );
    actions.insert(
        "sum".to_string(),
        Arc::new(|kwargs| Box::pin(action_sum(kwargs))),
    );
    actions
}

async fn action_handler(
    action: ir::ActionCall,
    kwargs: HashMap<String, Value>,
) -> Result<Value, ExecutionError> {
    match action.action_name.as_str() {
        "double" => {
            let value = kwargs
                .get("value")
                .and_then(|value| value.as_i64())
                .ok_or_else(|| {
                    ExecutionError::new("ExecutionError", "double expects integer value")
                })?;
            Ok(Value::Number((value * 2).into()))
        }
        "sum" => {
            let values = kwargs
                .get("values")
                .and_then(|value| value.as_array())
                .ok_or_else(|| {
                    ExecutionError::new("ExecutionError", "sum expects list of integers")
                })?;
            let mut total = 0i64;
            for item in values {
                total += item.as_i64().unwrap_or(0);
            }
            Ok(Value::Number(total.into()))
        }
        _ => Err(ExecutionError::new(
            "ExecutionError",
            format!("unknown action: {}", action.action_name),
        )),
    }
}

fn build_runner_demo_state() -> (RunnerState, HashMap<Uuid, Value>) {
    let mut state = RunnerState::new(None, None, None, true);
    let empty_list_expr = list_expr(Vec::new());
    let _ = state
        .record_assignment(
            vec!["results".to_string()],
            &empty_list_expr,
            None,
            Some("results = []".to_string()),
        )
        .expect("record assignment");

    let mut action_results = HashMap::new();
    for (idx, item) in [1i64, 2i64].iter().enumerate() {
        let mut kwargs = HashMap::new();
        kwargs.insert(
            "item".to_string(),
            ValueExpr::Literal(LiteralValue {
                value: Value::Number((*item).into()),
            }),
        );
        let action_ref = state
            .queue_action(
                "action",
                Some(vec!["action_result".to_string()]),
                Some(kwargs),
                None,
                Some(idx as i32),
            )
            .expect("queue action");
        action_results.insert(action_ref.node_id, Value::Number((*item).into()));

        let action_plus = binary(
            variable("action_result"),
            ir::BinaryOperator::BinaryOpAdd,
            literal_int(2),
        );
        let concat_expr = binary(
            variable("results"),
            ir::BinaryOperator::BinaryOpAdd,
            list_expr(vec![action_plus]),
        );
        let _ = state
            .record_assignment(vec!["results".to_string()], &concat_expr, None, None)
            .expect("record assignment");
    }

    (state, action_results)
}

async fn run_executor_demo(
    dag: &DAG,
    inputs: &HashMap<String, Value>,
) -> Result<(), ExecutionError> {
    let mut state = RunnerState::new(Some(dag.clone()), None, None, false);
    let queue = Arc::new(Mutex::new(VecDeque::new()));
    let backend = MemoryBackend::with_queue(queue.clone());
    for (name, value) in inputs {
        let expr = literal_from_value(value);
        let label = format!("input {name} = {value}");
        let _ = state
            .record_assignment(vec![name.clone()], &expr, None, Some(label))
            .expect("record assignment");
    }

    let entry_node = dag
        .entry_node
        .clone()
        .ok_or_else(|| ExecutionError::new("ExecutionError", "DAG entry node not found"))?;
    let entry_exec = state
        .queue_template_node(&entry_node, None)
        .map_err(|err| ExecutionError::new("ExecutionError", err.0))?;

    let worker_pool = InlineWorkerPool::new(action_registry());
    let mut runloop = RunLoop::new(worker_pool, backend, 25, None, 0.05, 0.1);
    queue.lock().expect("queue lock").push_back(QueuedInstance {
        dag: dag.clone(),
        entry_node: entry_exec.node_id,
        state: Some(state),
        nodes: None,
        edges: None,
        action_results: Some(HashMap::new()),
        instance_id: Uuid::new_v4(),
    });
    let result = runloop
        .run()
        .await
        .map_err(|err| ExecutionError::new("ExecutionError", err.to_string()))?;
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

#[derive(Clone)]
struct SmokeCase {
    name: String,
    program: ir::Program,
    inputs: HashMap<String, Value>,
    run_runner_demo: bool,
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

async fn run_program_smoke(case: &SmokeCase) -> Result<(), ExecutionError> {
    println!("\nIR program ({})", case.name);
    println!("{}", format_program(&case.program));
    println!("IR inputs ({}): {:?}", case.name, case.inputs);
    let dag = convert_to_dag(&case.program)
        .map_err(|err| ExecutionError::new("ExecutionError", err.to_string()))?;
    let slug = slugify(&case.name);
    let output_path = PathBuf::from(format!("dag_smoke_{slug}.png"));
    let output_path = render_dag_image(&dag, &output_path)
        .map_err(|err| ExecutionError::new("ExecutionError", err.to_string()))?;
    println!(
        "DAG image ({}) written to {}",
        case.name,
        output_path.display()
    );

    let executor = StatementExecutor::new(
        case.program.clone(),
        Arc::new(|action, kwargs| Box::pin(action_handler(action, kwargs))),
        None,
    );
    let result = executor
        .execute_program(None, Some(case.inputs.clone()))
        .await?;
    println!("Execution result ({}): {}", case.name, result);

    if case.run_runner_demo {
        let (demo_state, action_results) = build_runner_demo_state();
        let replayed = replay_variables(&demo_state, &action_results)
            .map_err(|err| ExecutionError::new("ExecutionError", err.to_string()))?;
        println!("Runner replay variables: {:?}", replayed.variables);
        run_executor_demo(&dag, &case.inputs).await?;
    }
    Ok(())
}

async fn run_smoke(base: i64) -> i32 {
    let mut cases = Vec::new();
    cases.push(SmokeCase {
        name: "smoke".to_string(),
        program: build_program(),
        inputs: HashMap::from([("base".to_string(), Value::Number(base.into()))]),
        run_runner_demo: true,
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
            run_runner_demo: false,
        });
    }

    let mut failures = 0;
    for case in &cases {
        if let Err(err) = run_program_smoke(case).await {
            failures += 1;
            println!("Smoke case '{}' failed: {}", case.name, err);
        }
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
