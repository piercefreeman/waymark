//! CLI smoke check for Python worker components.

use std::collections::HashMap;
use std::sync::Arc;

use clap::Parser;
use waymark_smoke_sources::{
    build_control_flow_program, build_parallel_spread_program, build_program,
    build_try_except_program, build_while_loop_program,
};
use waymark_system_vm::{ReadyValue, Value};
use waymark_worker_core::LaunchWorkerPool as _;

#[derive(Parser, Debug)]
#[command(
    name = "waymark-smoke",
    about = "Smoke check Python worker components."
)]
struct SmokeArgs {
    #[arg(long, default_value_t = 5)]
    base: i64,
}

struct SmokeCase {
    name: String,
    program: waymark_vm_ast_old::Program,
    inputs: HashMap<String, Value>,
}

async fn run_program_smoke<Pool>(
    case: &SmokeCase,
    worker_pool: Pool,
) -> Result<(), color_eyre::eyre::Report>
where
    Pool: waymark_worker_core::QueueActionDispatch
        + waymark_worker_core::PollActionResults
        + Clone
        + Send
        + Sync
        + 'static,
    <Pool as waymark_worker_core::QueueActionDispatch>::Error: core::fmt::Debug + Send + 'static,
    <Pool as waymark_worker_core::PollActionResults>::Error: core::fmt::Debug + Send + 'static,
{
    println!("\nAST program ({})", case.name);
    println!("{}", waymark_vm_ast_old_fmt::display(&case.program));
    println!("Inputs ({}): {:?}", case.name, case.inputs);

    let runtime =
        waymark_transient_execution_bringup::setup_runtime(&case.program, case.inputs.clone())?;

    let waymark_transient_execution_bringup::Execution {
        workflow_outcome_rx,
        driver_handle,
    } = waymark_transient_execution_worker_pool_bringup::execute(
        runtime,
        worker_pool,
        false,
        tokio_util::sync::CancellationToken::new(),
    );

    let workflow_outcome = workflow_outcome_rx.await;

    // The driver terminates right after delivering the workflow outcome —
    // including on success — so join it unconditionally for its exit report.
    let Err(driver_exit) = driver_handle.await;
    tracing::debug!(?driver_exit, "vm driver exited");

    let workflow_outcome = workflow_outcome.map_err(|_recv_error| {
        color_eyre::eyre::eyre!("vm driver exited without a workflow outcome")
    })?;

    println!("Workflow outcome ({}): {:?}", case.name, workflow_outcome);

    match workflow_outcome {
        waymark_workflow_completion_core::Outcome::Completion(_value) => Ok(()),
        waymark_workflow_completion_core::Outcome::Exception(exception) => {
            Err(color_eyre::eyre::eyre!(
                "workflow terminated with an unhandled exception: {exception:?}"
            ))
        }
    }
}

async fn run_smoke(base: i64) -> i32 {
    let shutdown_token = tokio_util::sync::CancellationToken::new();

    let worker_config = waymark_worker_python::Config::new()
        .with_user_module("tests.fixtures.test_actions")
        .with_python_paths(vec![repo_root().join("python")]);

    let result = waymark_worker_remote_bringup::start(
        shutdown_token.clone(),
        None,
        |bridge_server_addr| waymark_worker_python::Spec {
            config: worker_config,
            bridge_server_addr,
        },
        2.try_into().unwrap(),
        None,
        10.try_into().unwrap(),
    )
    .await;

    let (process_pool, mut bridge_server_task) = match result {
        Ok(val) => val,
        Err(err) => {
            println!("Failed to start python worker pool: {err}");
            return 1;
        }
    };
    let worker_pool = Arc::new(waymark_worker_remote_pool::RemoteWorkerPool::new(
        process_pool,
    ));
    if let Err(err) = worker_pool.launch().await {
        println!("Failed to launch python worker pool: {err}");
        return 1;
    }

    let mut failures = 0;
    let mut cases = Vec::new();
    let examples = vec![
        ("smoke", Ok(build_program())),
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
                failures += 1;
                continue;
            }
        };
        let program = match waymark_vm_ast_old_proto::convert(program) {
            Ok(value) => value,
            Err(err) => {
                println!("Failed to convert {name} program to the AST: {err}");
                failures += 1;
                continue;
            }
        };
        let inputs = match name {
            "smoke" => HashMap::from([("base".to_string(), Value::Ready(ReadyValue::Int(base)))]),
            "control_flow" => {
                HashMap::from([("base".to_string(), Value::Ready(ReadyValue::Int(2)))])
            }
            "parallel_spread" => {
                HashMap::from([("base".to_string(), Value::Ready(ReadyValue::Int(3)))])
            }
            "try_except" => HashMap::from([(
                "values".to_string(),
                Value::Ready(ReadyValue::List(vec![
                    Value::Ready(ReadyValue::Int(1)),
                    Value::Ready(ReadyValue::Int(2)),
                    Value::Ready(ReadyValue::Int(3)),
                ])),
            )]),
            "while_loop" => {
                HashMap::from([("limit".to_string(), Value::Ready(ReadyValue::Int(6)))])
            }
            _ => HashMap::new(),
        };
        cases.push(SmokeCase {
            name: name.to_string(),
            program,
            inputs,
        });
    }

    for case in &cases {
        if let Err(err) = run_program_smoke(case, Arc::clone(&worker_pool)).await {
            failures += 1;
            println!("Smoke case '{}' failed: {}", case.name, err);
        }
    }

    shutdown_token.cancel();
    let bridge_server_shutdown =
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut bridge_server_task).await;
    if bridge_server_shutdown.is_err() {
        tracing::warn!("bridge server did not stop in time, aborting it");
        bridge_server_task.abort();
        let _ = bridge_server_task.await;
    }

    if let Err(err) = worker_pool.shutdown_arc().await {
        println!("Failed to shut down worker pool: {err}");
    }

    if failures > 0 { 1 } else { 0 }
}

/// The workspace root, resolved from this crate's manifest directory
/// (`crates/bin/smoke`).
fn repo_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("the manifest dir has a workspace root three levels up")
        .to_path_buf()
}

pub fn main() -> Result<(), waymark_fn_main_common::Error> {
    waymark_fn_main_common::init()?;

    let args = SmokeArgs::parse();
    let runtime = tokio::runtime::Runtime::new()?;
    let code = runtime.block_on(run_smoke(args.base));
    std::process::exit(code);
}
