use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

use waymark_dag_builder::convert_to_dag;
use waymark_ir_parser::parse_program;
use waymark_proto::ast as ir;
use waymark_runner::{RunnerExecutor, replay_variables};
use waymark_runner_state::RunnerState;

/// Environment variable that signals this process is the overflow probe
/// subprocess spawned by `test_deep_chain_overflows_default_stack`.
const OVERFLOW_PROBE_VAR: &str = "WAYMARK_EXECUTOR_OVERFLOW_PROBE";

/// Build a fully executed `RunnerState` for a `chain_len`-deep assignment chain
/// using the executor's iterative walk.  Safe on any stack size.
fn build_completed_state(chain_len: usize) -> RunnerState {
    let mut source = String::from("fn main(input: [x], output: [y]):\n");
    source.push_str("    acc = x\n");
    for _ in 0..chain_len {
        source.push_str("    acc = acc + 1\n");
    }
    source.push_str("    y = acc\n");
    source.push_str("    return y\n");

    let program = parse_program(source.trim()).expect("parse program");
    let dag = Arc::new(convert_to_dag(&program).expect("convert to dag"));

    let mut state = RunnerState::new(Some(Arc::clone(&dag)), None, None, false);
    state
        .record_assignment(
            vec!["x".to_string()],
            &ir::Expr {
                kind: Some(ir::expr::Kind::Literal(ir::Literal {
                    value: Some(ir::literal::Value::IntValue(1)),
                })),
                span: None,
            },
            None,
            Some("input x = 1".to_string()),
        )
        .expect("record assignment");

    let entry_node = dag.entry_node.as_ref().expect("DAG entry node").clone();
    let entry_exec = state
        .queue_template_node(&entry_node, None)
        .expect("queue entry node");

    // increment() is iterative (not recursive) and therefore safe on any stack.
    let mut executor = RunnerExecutor::<true>::new(Arc::clone(&dag), state, HashMap::new());
    executor
        .increment(&[entry_exec.node_id])
        .expect("executor increment");

    executor.state().clone()
}

/// Part 1 — validate that the problem still requires the fix.
///
/// `replay_variables` recurses once per assignment in the chain (each
/// `acc = acc + 1` must look up the prior `acc`).  On the OS-default thread
/// stack (~8 MB on Linux x86_64) a 2 000-step chain causes a stack overflow,
/// which Rust's signal handler converts to `abort()`, killing the process.
///
/// Because `abort()` is not recoverable inside the process, this test spawns
/// itself as a subprocess.  The subprocess runs the dangerous code and is
/// expected to die; the parent asserts non-zero exit.
#[test]
#[ignore = "stress regression; run manually with `cargo test --test stack_overflow -- --ignored`"]
fn test_deep_chain_overflows_default_stack() {
    if std::env::var(OVERFLOW_PROBE_VAR).is_ok() {
        // We are the subprocess. Build the state on the main thread (safe,
        // iterative), then replay on a default-stack OS thread (dangerous).
        let state = build_completed_state(2_000);
        std::thread::spawn(move || {
            let _ = replay_variables(&state, &HashMap::new());
            // If replay_variables returned without overflowing the chain is
            // too short; exit 0 so the parent assertion fails explicitly.
            std::process::exit(0);
        })
        .join()
        .ok();
        // The thread's abort() kills the subprocess before we reach here.
        unreachable!();
    }

    let self_exe = std::env::current_exe().expect("current exe path");
    let status = std::process::Command::new(&self_exe)
        .args([
            "test_deep_chain_overflows_default_stack",
            "--exact",
            "--ignored",
        ])
        .env(OVERFLOW_PROBE_VAR, "1")
        .env("RUST_BACKTRACE", "0")
        .status()
        .expect("spawn overflow-probe subprocess");

    assert!(
        !status.success(),
        "overflow-probe subprocess should have aborted (stack overflow); \
         if this fails the chain may be too short for the current platform's default stack"
    );
}

/// Part 2 — validate that the fix is sufficient.
///
/// The same `replay_variables` call that overflows on an 8 MB stack completes
/// correctly when given 128 MB, matching the stack size that `RunLoop` grants
/// each executor shard thread.
#[test]
#[ignore = "stress regression; run manually with `cargo test --test stack_overflow -- --ignored`"]
fn test_deep_chain_succeeds_with_128mb_stack() {
    let chain_len: usize = 2_000;
    let state = build_completed_state(chain_len);

    let handle = std::thread::Builder::new()
        .stack_size(128 * 1024 * 1024)
        .spawn(move || replay_variables(&state, &HashMap::new()))
        .expect("spawn large-stack thread");

    let result = handle
        .join()
        .expect("thread should not panic")
        .expect("replay_variables");

    assert_eq!(
        result.variables.get("y"),
        Some(&Value::Number(((chain_len as i64) + 1).into()))
    );
}
