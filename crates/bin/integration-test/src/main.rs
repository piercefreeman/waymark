//! Fixture integration parity runner.
//!
//! For each curated fixture case:
//! 1. Ask a Python helper for ground-truth inline execution and compiled IR.
//! 2. Execute that IR through the VM over a Python worker pool — transiently
//!    (in-memory, no persistence) and durably (postgres-backed snapshots,
//!    action calls, and sleeps via the execution subsystem).
//! 3. Assert the VM workflow outcome matches inline Python output.

mod cases;
mod cli;
mod compare;
mod durable;
mod ground_truth;
mod outcome;
mod transient;
mod worker_pool;

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser as _;
use color_eyre::eyre::{WrapErr as _, bail};

#[tokio::main]
async fn main() -> Result<(), waymark_fn_main_common::Error> {
    waymark_fn_main_common::init()?;

    let args = cli::Args::parse();
    let modes = cli::parse_modes(&args.modes)?;
    let selected_cases = cases::select_cases(&args.cases)?;
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
    let timeout = Duration::from_secs(args.timeout_seconds);

    if selected_cases.is_empty() {
        bail!("no fixture cases selected");
    }

    let mut prepared_cases = Vec::new();
    for case in selected_cases {
        prepared_cases.push(
            ground_truth::prepare_case(&repo_root, case.clone()).wrap_err_with(|| {
                format!(
                    "prepare fixture case '{}' ({}::{})",
                    case.id, case.module_name, case.workflow_class
                )
            })?,
        );
    }

    let mut failures = Vec::new();
    let mut comparisons = 0usize;

    for mode in &modes {
        let mode_failures = match mode {
            cli::ExecutionMode::Transient => {
                transient::run_transient_mode(
                    &repo_root,
                    &prepared_cases,
                    args.worker_count,
                    timeout,
                )
                .await
            }
            cli::ExecutionMode::Durable => {
                durable::run_durable_mode(&repo_root, &prepared_cases, args.worker_count, timeout)
                    .await
            }
        }
        .wrap_err_with(|| format!("run {} execution mode", mode.label()))?;

        comparisons += prepared_cases.len();
        failures.extend(
            mode_failures
                .into_iter()
                .map(|failure| format!("mode={}\n{}", mode.label(), failure)),
        );
    }

    if !failures.is_empty() {
        eprintln!(
            "fixture integration parity failed: {} mismatches across {} comparisons",
            failures.len(),
            comparisons,
        );
        for failure in failures {
            eprintln!(
                "--------------------------------------------------------------------------------"
            );
            eprintln!("{failure}");
        }
        std::process::exit(1);
    }

    println!(
        "fixture integration parity passed: {} cases across {} mode comparisons",
        prepared_cases.len(),
        comparisons,
    );

    Ok(())
}
