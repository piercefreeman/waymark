//! Command-line interface for the fixture integration parity runner.

use std::num::NonZeroUsize;

use anyhow::{Result, bail};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "integration_test")]
pub struct Args {
    /// Comma-separated execution mode list. Supported: transient,durable.
    #[arg(long, default_value = "transient,durable")]
    pub modes: String,

    /// Optional fixture case IDs to run.
    #[arg(long = "case")]
    pub cases: Vec<String>,

    /// Number of Python workers for VM execution.
    #[arg(long, default_value_t = 2.try_into().unwrap())]
    pub worker_count: NonZeroUsize,

    /// Timeout per case execution, in seconds.
    #[arg(long, default_value_t = 120)]
    pub timeout_seconds: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionMode {
    Transient,
    Durable,
}

impl ExecutionMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Durable => "durable",
        }
    }
}

pub fn parse_modes(raw: &str) -> Result<Vec<ExecutionMode>> {
    let mut parsed = Vec::new();
    for item in raw.split(',') {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            continue;
        }
        match trimmed {
            "transient" => parsed.push(ExecutionMode::Transient),
            "durable" => parsed.push(ExecutionMode::Durable),
            other => bail!("unsupported execution mode '{other}'"),
        }
    }

    if parsed.is_empty() {
        bail!("no execution modes requested")
    }

    Ok(parsed)
}
