//! Action benchmark harness.
//!
//! NOTE: Stubbed out pending migration to new store/scheduler model.

use std::time::Duration;
use anyhow::Result;

use crate::{
    benchmark::common::BenchmarkSummary,
    worker::PythonWorkerConfig,
    store::Store,
};

#[derive(Debug, Clone)]
pub struct HarnessConfig {
    pub total_messages: usize,
    pub in_flight: usize,
    pub payload_size: usize,
    pub progress_interval: Option<Duration>,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        Self {
            total_messages: 10_000,
            in_flight: 32,
            payload_size: 4096,
            progress_interval: None,
        }
    }
}

pub struct BenchmarkHarness {
    _store: Store,
}

impl BenchmarkHarness {
    pub async fn new(
        _config: PythonWorkerConfig,
        _worker_count: usize,
        store: Store,
    ) -> Result<Self> {
        Ok(Self { _store: store })
    }

    pub async fn run(&self, _config: &HarnessConfig) -> Result<BenchmarkSummary> {
        // TODO: Reimplement using new Store
        unimplemented!("Benchmark harness needs migration to new store/scheduler model")
    }

    pub async fn shutdown(self) -> Result<()> {
        Ok(())
    }
}
