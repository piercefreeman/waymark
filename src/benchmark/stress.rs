//! Stress benchmark harness.
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
pub struct StressBenchmarkConfig {
    pub instance_count: usize,
    pub fan_out_factor: usize,
    pub loop_iterations: usize,
    pub work_intensity: usize,
    pub payload_size: usize,
    pub in_flight: usize,
    pub progress_interval: Option<Duration>,
}

impl Default for StressBenchmarkConfig {
    fn default() -> Self {
        Self {
            instance_count: 50,
            fan_out_factor: 16,
            loop_iterations: 8,
            work_intensity: 1000,
            payload_size: 1024,
            in_flight: 64,
            progress_interval: None,
        }
    }
}

pub struct StressBenchmarkHarness {
    _store: Store,
}

impl StressBenchmarkHarness {
    pub async fn new(
        store: Store,
        _worker_count: usize,
        _worker_config: PythonWorkerConfig,
    ) -> Result<Self> {
        Ok(Self { _store: store })
    }

    pub async fn run(&self, _config: &StressBenchmarkConfig) -> Result<BenchmarkSummary> {
        unimplemented!("Stress benchmark needs migration to new store/scheduler model")
    }

    pub async fn shutdown(self) -> Result<()> {
        Ok(())
    }

    pub fn actions_per_instance(&self) -> usize {
        0
    }
}
