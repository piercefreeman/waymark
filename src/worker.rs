//! Worker process management.
//!
//! Manages two pools of Python worker processes:
//! - Action workers: Execute individual actions
//! - Instance workers: Run workflow instances with replay

use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

/// Type of worker process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerType {
    Action,
    Instance,
}

impl WorkerType {
    pub fn command_name(&self) -> &'static str {
        match self {
            WorkerType::Action => "rappel-action-worker",
            WorkerType::Instance => "rappel-instance-worker",
        }
    }

    pub fn grpc_addr_env(&self) -> &'static str {
        match self {
            WorkerType::Action => "RAPPEL_ACTION_GRPC_ADDR",
            WorkerType::Instance => "RAPPEL_INSTANCE_GRPC_ADDR",
        }
    }
}

/// A managed worker process.
pub struct WorkerProcess {
    pub id: u64,
    pub worker_type: WorkerType,
    process: Child,
}

impl WorkerProcess {
    /// Spawn a new worker process.
    pub fn spawn(
        id: u64,
        worker_type: WorkerType,
        grpc_addr: &str,
        user_modules: &[String],
        working_dir: Option<&str>,
    ) -> anyhow::Result<Self> {
        let mut cmd = Command::new("uv");
        cmd.arg("run")
            .arg(worker_type.command_name())
            .arg("--bridge")
            .arg(grpc_addr)
            .arg("--worker-id")
            .arg(id.to_string());

        for module in user_modules {
            cmd.arg("--user-module").arg(module);
        }

        // Set working directory if specified (needed for uv to find pyproject.toml)
        if let Some(dir) = working_dir {
            cmd.current_dir(dir);
        }

        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());

        let process = cmd.spawn()?;

        Ok(WorkerProcess {
            id,
            worker_type,
            process,
        })
    }

    /// Kill the worker process.
    pub fn kill(&mut self) -> anyhow::Result<()> {
        self.process.kill()?;
        Ok(())
    }

    /// Check if the process is still running.
    pub fn is_alive(&mut self) -> bool {
        matches!(self.process.try_wait(), Ok(None))
    }
}

impl Drop for WorkerProcess {
    fn drop(&mut self) {
        let _ = self.kill();
    }
}

/// Pool of worker processes.
pub struct WorkerPool {
    worker_type: WorkerType,
    grpc_addr: String,
    user_modules: Vec<String>,
    working_dir: Option<String>,
    target_count: usize,
    next_id: AtomicU64,
    workers: Arc<Mutex<Vec<WorkerProcess>>>,
}

impl WorkerPool {
    /// Create a new worker pool.
    pub fn new(
        worker_type: WorkerType,
        grpc_addr: String,
        user_modules: Vec<String>,
        working_dir: Option<String>,
        target_count: usize,
    ) -> Self {
        WorkerPool {
            worker_type,
            grpc_addr,
            user_modules,
            working_dir,
            target_count,
            next_id: AtomicU64::new(1),
            workers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Start all workers in the pool.
    pub async fn start(&self) -> anyhow::Result<()> {
        let mut workers = self.workers.lock().await;

        for _ in 0..self.target_count {
            let id = self.next_id.fetch_add(1, Ordering::SeqCst);
            match WorkerProcess::spawn(
                id,
                self.worker_type,
                &self.grpc_addr,
                &self.user_modules,
                self.working_dir.as_deref(),
            ) {
                Ok(process) => {
                    info!(
                        worker_type = ?self.worker_type,
                        id = id,
                        "Started worker process"
                    );
                    workers.push(process);
                }
                Err(e) => {
                    error!(
                        worker_type = ?self.worker_type,
                        error = %e,
                        "Failed to start worker process"
                    );
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Stop all workers in the pool.
    pub async fn stop(&self) {
        let mut workers = self.workers.lock().await;

        for mut worker in workers.drain(..) {
            info!(
                worker_type = ?self.worker_type,
                id = worker.id,
                "Stopping worker process"
            );
            let _ = worker.kill();
        }
    }

    /// Check and restart any dead workers.
    pub async fn maintain(&self) -> anyhow::Result<()> {
        let mut workers = self.workers.lock().await;

        // Remove dead workers
        workers.retain_mut(|w| {
            if !w.is_alive() {
                info!(
                    worker_type = ?self.worker_type,
                    id = w.id,
                    "Worker process died, will restart"
                );
                false
            } else {
                true
            }
        });

        // Spawn new workers to reach target count
        while workers.len() < self.target_count {
            let id = self.next_id.fetch_add(1, Ordering::SeqCst);
            match WorkerProcess::spawn(
                id,
                self.worker_type,
                &self.grpc_addr,
                &self.user_modules,
                self.working_dir.as_deref(),
            ) {
                Ok(process) => {
                    info!(
                        worker_type = ?self.worker_type,
                        id = id,
                        "Restarted worker process"
                    );
                    workers.push(process);
                }
                Err(e) => {
                    error!(
                        worker_type = ?self.worker_type,
                        error = %e,
                        "Failed to restart worker process"
                    );
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Get the current number of workers.
    pub async fn count(&self) -> usize {
        self.workers.lock().await.len()
    }
}
