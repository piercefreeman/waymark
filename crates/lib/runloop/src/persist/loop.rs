use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use tracing::{info, warn};
use uuid::Uuid;
use waymark_backends_core::BackendError;
use waymark_core_backend::{ActionDone, GraphUpdate, InstanceLockStatus, LockClaim};

const PERSIST_COALESCE_WINDOW: Duration = Duration::from_millis(2);
const PERSIST_COALESCE_MAX_COMMANDS: usize = 128;

pub struct Params<CoreBackend>
where
    CoreBackend: ?Sized,
{
    pub command_rx: tokio::sync::mpsc::Receiver<super::Command>,
    pub ack_tx: tokio::sync::mpsc::UnboundedSender<super::Ack>,
    pub core_backend: Arc<CoreBackend>,
    pub lock_ttl: Duration,
    pub lock_uuid: Uuid,
}

pub async fn run<CoreBackend>(params: Params<CoreBackend>)
where
    CoreBackend: ?Sized,
    CoreBackend: waymark_core_backend::CoreBackend,
{
    let Params {
        mut command_rx,
        ack_tx,
        core_backend,
        lock_ttl,
        lock_uuid,
    } = params;

    loop {
        let first_command = command_rx.recv().await;
        let Some(first_command) = first_command else {
            info!("persistence task channel closed");
            break;
        };
        let mut step_commands = vec![first_command];
        let mut channel_closed = false;
        let deadline = Instant::now() + PERSIST_COALESCE_WINDOW;
        while step_commands.len() < PERSIST_COALESCE_MAX_COMMANDS {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let wait = deadline.saturating_duration_since(now);
            match tokio::time::timeout(wait, command_rx.recv()).await {
                Ok(Some(command)) => step_commands.push(command),
                Ok(None) => {
                    channel_closed = true;
                    break;
                }
                Err(_) => break,
            }
        }

        let mut all_actions_done: Vec<ActionDone> = Vec::new();
        let mut all_graph_updates: Vec<GraphUpdate> = Vec::new();
        for command in &mut step_commands {
            all_actions_done.append(&mut command.actions_done);
            all_graph_updates.append(&mut command.graph_updates);
        }

        let outcome: Result<HashMap<Uuid, InstanceLockStatus>, BackendError> = async {
            if !all_actions_done.is_empty() {
                core_backend.save_actions_done(&all_actions_done).await?;
            }
            if all_graph_updates.is_empty() {
                return Ok(HashMap::new());
            }
            let lock_expires_at = Utc::now()
                + chrono::Duration::from_std(lock_ttl)
                    .unwrap_or_else(|_| chrono::Duration::seconds(0));
            let lock_statuses = core_backend
                .save_graphs(
                    LockClaim {
                        lock_uuid,
                        lock_expires_at,
                    },
                    &all_graph_updates,
                )
                .await?;
            let mut lock_status_by_instance: HashMap<Uuid, InstanceLockStatus> =
                HashMap::with_capacity(lock_statuses.len());
            for status in lock_statuses {
                lock_status_by_instance.insert(status.instance_id, status);
            }
            Ok(lock_status_by_instance)
        }
        .await;

        match outcome {
            Ok(lock_status_by_instance) => {
                for command in step_commands {
                    if command.instance_ids.is_empty() {
                        continue;
                    }
                    let graph_instance_count = command.graph_instance_ids.len();
                    let mut missing_lock_statuses = 0usize;
                    let mut lock_statuses = Vec::with_capacity(graph_instance_count);
                    for instance_id in command.graph_instance_ids {
                        if let Some(status) = lock_status_by_instance.get(&instance_id) {
                            lock_statuses.push(status.clone());
                        } else {
                            missing_lock_statuses += 1;
                            lock_statuses.push(InstanceLockStatus {
                                instance_id,
                                lock_uuid: None,
                                lock_expires_at: None,
                            });
                        }
                    }
                    if missing_lock_statuses > 0 {
                        warn!(
                            batch_id = command.batch_id,
                            graph_instance_count,
                            missing_lock_statuses,
                            "persist ack missing graph lock statuses"
                        );
                    }
                    let ack = super::Ack::StepsPersisted {
                        batch_id: command.batch_id,
                        lock_statuses,
                    };
                    if ack_tx.send(ack).is_err() {
                        warn!("persistence ack receiver dropped");
                        break;
                    }
                }
            }
            Err(error) => {
                let error_message = format!("persistence batch failed: {error}");
                for command in step_commands {
                    let ack = super::Ack::StepsPersistFailed {
                        batch_id: command.batch_id,
                        error: error_message.clone(),
                    };
                    if ack_tx.send(ack).is_err() {
                        warn!("persistence ack receiver dropped");
                        break;
                    }
                }
            }
        }
        if channel_closed {
            info!("persistence task channel closed");
            break;
        }
    }
    info!("persistence task exiting");
}
