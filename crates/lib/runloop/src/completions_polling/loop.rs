use std::sync::Arc;

use tracing::{debug, info};
use waymark_worker_core::ActionCompletion;

use crate::channel_utils::send_with_stop;

pub struct Params<WorkerPool>
where
    WorkerPool: ?Sized,
{
    pub shutdown_token: tokio_util::sync::CancellationToken,
    pub worker_pool: Arc<WorkerPool>,
    pub completion_tx: tokio::sync::mpsc::Sender<Vec<ActionCompletion>>,
}

pub async fn run<WorkerPool>(params: Params<WorkerPool>)
where
    WorkerPool: ?Sized,
    WorkerPool: waymark_worker_core::BaseWorkerPool,
{
    let Params {
        shutdown_token,
        worker_pool,
        completion_tx,
    } = params;

    loop {
        if shutdown_token.is_cancelled() {
            info!("completion task stop flag set");
            break;
        }
        debug!("completion task awaiting completions");
        let completions = tokio::select! {
            _ = shutdown_token.cancelled() => {
                info!("completion task stop notified");
                break;
            }
            completions = worker_pool.get_complete() => {
                debug!(count = completions.len(), "completion task received completions");
                completions
            },
        };
        if completions.is_empty() {
            continue;
        }
        debug!(
            count = completions.len(),
            "completion task sending completions"
        );

        if !send_with_stop(
            &completion_tx,
            completions,
            shutdown_token.cancelled(),
            "completions",
        )
        .await
        {
            break;
        }

        debug!("completion task sent completions");
    }
    info!("completion task exiting");
}
