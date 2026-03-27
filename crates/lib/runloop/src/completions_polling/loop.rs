use std::sync::Arc;

use tracing::{debug, info};
use waymark_worker_core::ActionCompletion;

use waymark_utils_tokio_channel::send_with_stop;

pub struct Params<WorkerPool>
where
    WorkerPool: ?Sized,
{
    pub shutdown_token: tokio_util::sync::CancellationToken,
    pub worker_pool: Arc<WorkerPool>,
    pub completion_tx: tokio::sync::mpsc::Sender<waymark_timed::Opaque<Vec<ActionCompletion>>>,
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
            Some(completions) = worker_pool.poll_complete() => {
                debug!(count = completions.len(), "completion task received completions");
                completions
            },
            else => {
                // No shutdown and no completions - we poll again.
                continue;
            }
        };

        debug!(
            count = completions.len(),
            "completion task sending completions"
        );

        let completions: Vec<_> = completions.into(); // TODO: pass non-empty vec

        let send_result = send_with_stop(
            &completion_tx,
            completions.into(),
            shutdown_token.cancelled(),
            "completions",
        )
        .await;
        if send_result.is_err() {
            break;
        }

        debug!("completion task sent completions");
    }
    info!("completion task exiting");
}
