pub use waymark_vcr_file::instance::LogItem;

pub type Sender = tokio::sync::mpsc::Sender<LogItem>;
pub type Receiver = tokio::sync::mpsc::Receiver<LogItem>;

pub struct Params<Backend> {
    pub execution_correlator_prep: crate::execution_correlator::PrepHandle,
    pub backend: Backend,
    pub player_rx: Receiver,
    pub semaphore: tokio::sync::Semaphore,
}

pub async fn run<Backend>(params: Params<Backend>)
where
    Backend: waymark_core_backend::CoreBackend,
{
    let Params {
        execution_correlator_prep,
        backend,
        player_rx,
        semaphore,
    } = params;

    loop {
        semaphore.acquire().await;

        let Some(log_item) = player_rx.recv().await else {
            break;
        };

        let waymark_vcr_file::instance::LogItem { actions } = log_item;

        let executor_id = waymark_ids::InstanceId::new_uuid_v4();

        let queued_instance = todo!();

        backend.queue_instances(&[queued_instance]).await.unwrap();

        for log_item in actions {
            execution_correlator_prep.prepare_correlation(
                executor_id,
                execution_id,
                attempt_number,
                log_item,
            );
        }
    }
}
