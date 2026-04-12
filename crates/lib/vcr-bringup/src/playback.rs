use std::{num::NonZeroUsize, sync::Arc};

#[must_use]
pub struct Bringup {
    pub player_params: PlayerParams,
    pub pool_params: PoolParams,
    pub loader_task: tokio::task::JoinHandle<std::result::Result<(), waymark_jsonlines::ReadError>>,
}

#[must_use]
pub struct PlayerParams {
    pub execution_correlator_prep: waymark_vcr_playback::execution_correlator::PrepHandle,
    pub player_rx: waymark_vcr_playback::player::Receiver,
}

#[must_use]
pub struct PoolParams {
    pub execution_correlator: Arc<waymark_vcr_playback::ExecutionCorrelator>,
    pub pool_ingest_buffer_size: NonZeroUsize,
}

pub async fn setup(
    log_file_path: impl AsRef<std::path::Path>,
    loaded_log_items_buffer_size: NonZeroUsize,
    pool_ingest_buffer_size: NonZeroUsize,
) -> Result<Bringup, std::io::Error> {
    let file = tokio::fs::File::open(log_file_path).await?;
    let reader = waymark_vcr_file::Reader::from(tokio::io::BufReader::new(file));

    let (player_tx, player_rx) = tokio::sync::mpsc::channel(loaded_log_items_buffer_size.get());

    let params = waymark_vcr_playback::loader::Params { reader, player_tx };
    let loader_task = tokio::spawn(waymark_vcr_playback::loader::run(params));

    let execution_correlator = Arc::new(waymark_vcr_playback::ExecutionCorrelator::default());
    let execution_correlator_prep = execution_correlator.prep_handle();

    let player_params = PlayerParams {
        execution_correlator_prep,
        player_rx,
    };

    let pool_params = PoolParams {
        execution_correlator,
        pool_ingest_buffer_size,
    };

    Ok(Bringup {
        player_params,
        loader_task,
        pool_params,
    })
}

#[must_use]
pub struct PlayerBringup {
    pub player_task:
        tokio::task::JoinHandle<std::result::Result<(), waymark_backends_core::BackendError>>,
}

pub fn player<Backend>(player_params: PlayerParams, backend: Backend) -> PlayerBringup
where
    Backend: waymark_core_backend::CoreBackend,
    Backend: waymark_workflow_registry_backend::WorkflowRegistryBackend,
    Backend: Send + 'static,
{
    let PlayerParams {
        execution_correlator_prep,
        player_rx,
    } = player_params;

    let params = waymark_vcr_playback::player::Params {
        execution_correlator_prep,
        backend,
        player_rx,
    };
    let player_task = tokio::spawn(waymark_vcr_playback::player::run(params));
    PlayerBringup { player_task }
}
