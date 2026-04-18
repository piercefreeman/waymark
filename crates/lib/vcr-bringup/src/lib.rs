//! VCR bringup provides the VCR setup procedures optimized for use in
//! the configurable application initialization.
//!
//! The types are tailored to be easy to use in the sequential initialization
//! logic and take care of type regularization without forcing the use of
//! dyn-traits.

use std::{num::NonZeroUsize, sync::Arc};

use either::Either;

pub mod playback;
pub mod recorder;

pub enum Mode {
    Record {
        log_file_path: std::path::PathBuf,
        command_buffer_size: NonZeroUsize,
    },
    Playback {
        log_file_path: std::path::PathBuf,
        loaded_log_items_buffer_size: NonZeroUsize,
        pool_ingest_buffer_size: NonZeroUsize,
    },
    Off,
}

#[must_use]
pub struct Bringup {
    pub pool: BringupPool,
    pub backend: BringupBackend,
    pub tasks: BringupTasks,
    pub more: BringupMore,
}

#[must_use]
pub enum BringupPool {
    Recorder(waymark_vcr_recorder::pool::Handle),
    Playback(playback::PoolParams),
    None,
}

#[must_use]
pub enum BringupBackend {
    Recorder(waymark_vcr_recorder::backend::Handle),
    None,
}

#[must_use]
pub enum BringupTasks {
    Recorder {
        recorder: tokio::task::JoinHandle<Result<(), waymark_vcr_recorder::Error>>,
    },
    Playback {
        loader: tokio::task::JoinHandle<Result<(), waymark_jsonlines::ReadError>>,
    },
    None,
}

#[must_use]
pub enum BringupMore {
    None,
    Playback {
        player_params: playback::PlayerParams,
    },
}

pub async fn setup(mode: Mode) -> Result<Bringup, std::io::Error> {
    Ok(match mode {
        Mode::Record {
            log_file_path,
            command_buffer_size,
        } => {
            let recorder::Bringup {
                backend_handle,
                pool_handle,
                recorder_task,
            } = recorder::setup(log_file_path, command_buffer_size).await?;

            let pool = BringupPool::Recorder(pool_handle);
            let backend = BringupBackend::Recorder(backend_handle);
            let tasks = BringupTasks::Recorder {
                recorder: recorder_task,
            };
            let more = BringupMore::None;

            Bringup {
                pool,
                backend,
                tasks,
                more,
            }
        }
        Mode::Playback {
            log_file_path,
            pool_ingest_buffer_size,
            loaded_log_items_buffer_size,
        } => {
            let playback::Bringup {
                player_params,
                pool_params,
                loader_task,
            } = playback::setup(
                log_file_path,
                loaded_log_items_buffer_size,
                pool_ingest_buffer_size,
            )
            .await?;

            let pool = BringupPool::Playback(pool_params);
            let backend = BringupBackend::None;
            let tasks = BringupTasks::Playback {
                loader: loader_task,
            };
            let more = BringupMore::Playback { player_params };

            Bringup {
                pool,
                backend,
                tasks,
                more,
            }
        }
        Mode::Off => Bringup {
            pool: BringupPool::None,
            backend: BringupBackend::None,
            tasks: BringupTasks::None,
            more: BringupMore::None,
        },
    })
}

pub type VcrPool<Pool> = Either<
    waymark_vcr_recorder_worker_pool::Pool<Pool>,
    waymark_vcr_playback_worker_pool::Pool<Arc<waymark_vcr_playback::ExecutionCorrelator>>,
>;
pub type VcrBackend<Backend> = waymark_vcr_recorder_backend::Backend<Backend>;

pub type MaybeVcrPool<Pool> = Either<Pool, VcrPool<Pool>>;
pub type MaybeVcrBackend<Backend> = Either<Backend, VcrBackend<Backend>>;

pub fn pool<Pool>(bringup: BringupPool, pool: Pool) -> MaybeVcrPool<Pool> {
    match bringup {
        BringupPool::Recorder(recorder) => {
            Either::Right(Either::Left(waymark_vcr_recorder_worker_pool::Pool {
                inner: pool,
                recorder,
            }))
        }
        BringupPool::Playback(pool_params) => {
            let playback::PoolParams {
                execution_correlator,
                pool_ingest_buffer_size,
            } = pool_params;

            Either::Right(Either::Right(waymark_vcr_playback_worker_pool::Pool::new(
                execution_correlator,
                pool_ingest_buffer_size,
            )))
        }
        BringupPool::None => Either::Left(pool),
    }
}

pub fn backend<Backend>(bringup: BringupBackend, backend: Backend) -> MaybeVcrBackend<Backend> {
    match bringup {
        BringupBackend::Recorder(recorder) => {
            Either::Right(waymark_vcr_recorder_backend::Backend {
                inner: backend,
                recorder,
            })
        }
        BringupBackend::None => Either::Left(backend),
    }
}
