//! What the crate's tests share: a task error and a few task futures.

#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("shutdown observed")]
    Shutdown,

    #[error("broke")]
    Broke,
}

pub async fn until_shutdown(
    shutdown_token: tokio_util::sync::CancellationToken,
) -> Result<std::convert::Infallible, TaskError> {
    shutdown_token.cancelled().await;
    Err(TaskError::Shutdown)
}

pub async fn break_now() -> Result<std::convert::Infallible, TaskError> {
    Err(TaskError::Broke)
}

pub async fn panic_now() -> Result<std::convert::Infallible, TaskError> {
    panic!("boom");
}

pub async fn bare_unit_panic_now() {
    panic!("boom");
}

pub async fn bare_infallible_panic_now() -> std::convert::Infallible {
    panic!("boom");
}
