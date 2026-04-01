#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct InitTracingError(pub Box<dyn std::error::Error + Send + Sync + 'static>);

pub fn init_tracing() -> Result<(), InitTracingError> {
    tracing_subscriber::fmt::try_init().map_err(InitTracingError)
}
