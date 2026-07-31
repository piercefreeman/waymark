//! Common `fn main` things that are fit for any `fn main` in the project.
//!
//! This crate is not supposed to include any "business-logic"-specific things,
//! like bringup logic or executable-specific initialization.
//! Only the common things that would be used in an "arbitrarty" executable
//! are allowed.
//!
//! Binaries that want extra tracing layers (profiling, consoles, …) inject
//! them through [`init_with_tracing_layer`]; the layer providers live in
//! their own crates and are only linked by the binaries that pull them in.

#![warn(missing_docs)]

use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

/// The all-encompassing error type to use for `fn main`.
pub use color_eyre::eyre::Report as Error;

/// Error returned when tracing initialization fails.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct InitTracingError(pub Box<dyn std::error::Error + Send + Sync + 'static>);

/// Error returned when `color-eyre` initialization fails.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct InitColorEyreError(pub color_eyre::eyre::Report);

/// Error returned the initialization fails.
#[derive(Debug, thiserror::Error)]
pub enum InitError {
    /// The `tracing` init failed.
    #[error("tracing: {0}")]
    Tracing(InitTracingError),

    /// The `color-eyre` init failed.
    #[error("color-eyre: {0}")]
    ColorEyre(InitColorEyreError),
}

/// The `RUST_LOG` filter for the fmt output, defaulting to `INFO`.
///
/// Mirrors what [`tracing_subscriber::fmt::try_init`] builds without the
/// `env-filter` feature, so [`init`] keeps its filtering behavior now that
/// the subscriber is composed from layers.
fn fmt_filter() -> tracing_subscriber::filter::Targets {
    use tracing_subscriber::filter::{LevelFilter, Targets};

    let default = || Targets::new().with_default(LevelFilter::INFO);
    match std::env::var("RUST_LOG") {
        Ok(var) => var
            .parse()
            .map_err(|error| eprintln!("Ignoring `RUST_LOG={var:?}`: {error}"))
            .unwrap_or_default(),
        Err(std::env::VarError::NotPresent) => default(),
        Err(error) => {
            eprintln!("Ignoring `RUST_LOG`: {error}");
            default()
        }
    }
}

/// Initializes the global tracing subscriber for the process, with an
/// extra layer composed in front of the fmt output.
///
/// The extra layer sees everything (it applies its own filtering); the
/// fmt output stays filtered by `RUST_LOG` (default `INFO`).
pub fn init_tracing_with_layer<ExtraLayer>(extra: ExtraLayer) -> Result<(), InitTracingError>
where
    ExtraLayer: tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    use tracing_subscriber::Layer as _;

    tracing_subscriber::registry()
        .with(extra)
        .with(tracing_subscriber::fmt::layer().with_filter(fmt_filter()))
        .try_init()
        .map_err(|error| InitTracingError(Box::new(error)))
}

/// Initializes the global tracing subscriber for the process.
pub fn init_tracing() -> Result<(), InitTracingError> {
    init_tracing_with_layer(tracing_subscriber::layer::Identity::new())
}

/// Initializes the global panic and error report hooks for the process.
pub fn init_color_eyre() -> Result<(), InitColorEyreError> {
    color_eyre::install().map_err(InitColorEyreError)
}

/// Perform common global initialization, with an extra tracing layer
/// composed in; see [`init_tracing_with_layer`].
pub fn init_with_tracing_layer<ExtraLayer>(extra: ExtraLayer) -> Result<(), InitError>
where
    ExtraLayer: tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    init_color_eyre().map_err(InitError::ColorEyre)?;
    init_tracing_with_layer(extra).map_err(InitError::Tracing)?;
    Ok(())
}

/// Perform common global initialization.
pub fn init() -> Result<(), InitError> {
    init_with_tracing_layer(tracing_subscriber::layer::Identity::new())
}
