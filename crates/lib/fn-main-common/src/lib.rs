//! Common `fn main` things that are fit for any `fn main` in the project.
//!
//! This crate is not supposed to include any "business-logic"-specific things,
//! like bringup logic or executable-specific initialization.
//! Only the common things that would be used in an "arbitrarty" executable
//! are allowed.
//!
//! Binaries that want extra tracing layers (profiling, consoles, …) fill
//! the slots on [`tracing::Params`]; the layer providers live in their
//! own crates and are only linked by the binaries that pull them in.

#![warn(missing_docs)]

pub mod tracing;

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

/// Initializes the global panic and error report hooks for the process.
pub fn init_color_eyre() -> Result<(), InitColorEyreError> {
    color_eyre::install().map_err(InitColorEyreError)
}

/// The parameters for the common global initialization.
#[derive(Debug, Default)]
pub struct Params<FilterBypassingLayer, FilterWrappedLayer> {
    /// The tracing initialization parameters; see [`tracing::Params`].
    pub tracing: tracing::Params<FilterBypassingLayer, FilterWrappedLayer>,

    /// Skip installing the `color-eyre` panic and error report hooks.
    pub skip_color_eyre: bool,
}

impl Params<tracing::NoExtraLayer, tracing::NoExtraLayer> {
    /// The default [`Params`]: no extra tracing layers, `color-eyre`
    /// installed.
    pub fn new() -> Self {
        Self {
            tracing: tracing::Params::new(),
            skip_color_eyre: false,
        }
    }
}

/// Perform common global initialization, with the given parameters.
pub fn init_with<FilterBypassingLayer, FilterWrappedLayer>(
    params: Params<FilterBypassingLayer, FilterWrappedLayer>,
) -> Result<(), InitError>
where
    FilterBypassingLayer:
        tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
    FilterWrappedLayer: tracing_subscriber::Layer<
            tracing_subscriber::layer::Layered<FilterBypassingLayer, tracing_subscriber::Registry>,
        > + Send
        + Sync
        + 'static,
{
    if !params.skip_color_eyre {
        init_color_eyre().map_err(InitError::ColorEyre)?;
    }
    tracing::init(params.tracing).map_err(InitError::Tracing)?;
    Ok(())
}

/// Perform common global initialization.
pub fn init() -> Result<(), InitError> {
    init_with(Params::new())
}
