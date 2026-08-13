//! Observability setup for managing process-global observability state.
//!
//! Provides the extra tracing layer a binary can inject via the
//! filter-bypassing slot of `waymark_fn_main_common::tracing::Params`
//! (these layers do their own filtering).  Each layer lives
//! behind a rustc cfg flag, so only builds that enable them link the
//! corresponding dependencies:
//!
//! - `waymark_observability_chrome_trace` — chrome-trace file output via
//!   `tracing-chrome`.
//! - `waymark_observability_tokio_console` — the `tokio-console` layer via
//!   `console-subscriber`.  Note: the console only receives data when
//!   `tokio` itself is built with `--cfg tokio_unstable`; the cfg makes it
//!   compile, not observable.
//!
//! With a cfg disabled, requesting the corresponding option prints a note
//! and yields a no-op layer.

#![warn(missing_docs)]

use tracing_subscriber::{Layer, Registry};

/// What observability extras to enable.
#[derive(Clone, Debug, Default)]
pub struct ObservabilityOptions {
    /// Serve the `tokio-console` layer.
    pub tokio_console: bool,

    /// Write a chrome-trace file to this path.
    pub chrome_trace_path: Option<String>,
}

#[cfg(waymark_observability_chrome_trace)]
mod chrome_trace;

#[cfg(not(waymark_observability_chrome_trace))]
mod chrome_trace {
    use super::ObservabilityOptions;

    /// The no-op stand-in for the chrome-trace flush guard in builds
    /// with chrome tracing disabled; returned by [`crate::tracing_layer`].
    #[must_use = "the chrome trace is only flushed when this guard is dropped; bind it for the duration of the run"]
    pub struct FlushOnDrop;

    pub(crate) fn layer(
        options: &ObservabilityOptions,
    ) -> (Option<tracing_subscriber::layer::Identity>, FlushOnDrop) {
        if options.chrome_trace_path.is_some() {
            eprintln!(
                "chrome tracing disabled. Rebuild with \
                 `--cfg waymark_observability_chrome_trace` to enable it."
            );
        }
        (None, FlushOnDrop)
    }
}

#[cfg(waymark_observability_tokio_console)]
mod tokio_console;

#[cfg(not(waymark_observability_tokio_console))]
mod tokio_console {
    use super::ObservabilityOptions;

    pub(crate) fn layer(
        options: &ObservabilityOptions,
    ) -> Option<tracing_subscriber::layer::Identity> {
        if options.tokio_console {
            eprintln!(
                "tokio-console disabled. Rebuild with \
                 `--cfg waymark_observability_tokio_console` to enable it."
            );
        }
        None
    }
}

pub use chrome_trace::FlushOnDrop;

/// Build the extra tracing layer for the requested observability
/// options, along with the [`FlushOnDrop`] guard that finalizes the
/// chrome trace.
///
/// Construction is effectful where the option demands it: the chrome
/// layer opens its trace file, and the console layer spawns its server.
pub fn tracing_layer(
    options: &ObservabilityOptions,
) -> (impl Layer<Registry> + Send + Sync + 'static, FlushOnDrop) {
    let (chrome_layer, flush_on_drop) = chrome_trace::layer(options);
    // The trait form: `Option`'s inherent `and_then` shadows the
    // `Layer` combinator on the method call syntax.
    let layer = Layer::and_then(chrome_layer, tokio_console::layer(options));
    (layer, flush_on_drop)
}
