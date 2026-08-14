//! Tracing initialization for the process: subscriber composition with
//! explicit extra-layer slots.
//!
//! The subscriber is composed from three parts: the filter-bypassing
//! extra layer, then the filter-wrapped extra layer and the fmt output
//! together under the `RUST_LOG` filter (default `INFO`).  Binaries with
//! nothing to inject call [`init`] with [`Params::new`]; binaries with
//! extra layers fill the slots on [`Params`] directly.

use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;

use crate::InitTracingError;

mod meaningful_targets;
mod no_extra_layer;

#[cfg(test)]
mod tests;

pub use no_extra_layer::{NO_EXTRA_LAYER, NoExtraLayer};

/// The `RUST_LOG` filter for the fmt output, defaulting to `INFO`;
/// see [`unwrap_fmt_filter`].
fn fmt_filter() -> tracing_subscriber::filter::Targets {
    unwrap_fmt_filter(envfury::or_else(
        "RUST_LOG",
        meaningful_targets::MeaningfulTargets::info_default,
    ))
}

/// Unwraps a `RUST_LOG` env var reading into the fmt output filter,
/// falling back to the INFO default with a warning when the value could
/// not be meaningfully parsed — invalid unicode, a parse error, or an
/// empty/whitespace-only value (see
/// [`meaningful_targets::MeaningfulTargets`]).
///
/// The fallback deliberately diverges from what
/// [`tracing_subscriber::fmt::try_init`] builds without the `env-filter`
/// feature: upstream turns a malformed value into an empty filter (max
/// level `OFF`) and an empty value into a bare-ERROR one, silently
/// disabling all or nearly all fmt output either way.
fn unwrap_fmt_filter<ReadError>(
    read: Result<meaningful_targets::MeaningfulTargets, ReadError>,
) -> tracing_subscriber::filter::Targets
where
    ReadError: std::fmt::Display,
{
    match read {
        Ok(meaningful) => meaningful.0,
        Err(error) => {
            eprintln!("Ignoring `RUST_LOG`: {error}");
            meaningful_targets::MeaningfulTargets::info_default().0
        }
    }
}

/// The parameters for the global tracing subscriber initialization, with
/// a slot for each way an extra layer can relate to the `RUST_LOG`
/// filter.
///
/// Slots with nothing to inject take [`NO_EXTRA_LAYER`].
#[derive(Debug, Default)]
pub struct Params<FilterBypassingLayer, FilterWrappedLayer> {
    /// Composed outside the `RUST_LOG` filter: sees everything and does
    /// its own filtering (profilers, consoles).
    ///
    /// Filling this slot with a hint-less layer (profilers and consoles
    /// are) keeps every callsite in the process enabled — each
    /// `debug!`/`trace!` hit is dispatched and each span materialized,
    /// whether or not any layer ends up keeping it.
    pub filter_bypassing_layer: FilterBypassingLayer,

    /// Composed under the same `RUST_LOG` filter as the fmt output.
    ///
    /// While the filter-bypassing slot is empty, sub-filter callsites
    /// stay disabled at zero cost; a filled filter-bypassing slot keeps
    /// them enabled process-wide, and sub-filter events are then
    /// dispatched and dropped at the filter.
    pub filter_wrapped_layer: FilterWrappedLayer,
}

impl Params<NoExtraLayer, NoExtraLayer> {
    /// The [`Params`] with both extra-layer slots empty.
    pub fn new() -> Self {
        Self {
            filter_bypassing_layer: NO_EXTRA_LAYER,
            filter_wrapped_layer: NO_EXTRA_LAYER,
        }
    }
}

/// Composes the tracing subscriber described by the given parameters,
/// with the given `RUST_LOG` filter on the fmt output (and the
/// filter-wrapped slot).
fn subscriber<FilterBypassingLayer, FilterWrappedLayer>(
    params: Params<FilterBypassingLayer, FilterWrappedLayer>,
    fmt_filter: tracing_subscriber::filter::Targets,
) -> impl tracing::Subscriber + Send + Sync + 'static
where
    FilterBypassingLayer:
        tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
    FilterWrappedLayer: tracing_subscriber::Layer<
            tracing_subscriber::layer::Layered<FilterBypassingLayer, tracing_subscriber::Registry>,
        > + Send
        + Sync
        + 'static,
{
    use tracing_subscriber::Layer as _;

    // The trait form: `Option`'s inherent `and_then` shadows the
    // `Layer` combinator on the method call syntax.
    let filter_wrapped = tracing_subscriber::Layer::and_then(
        params.filter_wrapped_layer,
        tracing_subscriber::fmt::layer(),
    )
    .with_filter(fmt_filter);

    // The two slots must be stacked with two `with` calls, not
    // combined into one layer with `and_then`: the layer-over-layer
    // `Layered` folds max-level hints with a plain `Option` max,
    // where the bypassing layer's `None` ("no bound") loses to the
    // filter's `INFO` and the bypassing layer goes deaf below it.
    // Subscriber-level stacking treats `None` as unbounded.
    tracing_subscriber::registry()
        .with(params.filter_bypassing_layer)
        .with(filter_wrapped)
}

/// Initializes the global tracing subscriber for the process.
pub fn init<FilterBypassingLayer, FilterWrappedLayer>(
    params: Params<FilterBypassingLayer, FilterWrappedLayer>,
) -> Result<(), InitTracingError>
where
    FilterBypassingLayer:
        tracing_subscriber::Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
    FilterWrappedLayer: tracing_subscriber::Layer<
            tracing_subscriber::layer::Layered<FilterBypassingLayer, tracing_subscriber::Registry>,
        > + Send
        + Sync
        + 'static,
{
    subscriber(params, fmt_filter())
        .try_init()
        .map_err(|error| InitTracingError(Box::new(error)))
}
