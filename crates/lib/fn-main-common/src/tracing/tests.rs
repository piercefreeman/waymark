//! Unit tests for the tracing composition: the `RUST_LOG` parsing
//! fallbacks, the absent layer's composition-neutrality (it must never
//! veto or clamp sibling layers), the max-level collapse when both slots
//! are empty, and which events reach each slot of the composed
//! subscriber.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::meaningful_targets::MeaningfulTargets;
use super::{NO_EXTRA_LAYER, Params, subscriber, unwrap_fmt_filter};

/// A placeholder error type for readings that don't fail.
type NoReadError = &'static str;

/// A `RUST_LOG` filter as if the variable was unset.
fn default_fmt_filter() -> tracing_subscriber::filter::Targets {
    unwrap_fmt_filter::<NoReadError>(Ok(MeaningfulTargets::info_default()))
}

/// Counts the events delivered to the slot it occupies.
struct EventCounter(Arc<AtomicUsize>);

impl<Subscriber> tracing_subscriber::Layer<Subscriber> for EventCounter
where
    Subscriber: tracing::Subscriber,
{
    fn on_event(
        &self,
        _event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, Subscriber>,
    ) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn rust_log_unset_defaults_to_info() {
    let filter = default_fmt_filter();
    assert!(filter.would_enable("anything", &tracing::Level::INFO));
    assert!(!filter.would_enable("anything", &tracing::Level::DEBUG));
}

#[test]
fn rust_log_meaningless_values_fail_to_parse() {
    // The empty string would "parse" into a bare-ERROR filter that
    // near-silently disables fmt output (and whitespace parses into
    // whatever the junk means); both are rejected before parsing. A
    // realistic env-filter-syntax value genuinely fails `Targets`
    // parsing.
    for raw in ["", "   ", "foo[{bar=3}]=trace"] {
        assert!(raw.parse::<MeaningfulTargets>().is_err(), "raw: {raw:?}");
    }
}

#[test]
fn rust_log_read_error_defaults_to_info() {
    // Every meaningless value funnels here via the parse error; upstream
    // `fmt::try_init` instead runs a malformed value as an all-off
    // filter, silently disabling all fmt output — we deliberately
    // diverge.
    let filter = unwrap_fmt_filter(Err("value is not a valid unicode"));
    assert!(filter.would_enable("anything", &tracing::Level::INFO));
    assert!(!filter.would_enable("anything", &tracing::Level::DEBUG));
}

#[test]
fn rust_log_directives_are_honored() {
    let targets = "warn,waymark=debug"
        .parse::<MeaningfulTargets>()
        .expect("directives parse");
    let filter = unwrap_fmt_filter::<NoReadError>(Ok(targets));
    assert!(!filter.would_enable("anything", &tracing::Level::INFO));
    assert!(filter.would_enable("anything", &tracing::Level::WARN));
    assert!(filter.would_enable("waymark", &tracing::Level::DEBUG));
}

#[test]
fn no_extra_layer_matches_the_absent_option_layer() {
    type Registry = tracing_subscriber::Registry;
    type AbsentOption = Option<tracing_subscriber::layer::Identity>;

    assert_eq!(
        tracing_subscriber::Layer::<Registry>::max_level_hint(&NO_EXTRA_LAYER),
        tracing_subscriber::Layer::<Registry>::max_level_hint(&AbsentOption::None),
    );

    // An `and_then` sibling's hint must not get clamped by the absent
    // layer's `OFF` hint — the delegated none-layer probe is what keeps
    // the pair unbounded, exactly like a real absent `Option` layer.
    let ours = tracing_subscriber::Layer::<Registry>::and_then(
        NO_EXTRA_LAYER,
        tracing_subscriber::layer::Identity::new(),
    );
    let baseline = tracing_subscriber::Layer::<Registry>::and_then(
        AbsentOption::None,
        tracing_subscriber::layer::Identity::new(),
    );
    let ours = tracing_subscriber::Layer::<Registry>::max_level_hint(&ours);
    assert_eq!(
        ours,
        tracing_subscriber::Layer::<Registry>::max_level_hint(&baseline),
    );
    assert_eq!(ours, None);
}

#[test]
fn absent_slots_collapse_to_the_fmt_filter_level() {
    let subscriber = subscriber(Params::new(), default_fmt_filter());
    assert_eq!(
        tracing::Subscriber::max_level_hint(&subscriber),
        Some(tracing_subscriber::filter::LevelFilter::INFO),
    );
}

#[test]
fn a_filter_bypassing_layer_uncaps_the_subscriber() {
    // The `Identity` stand-in has no filter of its own: with it in the
    // bypassing slot every callsite in the process stays enabled — the
    // documented cost of filling that slot.
    let subscriber = subscriber(
        Params {
            filter_bypassing_layer: tracing_subscriber::layer::Identity::new(),
            filter_wrapped_layer: NO_EXTRA_LAYER,
        },
        default_fmt_filter(),
    );
    assert_eq!(tracing::Subscriber::max_level_hint(&subscriber), None);
}

#[test]
fn filter_wrapped_slot_sits_under_the_fmt_filter() {
    let seen = Arc::new(AtomicUsize::new(0));
    let subscriber = subscriber(
        Params {
            filter_bypassing_layer: NO_EXTRA_LAYER,
            filter_wrapped_layer: EventCounter(Arc::clone(&seen)),
        },
        default_fmt_filter(),
    );
    tracing::subscriber::with_default(subscriber, || {
        tracing::debug!("below the filter");
        tracing::info!("at the filter");
    });
    assert_eq!(seen.load(Ordering::Relaxed), 1);
}

#[test]
fn filter_bypassing_slot_sees_below_the_fmt_filter() {
    let seen = Arc::new(AtomicUsize::new(0));
    let subscriber = subscriber(
        Params {
            filter_bypassing_layer: EventCounter(Arc::clone(&seen)),
            filter_wrapped_layer: NO_EXTRA_LAYER,
        },
        default_fmt_filter(),
    );
    tracing::subscriber::with_default(subscriber, || {
        tracing::debug!("below the filter");
        tracing::info!("at the filter");
    });
    assert_eq!(seen.load(Ordering::Relaxed), 2);
}
