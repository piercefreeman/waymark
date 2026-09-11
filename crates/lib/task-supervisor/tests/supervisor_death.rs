//! The supervising task's own panic is re-raised by the drain, never
//! reported.
//!
//! Its own test binary: the supervising task renders every failed end
//! through `tracing`, and the callsite's interest is cached on first use,
//! so the rendering subscriber has to be in place before anything in this
//! process hits it.

/// A task error whose rendering panics: the supervising task renders every
/// failed end, so this one kills it.
#[derive(Debug)]
struct RenderingPanics;

impl std::fmt::Display for RenderingPanics {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("rendering panics");
    }
}

impl std::error::Error for RenderingPanics {}

/// A subscriber that renders every event's fields, as a real one does;
/// without one, `tracing` never formats them.
struct RenderingSubscriber;

impl tracing::Subscriber for RenderingSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        event.record(&mut RenderFields);
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

struct RenderFields;

impl tracing::field::Visit for RenderFields {
    fn record_debug(&mut self, _field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let _rendered = format!("{value:?}");
    }
}

#[tokio::test]
async fn the_drain_re_raises_the_supervisors_panic() {
    let _subscriber = tracing::subscriber::set_default(RenderingSubscriber);
    let shutdown_token = tokio_util::sync::CancellationToken::new();
    let mut supervisor = waymark_task_supervisor::start::<RenderingPanics>(shutdown_token.clone());

    supervisor.track(
        "renders and kills",
        tokio::spawn(async { Err::<std::convert::Infallible, _>(RenderingPanics) }),
    );

    let drain = tokio::time::timeout(std::time::Duration::from_secs(5), supervisor.drain());
    let unwound = {
        use futures_util::FutureExt as _;
        std::panic::AssertUnwindSafe(drain).catch_unwind().await
    };

    let payload = unwound.expect_err("the drain re-raises the panic");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"rendering panics"));
    assert!(shutdown_token.is_cancelled());
}
