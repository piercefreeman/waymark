//! Chrome-trace file output.

use tracing_subscriber::Registry;

use crate::ObservabilityOptions;

/// Flushes and finalizes the chrome trace file on drop, if one was being
/// written; returned by [`crate::tracing_layer`].
///
/// Bind it for the duration of `main` so the trace survives error
/// returns — the trace tail is otherwise lost precisely on the runs
/// where it would help diagnose the failure.
#[must_use = "the chrome trace is only flushed when this guard is dropped; bind it for the duration of the run"]
pub struct FlushOnDrop(Option<tracing_chrome::FlushGuard>);

impl Drop for FlushOnDrop {
    fn drop(&mut self) {
        self.0.take();
    }
}

pub(crate) fn layer(
    options: &ObservabilityOptions,
) -> (Option<tracing_chrome::ChromeLayer<Registry>>, FlushOnDrop) {
    let Some(path) = options.chrome_trace_path.as_ref() else {
        return (None, FlushOnDrop(None));
    };
    let (layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
        .file(path.clone())
        .build();
    eprintln!("tracing-chrome enabled (trace at {path})");
    (Some(layer), FlushOnDrop(Some(guard)))
}
