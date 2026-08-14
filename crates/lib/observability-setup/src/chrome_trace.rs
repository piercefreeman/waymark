//! Chrome-trace file output.

use std::sync::OnceLock;

use tracing_chrome::FlushGuard;
use tracing_subscriber::Registry;

use crate::ObservabilityOptions;

static TRACE_GUARD: OnceLock<std::sync::Mutex<Option<FlushGuard>>> = OnceLock::new();

fn store_trace_guard(guard: FlushGuard) {
    let cell = TRACE_GUARD.get_or_init(|| std::sync::Mutex::new(None));
    let mut slot = cell.lock().expect("trace guard lock poisoned");
    *slot = Some(guard);
}

pub(crate) fn flush() {
    if let Some(cell) = TRACE_GUARD.get() {
        let mut slot = cell.lock().expect("trace guard lock poisoned");
        slot.take();
    }
}

pub(crate) fn layer(
    options: &ObservabilityOptions,
) -> Option<tracing_chrome::ChromeLayer<Registry>> {
    options.chrome_trace_path.as_ref().map(|path| {
        let (layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
            .file(path.clone())
            .build();
        eprintln!("tracing-chrome enabled (trace at {path})");
        store_trace_guard(guard);
        layer
    })
}
