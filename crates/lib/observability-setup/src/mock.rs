use crate::ObservabilityOptions;

pub fn init(_options: ObservabilityOptions) {
    eprintln!("Tracing disabled. Rebuild with `--cfg waymark_observability_trace`.");
}

pub fn flush() {}
