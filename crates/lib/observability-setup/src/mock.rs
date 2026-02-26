use crate::ObservabilityOptions;

pub fn init(_options: ObservabilityOptions) {
    eprintln!("Tracing disabled. Rebuild with --features trace or --features observability.");
}

pub fn flush() {}
