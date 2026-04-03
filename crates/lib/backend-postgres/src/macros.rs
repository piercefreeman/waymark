#[macro_export]
macro_rules! query_timing_histogram {
    ($label:expr) => {{ $crate::PostgresBackend::query_timing_histogram(function_name!(), $label) }};
}
