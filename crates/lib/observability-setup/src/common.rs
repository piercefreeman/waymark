#[derive(Clone, Debug, Default)]
pub struct ObservabilityOptions {
    pub console: bool,
    pub trace_path: Option<String>,
}
