pub struct MetricVal(pub usize);

impl metrics::IntoF64 for MetricVal {
    fn into_f64(self) -> f64 {
        self.0 as f64
    }
}
