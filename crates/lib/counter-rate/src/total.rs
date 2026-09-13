/// A cumulative counter's reading.
pub trait Total: Copy {
    /// Growth from `earlier` to `self`, saturating at zero — a counter
    /// reset reads as zero growth.
    fn delta_since(self, earlier: Self) -> Self;

    /// The value in rate math's domain.
    fn as_f64(self) -> f64;
}

impl Total for u64 {
    fn delta_since(self, earlier: Self) -> Self {
        self.saturating_sub(earlier)
    }

    fn as_f64(self) -> f64 {
        self as f64
    }
}

impl Total for u32 {
    fn delta_since(self, earlier: Self) -> Self {
        self.saturating_sub(earlier)
    }

    fn as_f64(self) -> f64 {
        self as f64
    }
}

impl Total for usize {
    fn delta_since(self, earlier: Self) -> Self {
        self.saturating_sub(earlier)
    }

    fn as_f64(self) -> f64 {
        self as f64
    }
}
