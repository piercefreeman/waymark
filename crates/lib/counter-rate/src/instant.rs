/// The instant a counter reading was taken.
pub trait Instant: Copy + PartialOrd {
    /// Seconds from `earlier` to `self`.
    fn seconds_since(self, earlier: Self) -> f64;
}

#[cfg(feature = "chrono")]
impl Instant for chrono::DateTime<chrono::Utc> {
    fn seconds_since(self, earlier: Self) -> f64 {
        (self - earlier).as_seconds_f64()
    }
}

impl Instant for std::time::Instant {
    fn seconds_since(self, earlier: Self) -> f64 {
        self.duration_since(earlier).as_secs_f64()
    }
}

impl Instant for std::time::SystemTime {
    fn seconds_since(self, earlier: Self) -> f64 {
        match self.duration_since(earlier) {
            Ok(duration) => duration.as_secs_f64(),
            Err(error) => -error.duration().as_secs_f64(),
        }
    }
}
