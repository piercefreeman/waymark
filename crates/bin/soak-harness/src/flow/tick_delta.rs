#[derive(Debug)]
pub struct TickDelta(tokio::time::Instant);

impl TickDelta {
    pub const fn new(instant: tokio::time::Instant) -> Self {
        Self(instant)
    }

    pub fn tick(&mut self, instant: tokio::time::Instant) -> std::time::Duration {
        let old_now = std::mem::replace(&mut self.0, instant);
        self.0.saturating_duration_since(old_now)
    }
}
