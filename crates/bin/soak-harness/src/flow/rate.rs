use std::num::NonZeroU128;

pub struct Rate {
    pub amount: NonZeroU128,
    pub per: std::time::Duration,
}

impl Rate {
    pub fn per_minute(amount: impl Into<NonZeroU128>) -> Self {
        Self {
            amount: amount.into(),
            per: std::time::Duration::from_secs(60),
        }
    }

    pub fn for_delta(&self, delta: std::time::Duration) -> u128 {
        let quant = self.per.as_millis();
        let span = delta.as_millis();

        let Some(occurrences) = span.checked_div_euclid(quant) else {
            return 0;
        };

        occurrences * self.amount.get()
    }
}
