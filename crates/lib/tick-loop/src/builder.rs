#[derive(Debug)]
pub struct Builder {
    cancellation_token: tokio_util::sync::CancellationToken,
    tick_interval: Option<tokio::time::Interval>,
}

impl Builder {
    pub fn with_tick_interval(
        mut self,
        tick_interval: impl Into<Option<tokio::time::Interval>>,
    ) -> Self {
        self.tick_interval = tick_interval.into();
        self
    }

    pub fn params<TickFn>(self, tick_fn: TickFn) -> crate::Params<TickFn> {
        let Self {
            cancellation_token,
            tick_interval,
        } = self;

        crate::Params {
            cancellation_token,
            tick_interval,
            tick_fn,
        }
    }
}

pub fn new(cancellation_token: tokio_util::sync::CancellationToken) -> Builder {
    Builder {
        cancellation_token,
        tick_interval: None,
    }
}
