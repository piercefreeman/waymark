pub fn transparent<T>(data: T) -> Transparent<T> {
    Transparent::wrap(data)
}

pub fn opaque<T>(data: T) -> Opaque<T> {
    Opaque::wrap(data)
}

#[derive(Debug)]
pub struct Transparent<T> {
    data: T,
    created_at: std::time::Instant,
}

#[derive(Debug)]
pub struct Opaque<T>(Transparent<T>);

impl<T> Opaque<T> {
    pub fn wrap(data: T) -> Self {
        Self(Transparent::wrap(data))
    }

    pub fn into_transparent(self) -> Transparent<T> {
        self.0
    }

    pub fn unpack(self) -> (T, std::time::Instant) {
        Transparent::unpack(self.0)
    }

    pub fn into_inner(self) -> T {
        Transparent::into_inner(self.0)
    }

    pub fn since_creation(&self) -> std::time::Duration {
        Transparent::since_creation(&self.0)
    }

    pub fn created_at(&self) -> std::time::Instant {
        Transparent::created_at(&self.0)
    }

    #[cfg(feature = "metrics")]
    pub fn into_inner_measured(self, what: &'static str) -> T {
        Transparent::into_inner_measured(self.0, what)
    }

    #[cfg(feature = "metrics")]
    pub fn into_inner_measured_multi(self, what: &[&'static str]) -> T {
        Transparent::into_inner_measured_multi(self.0, what)
    }
}

impl<T> Transparent<T> {
    pub fn wrap(data: T) -> Self {
        Self {
            data,
            created_at: std::time::Instant::now(),
        }
    }

    pub fn into_opaque(self) -> Opaque<T> {
        Opaque(self)
    }

    pub fn unpack(value: Self) -> (T, std::time::Instant) {
        (value.data, value.created_at)
    }

    pub fn into_inner(value: Self) -> T {
        value.data
    }

    pub fn since_creation(value: &Self) -> std::time::Duration {
        value.created_at.elapsed()
    }

    pub fn created_at(value: &Self) -> std::time::Instant {
        value.created_at
    }

    #[cfg(feature = "metrics")]
    pub fn into_inner_measured(value: Self, what: &'static str) -> T {
        Self::into_inner_measured_multi(value, &[what])
    }

    #[cfg(feature = "metrics")]
    pub fn into_inner_measured_multi(value: Self, what: &[&'static str]) -> T {
        let elapsed = Self::since_creation(&value);

        for what in what {
            metrics::histogram!("waymark_timed_seconds", "what" => *what).record(elapsed);
        }

        Self::into_inner(value)
    }
}

impl<T> core::ops::Deref for Transparent<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> core::ops::DerefMut for Transparent<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T> From<Opaque<T>> for Transparent<T> {
    fn from(value: Opaque<T>) -> Self {
        Opaque::into_transparent(value)
    }
}

impl<T> From<Transparent<T>> for Opaque<T> {
    fn from(value: Transparent<T>) -> Self {
        Transparent::into_opaque(value)
    }
}

impl<T> From<T> for Opaque<T> {
    fn from(value: T) -> Self {
        Self::wrap(value)
    }
}
