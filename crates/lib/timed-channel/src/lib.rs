pub mod tokio {
    pub mod mpsc;

    pub mod sync {
        #[deprecated = "lose the `sync` mod"]
        pub use super::mpsc;
    }
}

pub mod std {
    pub mod mpsc;
}

pub trait What {
    fn description() -> &'static str;
}

#[macro_export]
macro_rules! named_what {
    ($name:ident, $desc:expr) => {
        #[derive(Debug)]
        pub struct $name;

        impl $crate::What for $name {
            fn description() -> &'static str {
                $desc
            }
        }
    };
}

pub fn wrap<Sender, Receiver, What>(
    channels: (Sender, Receiver),
) -> (Tx<Sender>, Rx<Receiver, What>) {
    let (sender, receiver) = channels;
    (Tx::new(sender), Rx::new(receiver))
}

pub struct Tx<C> {
    pub channel: C,
}

pub struct Rx<C, What> {
    pub channel: C,
    pub phantom_data: core::marker::PhantomData<What>,
}

impl<C> core::ops::Deref for Tx<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.channel
    }
}

impl<C> core::ops::DerefMut for Tx<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.channel
    }
}

impl<C, What> core::ops::Deref for Rx<C, What> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.channel
    }
}

impl<C, What> core::ops::DerefMut for Rx<C, What> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.channel
    }
}

impl<C> Tx<C> {
    pub const fn new(channel: C) -> Self {
        Self { channel }
    }
}

impl<C, What> Rx<C, What> {
    pub const fn new(channel: C) -> Self {
        Self {
            channel,
            phantom_data: core::marker::PhantomData,
        }
    }
}

impl<C: Clone> Clone for Tx<C> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
        }
    }
}

impl<C: Clone, What> Clone for Rx<C, What> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            phantom_data: core::marker::PhantomData,
        }
    }
}

impl<C, What> Rx<C, What> {
    pub fn tag<NewWhat>(self) -> Rx<C, NewWhat> {
        let Self {
            channel,
            phantom_data: _,
        } = self;
        Rx {
            channel,
            phantom_data: core::marker::PhantomData,
        }
    }
}

impl<C, What: self::What> Rx<C, What> {
    fn measure_opaque_item<T>(item: waymark_timed::Opaque<T>) -> T {
        waymark_timed::Opaque::into_inner_measured(item, What::description())
    }
}
