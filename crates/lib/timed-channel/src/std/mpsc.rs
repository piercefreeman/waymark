use crate::{Rx, Tx};

pub type Sender<T> = Tx<std::sync::mpsc::Sender<waymark_timed::Opaque<T>>>;
pub type Receiver<T, What = ()> = Rx<std::sync::mpsc::Receiver<waymark_timed::Opaque<T>>, What>;

pub fn channel<T>() -> (Sender<T>, Receiver<T, ()>) {
    crate::wrap(std::sync::mpsc::channel())
}

impl<T> Sender<T> {
    pub fn send(&self, value: T) -> Result<(), std::sync::mpsc::SendError<T>> {
        self.channel
            .send(waymark_timed::Opaque::wrap(value))
            .map_err(|val| {
                let std::sync::mpsc::SendError(timed) = val;
                std::sync::mpsc::SendError(timed.into_inner())
            })
    }
}

impl<T, What: crate::What> Receiver<T, What> {
    pub fn recv(&self) -> Result<T, std::sync::mpsc::RecvError> {
        self.channel
            .recv()
            .map(|item| waymark_timed::Opaque::into_inner_measured(item, What::description()))
    }
}
