use crate::{Rx, Tx};

pub type Sender<T> = Tx<tokio::sync::mpsc::Sender<waymark_timed::Opaque<T>>>;
pub type Receiver<T, What = ()> = Rx<tokio::sync::mpsc::Receiver<waymark_timed::Opaque<T>>, What>;
pub type UnboundedSender<T> = Tx<tokio::sync::mpsc::UnboundedSender<waymark_timed::Opaque<T>>>;
pub type UnboundedReceiver<T, What = ()> =
    Rx<tokio::sync::mpsc::UnboundedReceiver<waymark_timed::Opaque<T>>, What>;

pub fn channel<T>(buffer: usize) -> (Sender<T>, Receiver<T, ()>) {
    crate::wrap(tokio::sync::mpsc::channel(buffer))
}

pub fn unbounded_channel<T>() -> (UnboundedSender<T>, UnboundedReceiver<T, ()>) {
    crate::wrap(tokio::sync::mpsc::unbounded_channel())
}

impl<T> Sender<T> {
    pub async fn send(&self, value: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        self.channel
            .send(waymark_timed::Opaque::wrap(value))
            .await
            .map_err(|val| {
                let tokio::sync::mpsc::error::SendError(timed) = val;
                tokio::sync::mpsc::error::SendError(timed.into_inner())
            })
    }
}

impl<T, What: crate::What> Receiver<T, What> {
    pub async fn recv(&mut self) -> Option<T> {
        self.channel
            .recv()
            .await
            .map(|item| waymark_timed::Opaque::into_inner_measured(item, What::description()))
    }

    pub fn try_recv(&mut self) -> Result<T, tokio::sync::mpsc::error::TryRecvError> {
        self.channel
            .try_recv()
            .map(|item| waymark_timed::Opaque::into_inner_measured(item, What::description()))
    }
}

impl<T> UnboundedSender<T> {
    pub fn send(&self, value: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        self.channel
            .send(waymark_timed::Opaque::wrap(value))
            .map_err(|val| {
                let tokio::sync::mpsc::error::SendError(timed) = val;
                tokio::sync::mpsc::error::SendError(timed.into_inner())
            })
    }
}

impl<T, What: crate::What> UnboundedReceiver<T, What> {
    pub async fn recv(&mut self) -> Option<T> {
        self.channel
            .recv()
            .await
            .map(|item| waymark_timed::Opaque::into_inner_measured(item, What::description()))
    }

    pub fn try_recv(&mut self) -> Result<T, tokio::sync::mpsc::error::TryRecvError> {
        self.channel
            .try_recv()
            .map(|item| waymark_timed::Opaque::into_inner_measured(item, What::description()))
    }
}
