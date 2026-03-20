use std::sync::Arc;

use crate::Registry;

/// A reservation handle that allows to wait for payload.
pub struct Reservation<Payload> {
    registry: Arc<Registry<Payload>>,
    id: crate::Id,
    rx: Option<tokio::sync::oneshot::Receiver<Payload>>,
}

/// Error returned when a reservation is cancelled before receiving a payload.
#[derive(Debug, thiserror::Error)]
#[error("reservation {reservation_id} was cancelled")]
pub struct ReservationCancelledError {
    /// The reservation that was dropped or otherwise cancelled before
    /// a payload arrived.
    pub reservation_id: crate::Id,
}

impl<Payload> Reservation<Payload> {
    pub(crate) fn issue_from_registry(
        registry: Arc<Registry<Payload>>,
        id: crate::Id,
        rx: tokio::sync::oneshot::Receiver<Payload>,
    ) -> Self {
        Self {
            registry,
            id,
            rx: Some(rx),
        }
    }

    /// Returns the identifier that the registry uses to complete
    /// this reservation.
    pub fn id(&self) -> crate::Id {
        self.id
    }

    /// Waits for the payload to be sent for this reservation.
    ///
    /// Returns [`ReservationCancelledError`] if the reservation is dropped
    /// or the sender disappears before delivering a payload.
    pub async fn wait(mut self) -> Result<Payload, ReservationCancelledError> {
        let channel_rx = self.rx.take().unwrap(); // only ever consumed here
        match channel_rx.await {
            Ok(val) => Ok(val),
            Err(_) => Err(ReservationCancelledError {
                reservation_id: self.id,
            }),
        }
    }
}

impl<Payload> Drop for Reservation<Payload> {
    fn drop(&mut self) {
        if let Some(rx) = &mut self.rx {
            rx.close()
        }
        self.registry.reservation_drop_cleanup(self.id);
    }
}
