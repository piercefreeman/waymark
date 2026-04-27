use std::sync::Arc;

use slotmap::SlotMap;

use crate::Reservation;

pub(crate) type ActiveReservations<Payload> =
    SlotMap<crate::Id, tokio::sync::oneshot::Sender<Payload>>;

/// Tracks outstanding worker reservations until their payloads arrive.
pub struct Registry<Payload> {
    active_reservations: std::sync::Mutex<ActiveReservations<Payload>>,
}

impl<Payload> Default for Registry<Payload> {
    fn default() -> Self {
        Self {
            active_reservations: Default::default(),
        }
    }
}

/// Errors that can occur while completing a reservation.
#[derive(Debug, thiserror::Error)]
pub enum RegisterError<Payload> {
    /// The reservation no longer exists, usually because it was already
    /// dropped or completed.
    #[error("reservation {reservation_id} not found")]
    ReservationNotFound {
        /// The requested reservation ID.
        reservation_id: crate::Id,

        /// The payload that could not be delivered.
        payload: Payload,
    },

    /// The reservation existed, but the waiting side was already gone when sending.
    #[error("unable to send payload for reservation {reservation_id}")]
    PayloadSend {
        /// The requested reservation ID.
        reservation_id: crate::Id,

        /// The payload that could not be delivered.
        payload: Payload,
    },
}

impl<Payload> Registry<Payload> {
    /// Creates a new reservation that can later be completed
    /// with [`Self::register`].
    pub fn reserve(self: &Arc<Self>) -> Reservation<Payload> {
        let mut active_reservations = self.active_reservations.lock().unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel();

        let reservation_id = active_reservations.insert(tx);

        let registry = Arc::clone(self);

        Reservation::issue_from_registry(registry, reservation_id, rx)
    }

    /// Completes a reservation by delivering its payload to the waiting handle.
    ///
    /// Returns [`RegisterError::ReservationNotFound`] if the reservation no longer
    /// exists, or [`RegisterError::PayloadSend`] if the receiver was already gone.
    pub fn register(
        &self,
        reservation_id: crate::Id,
        payload: Payload,
    ) -> Result<(), RegisterError<Payload>> {
        let mut active_reservations = self.active_reservations.lock().unwrap();

        let Some(tx) = active_reservations.remove(reservation_id) else {
            return Err(RegisterError::ReservationNotFound {
                reservation_id,
                payload,
            });
        };

        tx.send(payload)
            .map_err(|payload| RegisterError::PayloadSend {
                reservation_id,
                payload,
            })
    }

    pub(crate) fn reservation_drop_cleanup(&self, reservation_id: crate::Id) {
        let mut active_reservations = self.active_reservations.lock().unwrap();
        let _ = active_reservations.remove(reservation_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    async fn register_then_wait_delivers_payload() {
        let registry = Arc::new(Registry::default());

        let reservation = registry.reserve();
        let reservation_id = reservation.id();

        registry.register(reservation_id, "hello").unwrap();

        let payload = reservation.wait().await.unwrap();
        assert_eq!(payload, "hello");
    }

    #[tokio::test]
    async fn dropping_reservation_cleans_up_registry_entry() {
        let registry = Arc::new(Registry::default());

        let reservation = registry.reserve();
        let reservation_id = reservation.id();
        drop(reservation);

        let result = registry.register(reservation_id, "payload");

        match result {
            Err(RegisterError::ReservationNotFound {
                reservation_id: id,
                payload,
            }) => {
                assert_eq!(id, reservation_id);
                assert_eq!(payload, "payload");
            }
            other => panic!("expected ReservationNotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn multiple_reservations_are_isolated() {
        let registry = Arc::new(Registry::default());

        let first = registry.reserve();
        let first_id = first.id();
        let second = registry.reserve();
        let second_id = second.id();

        registry.register(second_id, "second").unwrap();
        registry.register(first_id, "first").unwrap();

        assert_eq!(first.wait().await.unwrap(), "first");
        assert_eq!(second.wait().await.unwrap(), "second");
    }
}
