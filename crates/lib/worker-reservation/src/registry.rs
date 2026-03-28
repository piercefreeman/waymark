use std::sync::Arc;

use slotmap::SlotMap;

use crate::Reservation;

pub(crate) type ActiveReservations<Payload> =
    SlotMap<crate::Id, tokio::sync::oneshot::Sender<Payload>>;

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

#[derive(Debug, thiserror::Error)]
pub enum RegisterError<Payload> {
    #[error("reservation {reservation_id} not found")]
    ReservationNotFound {
        reservation_id: crate::Id,
        payload: Payload,
    },

    #[error("unable to send payload for reservation {reservation_id}")]
    PayloadSend {
        reservation_id: crate::Id,
        payload: Payload,
    },
}

impl<Payload> Registry<Payload> {
    pub fn reserve(self: &Arc<Self>) -> Reservation<Payload> {
        let mut active_reservations = self.active_reservations.lock().unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel();

        let reservation_id = active_reservations.insert(tx);

        let registry = Arc::clone(self);

        Reservation::issue_from_registry(registry, reservation_id, rx)
    }

    /// Complete the worker registration after receiving the handshake.
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
