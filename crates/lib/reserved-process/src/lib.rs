use std::time::Duration;

pub struct SpawnParams {
    pub command: tokio::process::Command,
    pub wait_for_playload_timeout: Duration,
    pub graceful_shutdown_timeout: Duration,
    pub kill_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("spawn: {0}")]
    Spawn(std::io::Error),

    #[error("timed out after waiting for worker to attach")]
    ReservationWaitTimeout { timeout: Duration },

    #[error("reservation wait: {0}")]
    ReservationWaitError(waymark_worker_reservation::ReservationCancelledError),
}

pub async fn spawn<Payload>(
    reservation: waymark_worker_reservation::Reservation<Payload>,
    params: SpawnParams,
) -> Result<(waymark_managed_process::Child, Payload), SpawnError> {
    let SpawnParams {
        wait_for_playload_timeout,
        command,
        graceful_shutdown_timeout,
        kill_timeout,
    } = params;

    // Spawn the process.
    let child = waymark_managed_process::spawn(command).map_err(SpawnError::Spawn)?;

    // Wait for the worker to connect (with timeout).
    let result = tokio::time::timeout(wait_for_playload_timeout, reservation.wait()).await;
    let result = match result {
        Ok(Ok(channels)) => Ok(channels),
        Ok(Err(err)) => Err(SpawnError::ReservationWaitError(err)),
        Err(tokio::time::error::Elapsed { .. }) => Err(SpawnError::ReservationWaitTimeout {
            timeout: wait_for_playload_timeout,
        }),
    };

    // Pass the payload or terminate the process gracefully.
    let payload = match result {
        Ok(channels) => channels,
        Err(error) => {
            let shutdown_result = child
                .shutdown(graceful_shutdown_timeout, kill_timeout)
                .await;
            tracing::debug!(
                ?error,
                ?shutdown_result,
                "reserved process shut down after failed payload wait"
            );
            return Err(error);
        }
    };

    tracing::info!("reserved process connected");

    Ok((child, payload))
}
