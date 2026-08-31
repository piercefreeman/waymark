//! The scheduler's firing loop.
//!
//! A single background task polls the backend for due schedules and
//! spawns their runs: for each due row it decodes the schedule's
//! definition, computes the advanced run cursor, mints a fresh VM id,
//! and hands the whole batch to the backend's fenced registration —
//! which advances each schedule past this occurrence and registers a VM
//! runtime from its baked initial snapshot, reporting a per-row
//! [`Outcome`](waymark_scheduler_backend::register_scheduled_vm_runtimes::Outcome).
//!
//! Any number of loops may run concurrently: claiming happens inside
//! the registration statement, fenced on the run cursor, so a competing
//! loop's rows simply come back
//! [`Superseded`](waymark_scheduler_backend::register_scheduled_vm_runtimes::Outcome::Superseded)
//! — the healthy shape of the race, logged at debug.
//!
//! Rows that cannot be processed — an undecodable definition blob, or a
//! definition with no producible next occurrence — are skipped with a
//! warning and stay due; after registration-time validation such rows
//! are unreachable short of data corruption or the cron evaluation
//! horizon, so no pausing machinery exists for them.
//!
//! Backend failures do not stop the loop outright: the registration
//! statement is atomic, so nothing of a failed batch landed and its
//! rows stay due — a failed poll or registration is logged and retried
//! next interval.  Only [`MAX_CONSECUTIVE_FAILURES`]
//! consecutive failures of the same statement stop the loop with the
//! error; the binary turns that into a loud process exit, because a
//! scheduler that cannot fire must crash visibly, never idle while
//! schedules silently stay due.

#![warn(missing_docs)]

#[cfg(test)]
mod tests;

use std::num::NonZeroUsize;
use std::sync::Arc;

use waymark_nonzero_duration::NonZeroDuration;
use waymark_scheduler_backend::{PollDueSchedules, RegisterScheduledVmRuntimes};

/// How many consecutive failures of the same backend statement the
/// firing loop tolerates — logging and retrying next interval — before
/// it stops with the error.  Spaced by the poll interval, this rides
/// out a transient database blip (a failover, a deadlock burst) but
/// not a persistent failure.
pub const MAX_CONSECUTIVE_FAILURES: usize = 5;

/// Error returned when the firing loop stops on persistent backend
/// failure — [`MAX_CONSECUTIVE_FAILURES`] consecutive failures of the
/// same statement.
#[derive(Debug, thiserror::Error)]
pub enum Error<PollError, RegisterError> {
    /// The backend failed to poll for due schedules.
    #[error("polling due schedules: {0}")]
    Poll(#[source] PollError),

    /// The backend failed to register the batch of scheduled VM
    /// runtimes. The statement is atomic, so nothing of the batch
    /// landed; the rows stay due and a healthy loop would pick them up
    /// again.
    #[error("registering scheduled VM runtimes: {0}")]
    Register(#[source] RegisterError),
}

/// Shorthand for an [`Error`] using the associated types of `T`.
pub type ErrorFor<T> =
    Error<<T as PollDueSchedules>::Error, <T as RegisterScheduledVmRuntimes>::Error>;

/// Parameters for [`run`].
pub struct Params<Backend, Codec, MintVmIdFn> {
    /// The scheduler backend to poll and register through.
    pub backend: Arc<Backend>,

    /// Decodes the schedules' definition blobs.
    pub codec: Arc<Codec>,

    /// Mints the id for each spawned VM runtime.
    pub mint_vm_id_fn: MintVmIdFn,

    /// How long the loop waits between polls that find less than a full
    /// batch. A poll that fills the batch is followed by another poll
    /// immediately, draining a backlog without waiting.
    pub poll_interval: NonZeroDuration,

    /// The most due schedules a single poll fetches (and a single
    /// registration statement carries).
    pub max_items: NonZeroUsize,
}

/// Poll for due schedules and spawn their runs until persistent
/// backend failure.
///
/// Drive this in a background task. Isolated backend failures are
/// logged and retried next interval — the batch statement is atomic,
/// so a failed batch's rows simply stay due.  The loop
/// stops only after [`MAX_CONSECUTIVE_FAILURES`] consecutive failures
/// of the same statement; the caller must treat that as fatal.
pub async fn run<Backend, Codec, MintVmIdFn>(
    params: Params<Backend, Codec, MintVmIdFn>,
) -> Result<std::convert::Infallible, ErrorFor<Backend>>
where
    Backend: PollDueSchedules,
    Backend: RegisterScheduledVmRuntimes,
    Backend: waymark_scheduler_backend::HasTimestamp<Timestamp = chrono::DateTime<chrono::Utc>>,
    Codec: waymark_vm_codec_core::DeserializerProvider,
    MintVmIdFn: Fn() -> <<Backend as waymark_scheduler_backend::HasVmId>::VmId as ToOwned>::Owned,
    <Backend as waymark_scheduler_backend::HasVmId>::VmId: ToOwned,
    <Backend as PollDueSchedules>::Error: core::fmt::Display,
    <Backend as RegisterScheduledVmRuntimes>::Error: core::fmt::Display,
{
    let Params {
        backend,
        codec,
        mint_vm_id_fn,
        poll_interval,
        max_items,
    } = params;

    // Failures are tracked per statement: a healthy poll must not
    // vouch for a registration that keeps failing, or a broken
    // registration path with a healthy poll would retry forever.
    let mut consecutive_poll_failures = 0usize;
    let mut consecutive_register_failures = 0usize;

    loop {
        // Each error value is returned or logged-and-dropped inside its
        // match arm, before the retry sleeps below — keeping the future
        // Send without bounding the backend error types.
        let now = chrono::Utc::now();
        let polled = match backend.poll_due_schedules(now, max_items).await {
            Ok(polled) => {
                consecutive_poll_failures = 0;
                polled
            }
            Err(error) => {
                consecutive_poll_failures += 1;
                if consecutive_poll_failures >= MAX_CONSECUTIVE_FAILURES {
                    return Err(Error::Poll(error));
                }
                tracing::warn!(%error, "polling due schedules failed; will retry");
                // The retry shares the idle path's sleep below.
                None
            }
        };

        let Some(due) = polled else {
            tokio::time::sleep(poll_interval.get()).await;
            continue;
        };

        let full_batch = due.len() == max_items;
        let registered =
            match process_due_batch(&*backend, &*codec, &mint_vm_id_fn, now, &due).await {
                Ok(()) => {
                    consecutive_register_failures = 0;
                    true
                }
                Err(error) => {
                    consecutive_register_failures += 1;
                    if consecutive_register_failures >= MAX_CONSECUTIVE_FAILURES {
                        return Err(Error::Register(error));
                    }
                    tracing::warn!(
                        %error,
                        "registering scheduled VM runtimes failed; the rows stay due, will retry"
                    );
                    false
                }
            };
        if !registered {
            tokio::time::sleep(poll_interval.get()).await;
            continue;
        }

        // A full batch may leave a backlog behind — drain it before
        // waiting out the interval.
        if !full_batch {
            tokio::time::sleep(poll_interval.get()).await;
        }
    }
}

/// Spawn one polled batch: decode, compute, mint, register, trace.
///
/// Rows whose definition cannot be decoded or whose next occurrence
/// cannot be produced are skipped with a warning; they stay due. If
/// every row was skipped there is nothing to register.
async fn process_due_batch<Backend, Codec, MintVmIdFn>(
    backend: &Backend,
    codec: &Codec,
    mint_vm_id_fn: &MintVmIdFn,
    now: chrono::DateTime<chrono::Utc>,
    due: &nonempty_collections::NEVec<
        waymark_scheduler_backend::poll_due_schedules::DueScheduleFor<Backend>,
    >,
) -> Result<(), <Backend as RegisterScheduledVmRuntimes>::Error>
where
    Backend: RegisterScheduledVmRuntimes,
    Backend: waymark_scheduler_backend::HasTimestamp<Timestamp = chrono::DateTime<chrono::Utc>>,
    Codec: waymark_vm_codec_core::DeserializerProvider,
    MintVmIdFn: Fn() -> <<Backend as waymark_scheduler_backend::HasVmId>::VmId as ToOwned>::Owned,
    <Backend as waymark_scheduler_backend::HasVmId>::VmId: ToOwned,
{
    let mut items = Vec::with_capacity(due.len().get());
    for row in due.iter() {
        let decoded_definition = codec.with_deserializer(&row.definition, |deserializer| {
            serde::Deserialize::deserialize(deserializer)
        });
        let definition = match decoded_definition {
            Ok(definition) => definition,
            Err(err) => {
                tracing::warn!(
                    schedule_name = row.schedule_name,
                    ?err,
                    "undecodable schedule definition; skipping the row"
                );
                continue;
            }
        };

        let new_next_run_at = match waymark_scheduler_core::compute_next_run(&definition, now) {
            Ok(Some(new_next_run_at)) => new_next_run_at,
            Ok(None) => {
                tracing::warn!(
                    schedule_name = row.schedule_name,
                    "schedule definition has no occurrences; skipping the row"
                );
                continue;
            }
            Err(err) => {
                tracing::warn!(
                    schedule_name = row.schedule_name,
                    ?err,
                    "no producible next run; skipping the row"
                );
                continue;
            }
        };

        items.push(
            waymark_scheduler_backend::register_scheduled_vm_runtimes::Item {
                schedule_name: std::borrow::Cow::Borrowed(row.schedule_name.as_str()),
                expected_next_run_at: std::borrow::Cow::Borrowed(&row.next_run_at),
                vm_id: std::borrow::Cow::Owned(mint_vm_id_fn()),
                new_next_run_at: std::borrow::Cow::Owned(new_next_run_at),
                check_overlap: !definition.allow_duplicate,
            },
        );
    }

    let Some(items) = nonempty_collections::NEVec::try_from_vec(items) else {
        return Ok(());
    };

    let outcomes = backend
        .register_scheduled_vm_runtimes(items.as_nonempty_slice())
        .await?;

    for (item, outcome) in items.iter().zip(outcomes) {
        let schedule_name = item.schedule_name.as_ref();
        match outcome {
            waymark_scheduler_backend::register_scheduled_vm_runtimes::Outcome::Registered => {
                tracing::info!(schedule_name, "spawned a scheduled VM runtime");
            }
            waymark_scheduler_backend::register_scheduled_vm_runtimes::Outcome::SkippedOverlap => {
                tracing::info!(
                    schedule_name,
                    "previous instance still running; skipped this occurrence"
                );
            }
            waymark_scheduler_backend::register_scheduled_vm_runtimes::Outcome::Superseded => {
                tracing::debug!(
                    schedule_name,
                    "another registrar took this occurrence; nothing to do"
                );
            }
        }
    }

    Ok(())
}
