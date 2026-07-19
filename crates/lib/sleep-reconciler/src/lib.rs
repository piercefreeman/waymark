//! Sleep extcall reconciler.
//!
//! Manages sleep deadline tracking and settlement generation.

#[cfg(test)]
mod tests;

use std::collections::btree_map::BTreeMap;
use std::time::Instant;

use nonempty_collections::NEVec;
use tokio::sync::mpsc;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_vm_driver_core::{PromiseResolution, PromiseSettlement};
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Create a paired sleep handler and poller.
///
/// `SleepValueProvider` supplies the value elapsed sleeps resolve with.
///
/// Set `skip_sleep` to true to force all sleeps to resolve immediately
/// (useful for testing and debugging).
pub fn new<SleepValueProvider>(skip_sleep: bool) -> (Handler, Poller<SleepValueProvider>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let handler = Handler { tx, skip_sleep };
    let poller = Poller {
        rx,
        pending: BTreeMap::new(),
        provider: std::marker::PhantomData,
    };
    (handler, poller)
}

/// Pending sleeps grouped by deadline, earliest first.
type PendingSleeps = BTreeMap<Instant, Vec<PromiseStateId>>;

/// Records sleep deadlines.
pub struct Handler {
    /// Channel to the poller.
    pub tx: mpsc::UnboundedSender<(PromiseStateId, Instant)>,

    /// When true, sleep deadlines are set to now (immediate) instead of
    /// now + duration, effectively skipping all sleeps.
    pub skip_sleep: bool,
}

impl Handler {
    /// Record a sleep effect.
    pub fn record(&self, promise_state_id: PromiseStateId, duration: NonZeroDuration) {
        let deadline = if self.skip_sleep {
            Instant::now()
        } else {
            Instant::now() + duration.get()
        };
        tracing::debug!(
            ?promise_state_id,
            ?deadline,
            skip_sleep = self.skip_sleep,
            "recording sleep"
        );
        let _ = self.tx.send((promise_state_id, deadline));
    }
}

impl waymark_extcall_reconciler_core::SleepEffectHandler for Handler {
    /// Recording a sleep is infallible — it only sends on an unbounded channel.
    type Error = std::convert::Infallible;

    async fn record_sleep(
        &mut self,
        promise_state_id: PromiseStateId,
        duration: NonZeroDuration,
    ) -> Result<(), Self::Error> {
        self.record(promise_state_id, duration);
        Ok(())
    }
}

/// Always-active polling handle for sleep deadlines.
pub struct Poller<SleepValueProvider> {
    /// Receives new sleep deadlines from the handler.
    pub rx: mpsc::UnboundedReceiver<(PromiseStateId, Instant)>,
    /// Pending sleeps grouped by deadline, earliest first.
    pub pending: PendingSleeps,
    /// The sleep value provider is purely type-level.
    provider: std::marker::PhantomData<fn() -> SleepValueProvider>,
}

pub struct Ack;

impl waymark_vm_driver_core::PromiseSettlementAck for Ack {
    fn acknowledge_promise_settlement(self) {}
}

impl<ActionAck> From<Ack> for waymark_extcall_reconciler_core::Ack<ActionAck, Ack> {
    fn from(value: Ack) -> Self {
        waymark_extcall_reconciler_core::Ack::Sleep(value)
    }
}

impl<SleepValueProvider> Poller<SleepValueProvider>
where
    SleepValueProvider: waymark_sleep_core::SleepValueProvider,
{
    /// Wait for the next batch of elapsed sleep settlements.
    pub async fn poll<Ack>(
        &mut self,
    ) -> Option<NEVec<PromiseSettlement<SleepValueProvider::Value, Ack>>>
    where
        Ack: From<self::Ack>,
    {
        loop {
            // Drain any new sleeps from the channel.
            while let Ok((promise_state_id, deadline)) = self.rx.try_recv() {
                self.pending
                    .entry(deadline)
                    .or_default()
                    .push(promise_state_id);
            }

            if let Some(settlements) = collect_elapsed::<SleepValueProvider, Ack>(&mut self.pending)
            {
                return Some(settlements);
            }

            let deadline = earliest_deadline(&self.pending);

            // Race the deadline against new-sleep arrivals — a shorter
            // sleep may be recorded while we wait for a longer one.
            tokio::select! {
                _ = async {
                    if let Some(d) = deadline {
                        tokio::time::sleep_until(d.into()).await;
                    }
                }, if deadline.is_some() => {}

                Some((promise_state_id, dl)) = self.rx.recv() => {
                    self.pending.entry(dl).or_default().push(promise_state_id);
                }
                else => return None,
            }
        }
    }
}

/// Error returned when polling for sleep settlements fails.
#[derive(Debug)]
pub enum PollSleepError {
    /// The sleep channel has been closed; no more sleeps will be recorded.
    ChannelClosed,
}

impl<SleepValueProvider> waymark_extcall_reconciler_core::SettlerAck
    for Poller<SleepValueProvider>
{
    type Ack = Ack;
}

impl<SleepValueProvider> waymark_extcall_reconciler_core::HasValue for Poller<SleepValueProvider>
where
    SleepValueProvider: waymark_sleep_core::SleepValueProvider,
{
    type Value = SleepValueProvider::Value;
}

impl<SleepValueProvider, UnifiedAck>
    waymark_extcall_reconciler_core::SleepPromiseSettler<UnifiedAck> for Poller<SleepValueProvider>
where
    SleepValueProvider: waymark_sleep_core::SleepValueProvider,
    UnifiedAck: From<Ack>,
{
    /// The error type returned when polling for sleep settlements fails.
    type Error = PollSleepError;

    async fn poll_sleep_settlements<'a>(
        &'a mut self,
        // Elapsed sleeps settle on their own deadlines; the demand set is
        // not consulted.
        _waiting_promise_state_ids: nonempty_collections::NESlice<
            'a,
            waymark_vm_runtime_promise_core::PromiseStateId,
        >,
    ) -> Result<NEVec<PromiseSettlement<Self::Value, UnifiedAck>>, Self::Error>
    where
        UnifiedAck: 'a,
    {
        self.poll::<UnifiedAck>()
            .await
            .ok_or(PollSleepError::ChannelClosed)
    }
}

/// Move any elapsed sleeps into settlements.
fn collect_elapsed<SleepValueProvider, Ack>(
    pending: &mut PendingSleeps,
) -> Option<NEVec<PromiseSettlement<SleepValueProvider::Value, Ack>>>
where
    SleepValueProvider: waymark_sleep_core::SleepValueProvider,
    Ack: From<self::Ack>,
{
    let now = Instant::now();
    let mut settlements = Vec::new();
    while let Some(entry) = pending.first_entry() {
        if *entry.key() > now {
            break;
        }
        let (_, targets) = entry.remove_entry();
        for promise_state_id in targets {
            tracing::debug!(?promise_state_id, "sleep elapsed");
            settlements.push(PromiseSettlement {
                promise_state_id,
                resolution: PromiseResolution::Resolved(SleepValueProvider::value()),
                ack: Ack::from(self::Ack),
            });
        }
    }
    NEVec::try_from_vec(settlements)
}

/// Return the earliest sleep deadline, if any.
fn earliest_deadline(pending: &PendingSleeps) -> Option<Instant> {
    pending.first_key_value().map(|(k, _)| *k)
}
