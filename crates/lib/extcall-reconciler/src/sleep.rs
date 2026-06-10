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

use crate::Ack;

/// Pending sleeps grouped by deadline, earliest first.
type PendingSleeps<VmId> = BTreeMap<Instant, Vec<(VmId, PromiseStateId)>>;

/// Records sleep deadlines.
pub struct Handler<VmId> {
    /// Channel to the poller.
    pub tx: mpsc::UnboundedSender<(VmId, PromiseStateId, Instant)>,
}

impl<VmId> Handler<VmId> {
    /// Record a sleep effect.
    pub fn record(&self, vm_id: VmId, promise_state_id: PromiseStateId, duration: NonZeroDuration) {
        let deadline = Instant::now() + duration.get();
        tracing::debug!(?promise_state_id, ?deadline, "recording sleep");
        let _ = self.tx.send((vm_id, promise_state_id, deadline));
    }
}

/// Always-active polling handle for sleep deadlines.
pub struct Poller<VmId> {
    /// Receives new sleep deadlines from the handler.
    pub rx: mpsc::UnboundedReceiver<(VmId, PromiseStateId, Instant)>,
    /// Pending sleeps grouped by deadline, earliest first.
    pub pending: PendingSleeps<VmId>,
}

impl<VmId> Poller<VmId>
where
    VmId: core::fmt::Debug,
{
    /// Wait for the next batch of elapsed sleep settlements.
    pub async fn poll<Converter, Value>(&mut self) -> Option<NEVec<PromiseSettlement<Value, Ack>>>
    where
        Converter: waymark_convert_core::Convert<serde_json::Value, Value>,
    {
        loop {
            // Drain any new sleeps from the channel.
            while let Ok((vm_id, promise_state_id, deadline)) = self.rx.try_recv() {
                self.pending
                    .entry(deadline)
                    .or_default()
                    .push((vm_id, promise_state_id));
            }

            if let Some(settlements) = collect_elapsed::<VmId, Converter, Value>(&mut self.pending)
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

                Some((vm_id, promise_state_id, dl)) = self.rx.recv() => {
                    self.pending.entry(dl).or_default().push((vm_id, promise_state_id));
                }
                else => return None,
            }
        }
    }
}

/// Create a paired sleep handler and poller.
pub(crate) fn new<VmId>() -> (Handler<VmId>, Poller<VmId>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let handler = Handler { tx };
    let poller = Poller {
        rx,
        pending: BTreeMap::new(),
    };
    (handler, poller)
}

/// Move any elapsed sleeps into settlements.
fn collect_elapsed<VmId, Converter, Value>(
    pending: &mut PendingSleeps<VmId>,
) -> Option<NEVec<PromiseSettlement<Value, Ack>>>
where
    Converter: waymark_convert_core::Convert<serde_json::Value, Value>,
    VmId: core::fmt::Debug,
{
    let now = Instant::now();
    let mut settlements = Vec::new();
    while let Some(entry) = pending.first_entry() {
        if *entry.key() > now {
            break;
        }
        let (_, targets) = entry.remove_entry();
        for (vm_id, promise_state_id) in targets {
            tracing::debug!(?vm_id, ?promise_state_id, "sleep elapsed");
            settlements.push(PromiseSettlement {
                promise_state_id,
                resolution: PromiseResolution::Resolved(Converter::convert(
                    serde_json::Value::Null,
                )),
                ack: Ack::Sleep,
            });
        }
    }
    NEVec::try_from_vec(settlements)
}

/// Return the earliest sleep deadline, if any.
fn earliest_deadline<VmId>(pending: &PendingSleeps<VmId>) -> Option<Instant> {
    pending.first_key_value().map(|(k, _)| *k)
}
