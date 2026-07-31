//! Per-VM sleep effect handler — durably records wake deadlines.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use nonempty_collections::NEVec;
use waymark_nonzero_duration::NonZeroDuration;
use waymark_sleep_reconciler_backend::{RecordSleeps, SleepRecord};
use waymark_vm_runtime_effect::EffectNumber;
use waymark_vm_runtime_promise_core::PromiseStateId;

/// Handles sleep effects for one VM by durably recording their wake
/// deadlines.
///
/// Implements [`waymark_extcall_reconciler_core::SleepEffectHandler`]:
/// the absolute deadline is computed once, at first record, and stored;
/// a revival replay re-records the same `(vm_id, promise_state_id)` key
/// and is silently ignored by the backend, so the original deadline
/// stands — re-emitted sleep effects must not walk the deadline forward.
pub struct EffectHandler<Backend>
where
    Backend: waymark_sleep_reconciler_backend::HasVmId,
{
    /// The durable sleeps backend to record through.
    pub backend: Arc<Backend>,

    /// The VM this handler records sleeps for.
    pub vm_id: Backend::VmId,
}

impl<Backend> waymark_extcall_reconciler_core::SleepEffectHandler for EffectHandler<Backend>
where
    Backend: RecordSleeps<Timestamp = chrono::DateTime<chrono::Utc>>,
    Backend: Send + Sync,
    Backend::VmId: Clone + Send + Sync,
{
    type Error = <Backend as RecordSleeps>::Error;

    async fn record_sleep(
        &mut self,
        effect_number: EffectNumber,
        promise_state_id: PromiseStateId,
        duration: NonZeroDuration,
    ) -> Result<(), Self::Error> {
        let wake_at = chrono::Utc::now()
            + chrono::Duration::from_std(duration.get()).unwrap_or(chrono::Duration::MAX);
        tracing::debug!(?promise_state_id, ?wake_at, "recording sleep");
        let record = SleepRecord {
            vm_id: self.vm_id.clone(),
            promise_state_id,
            effect_number,
            wake_at,
        };
        self.backend
            .record_sleeps(NEVec::new(record).as_nonempty_slice())
            .await
    }
}
