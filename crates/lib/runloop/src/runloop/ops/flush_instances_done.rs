use waymark_core_backend::InstanceDone;

use crate::runloop::RunLoopError;

pub struct Params<'a, CoreBackend: ?Sized> {
    pub core_backend: &'a CoreBackend,
    pub pending: &'a mut Vec<InstanceDone>,
}

/// Persists completed or failed instances to the backend.
///
/// Once instances complete (success or failure), they are staged in a pending buffer.
/// This operation atomically flushes that buffer to the backend for durability.
/// The buffer is cleared after successful persistence.
///
/// This ensures that the runloop's in-memory instance state accurately reflects
/// the authoritative backend state, even if the runloop crashes.
pub async fn run<CoreBackend>(params: Params<'_, CoreBackend>) -> Result<(), RunLoopError>
where
    CoreBackend: ?Sized + waymark_core_backend::CoreBackend,
{
    let Params {
        core_backend,
        pending,
    } = params;

    if pending.is_empty() {
        return Ok(());
    }
    let batch = std::mem::take(pending);
    core_backend.save_instances_done(&batch).await?;
    Ok(())
}
