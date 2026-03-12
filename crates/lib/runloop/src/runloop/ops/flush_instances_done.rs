use waymark_core_backend::InstanceDone;

use crate::runloop::RunLoopError;

pub async fn flush_instances_done<CoreBackend>(
    core_backend: &CoreBackend,
    pending: &mut Vec<InstanceDone>,
) -> Result<(), RunLoopError>
where
    CoreBackend: ?Sized + waymark_core_backend::CoreBackend,
{
    if pending.is_empty() {
        return Ok(());
    }
    let batch = std::mem::take(pending);
    core_backend.save_instances_done(&batch).await?;
    Ok(())
}
