use nonempty_collections::NEVec;

pub struct Backend<InnerBackend> {
    pub inner: InnerBackend,
    pub recorder: waymark_vcr_recorder::backend::Handle,
}

impl<InnerBackend> waymark_core_backend::CoreBackend for Backend<InnerBackend>
where
    InnerBackend: waymark_core_backend::CoreBackend,
    InnerBackend: Sync,
{
    fn save_graphs<'a>(
        &'a self,
        claim: waymark_core_backend::LockClaim,
        graphs: &'a [waymark_core_backend::GraphUpdate],
    ) -> impl Future<
        Output = waymark_backends_core::BackendResult<
            Vec<waymark_core_backend::InstanceLockStatus>,
        >,
    > {
        self.inner.save_graphs(claim, graphs)
    }

    fn save_actions_done<'a>(
        &'a self,
        actions: &'a [waymark_core_backend::ActionDone],
    ) -> impl Future<Output = waymark_backends_core::BackendResult<()>> {
        self.inner.save_actions_done(actions)
    }

    type PollQueuedInstancesError = InnerBackend::PollQueuedInstancesError;

    async fn poll_queued_instances(
        &self,
        size: std::num::NonZeroUsize,
        claim: waymark_core_backend::LockClaim,
    ) -> Result<NEVec<waymark_core_backend::QueuedInstance>, Self::PollQueuedInstancesError> {
        let polled_instances = self.inner.poll_queued_instances(size, claim).await?;

        for queued_instance in &polled_instances {
            if let Err(error) = self.recorder.open_instance_log(queued_instance.clone()) {
                tracing::warn!(message = "unable to open a queued instance log", ?error);
                break;
            }
        }

        Ok(polled_instances)
    }

    fn refresh_instance_locks<'a>(
        &'a self,
        claim: waymark_core_backend::LockClaim,
        instance_ids: &'a [waymark_ids::InstanceId],
    ) -> impl Future<
        Output = waymark_backends_core::BackendResult<
            Vec<waymark_core_backend::InstanceLockStatus>,
        >,
    > {
        self.inner.refresh_instance_locks(claim, instance_ids)
    }

    fn release_instance_locks<'a>(
        &'a self,
        lock_uuid: waymark_ids::LockId,
        instance_ids: &'a [waymark_ids::InstanceId],
    ) -> impl Future<Output = waymark_backends_core::BackendResult<()>> {
        self.inner.release_instance_locks(lock_uuid, instance_ids)
    }

    fn save_instances_done<'a>(
        &'a self,
        instances: &'a [waymark_core_backend::InstanceDone],
    ) -> impl Future<Output = waymark_backends_core::BackendResult<()>> {
        self.inner.save_instances_done(instances)
    }

    fn queue_instances<'a>(
        &'a self,
        instances: &'a [waymark_core_backend::QueuedInstance],
    ) -> impl Future<Output = waymark_backends_core::BackendResult<()>> {
        self.inner.queue_instances(instances)
    }
}
