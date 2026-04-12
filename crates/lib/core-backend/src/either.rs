use either::Either;

use crate::CoreBackend;

impl<Left, Right> CoreBackend for Either<Left, Right>
where
    Left: CoreBackend,
    Right: CoreBackend<PollQueuedInstancesError = Left::PollQueuedInstancesError>,
{
    fn save_graphs<'a>(
        &'a self,
        claim: crate::LockClaim,
        graphs: &'a [crate::GraphUpdate],
    ) -> impl Future<Output = waymark_backends_core::BackendResult<Vec<crate::InstanceLockStatus>>>
    + Send
    + 'a {
        match self {
            Either::Left(inner) => Either::Left(inner.save_graphs(claim, graphs)),
            Either::Right(inner) => Either::Right(inner.save_graphs(claim, graphs)),
        }
    }

    fn save_actions_done<'a>(
        &'a self,
        actions: &'a [crate::ActionDone],
    ) -> impl Future<Output = waymark_backends_core::BackendResult<()>> + Send + 'a {
        match self {
            Either::Left(inner) => Either::Left(inner.save_actions_done(actions)),
            Either::Right(inner) => Either::Right(inner.save_actions_done(actions)),
        }
    }

    type PollQueuedInstancesError = Left::PollQueuedInstancesError;

    fn poll_queued_instances(
        &self,
        size: std::num::NonZeroUsize,
        claim: crate::LockClaim,
    ) -> impl Future<
        Output = Result<
            nonempty_collections::NEVec<crate::QueuedInstance>,
            Self::PollQueuedInstancesError,
        >,
    > + Send
    + '_ {
        match self {
            Either::Left(inner) => Either::Left(inner.poll_queued_instances(size, claim)),
            Either::Right(inner) => Either::Right(inner.poll_queued_instances(size, claim)),
        }
    }

    fn refresh_instance_locks<'a>(
        &'a self,
        claim: crate::LockClaim,
        instance_ids: &'a [waymark_ids::InstanceId],
    ) -> impl Future<Output = waymark_backends_core::BackendResult<Vec<crate::InstanceLockStatus>>>
    + Send
    + 'a {
        match self {
            Either::Left(inner) => Either::Left(inner.refresh_instance_locks(claim, instance_ids)),
            Either::Right(inner) => {
                Either::Right(inner.refresh_instance_locks(claim, instance_ids))
            }
        }
    }

    fn release_instance_locks<'a>(
        &'a self,
        lock_uuid: waymark_ids::LockId,
        instance_ids: &'a [waymark_ids::InstanceId],
    ) -> impl Future<Output = waymark_backends_core::BackendResult<()>> + Send + 'a {
        match self {
            Either::Left(inner) => {
                Either::Left(inner.release_instance_locks(lock_uuid, instance_ids))
            }
            Either::Right(inner) => {
                Either::Right(inner.release_instance_locks(lock_uuid, instance_ids))
            }
        }
    }

    fn save_instances_done<'a>(
        &'a self,
        instances: &'a [crate::InstanceDone],
    ) -> impl Future<Output = waymark_backends_core::BackendResult<()>> + Send + 'a {
        match self {
            Either::Left(inner) => Either::Left(inner.save_instances_done(instances)),
            Either::Right(inner) => Either::Right(inner.save_instances_done(instances)),
        }
    }

    fn queue_instances<'a>(
        &'a self,
        instances: &'a [crate::QueuedInstance],
    ) -> impl Future<Output = waymark_backends_core::BackendResult<()>> + Send + 'a {
        match self {
            Either::Left(inner) => Either::Left(inner.queue_instances(instances)),
            Either::Right(inner) => Either::Right(inner.queue_instances(instances)),
        }
    }
}
