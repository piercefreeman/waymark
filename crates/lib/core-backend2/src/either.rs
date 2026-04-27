use either::Either;

use crate::common::*;
use crate::traits::*;

impl<Left, Right> InstanceEnqueue for Either<Left, Right>
where
    Left: InstanceEnqueue,
    Right: InstanceEnqueue<Error = Left::Error, Metadata = Left::Metadata>,
{
    type Error = Left::Error;
    type Metadata = Left::Metadata;

    fn enqueue_instances<'a>(
        &'a self,
        instances: impl Iterator<Item = EnqueueInstancesItem<Self::Metadata>> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        match self {
            Either::Left(inner) => Either::Left(inner.enqueue_instances(instances)),
            Either::Right(inner) => Either::Right(inner.enqueue_instances(instances)),
        }
    }
}

impl<Left, Right> InstanceQueuePoll for Either<Left, Right>
where
    Left: InstanceQueuePoll,
    Right: InstanceQueuePoll<Error = Left::Error>,
{
    type Error = Left::Error;

    fn poll_queued_instances(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        claim: LockClaim,
        max_instances: std::num::NonZeroUsize,
    ) -> impl Future<Output = Result<nonempty_collections::NEVec<PolledInstance>, Self::Error>> + Send + '_
    {
        match self {
            Either::Left(inner) => {
                Either::Left(inner.poll_queued_instances(now, claim, max_instances))
            }
            Either::Right(inner) => {
                Either::Right(inner.poll_queued_instances(now, claim, max_instances))
            }
        }
    }
}

impl<Left, Right> InstanceQueueCompletion for Either<Left, Right>
where
    Left: InstanceQueueCompletion,
    Right: InstanceQueueCompletion<Error = Left::Error>,
{
    type Error = Left::Error;

    fn save_instances_done<'a>(
        &'a self,
        instances: impl nonempty_collections::IntoNonEmptyIterator<Item = InstanceDone> + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        match self {
            Either::Left(inner) => Either::Left(inner.save_instances_done(instances)),
            Either::Right(inner) => Either::Right(inner.save_instances_done(instances)),
        }
    }
}

impl<Left, Right> InstanceQueueLocksKeepalive for Either<Left, Right>
where
    Left: InstanceQueueLocksKeepalive,
    Right: InstanceQueueLocksKeepalive<Error = Left::Error>,
{
    type Error = Left::Error;

    fn refresh_instance_locks<'a>(
        &'a self,
        claim: LockClaim,
        instance_ids: impl nonempty_collections::IntoNonEmptyIterator<Item = waymark_ids::InstanceId>
        + 'a,
    ) -> impl Future<Output = Result<nonempty_collections::NEVec<InstanceLockStatus>, Self::Error>>
    + Send
    + 'a {
        match self {
            Either::Left(inner) => Either::Left(inner.refresh_instance_locks(claim, instance_ids)),
            Either::Right(inner) => {
                Either::Right(inner.refresh_instance_locks(claim, instance_ids))
            }
        }
    }
}

impl<Left, Right> InstanceQueueLocksRelease for Either<Left, Right>
where
    Left: InstanceQueueLocksRelease,
    Right: InstanceQueueLocksRelease<Error = Left::Error>,
{
    type Error = Left::Error;

    fn release_instance_locks<'a>(
        &'a self,
        lock_id: waymark_ids::LockId,
        instance_ids: impl nonempty_collections::IntoNonEmptyIterator<Item = waymark_ids::InstanceId>
        + 'a,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        match self {
            Either::Left(inner) => {
                Either::Left(inner.release_instance_locks(lock_id, instance_ids))
            }
            Either::Right(inner) => {
                Either::Right(inner.release_instance_locks(lock_id, instance_ids))
            }
        }
    }
}

impl<Left, Right> ExecutionStatePersistence for Either<Left, Right>
where
    Left: ExecutionStatePersistence,
    Right: ExecutionStatePersistence<Error = Left::Error>,
{
    type Error = Left::Error;

    fn save_graphs<'a>(
        &'a self,
        claim: LockClaim,
        graph_updates: impl nonempty_collections::IntoNonEmptyIterator<Item = GraphUpdate> + 'a,
    ) -> impl Future<Output = Result<nonempty_collections::NEVec<InstanceLockStatus>, Self::Error>>
    + Send
    + 'a {
        match self {
            Either::Left(inner) => Either::Left(inner.save_graphs(claim, graph_updates)),
            Either::Right(inner) => Either::Right(inner.save_graphs(claim, graph_updates)),
        }
    }
}

impl<Left, Right> ActionsDonePersistence for Either<Left, Right>
where
    Left: ActionsDonePersistence,
    Right: ActionsDonePersistence<Error = Left::Error, Metadata = Left::Metadata>,
{
    type Error = Left::Error;

    type Metadata = Left::Metadata;

    fn save_actions_done<'a>(
        &'a self,
        actions: impl nonempty_collections::IntoNonEmptyIterator<Item = ActionDone<Self::Metadata>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a {
        match self {
            Either::Left(inner) => Either::Left(inner.save_actions_done(actions)),
            Either::Right(inner) => Either::Right(inner.save_actions_done(actions)),
        }
    }
}

impl<Left, Right> Error for Either<Left, Right>
where
    Left: Error,
    Right: Error,
{
    fn kind(&self) -> ErrorKind {
        match self {
            Either::Left(inner) => inner.kind(),
            Either::Right(inner) => inner.kind(),
        }
    }
}
