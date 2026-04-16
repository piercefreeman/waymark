use either::Either;
use waymark_ids::WorkflowVersionId;

use crate::WorkflowRegistryBackend;

impl<Left, Right> WorkflowRegistryBackend for Either<Left, Right>
where
    Left: WorkflowRegistryBackend,
    Right: WorkflowRegistryBackend,
{
    fn upsert_workflow_version<'a>(
        &'a self,
        registration: &'a crate::WorkflowRegistration,
    ) -> impl Future<Output = waymark_backends_core::BackendResult<WorkflowVersionId>> + Send + 'a
    {
        match self {
            Either::Left(inner) => Either::Left(inner.upsert_workflow_version(registration)),
            Either::Right(inner) => Either::Right(inner.upsert_workflow_version(registration)),
        }
    }

    fn get_workflow_versions<'a>(
        &'a self,
        ids: &'a [WorkflowVersionId],
    ) -> impl Future<Output = waymark_backends_core::BackendResult<Vec<crate::WorkflowVersion>>>
    + Send
    + 'a {
        match self {
            Either::Left(inner) => Either::Left(inner.get_workflow_versions(ids)),
            Either::Right(inner) => Either::Right(inner.get_workflow_versions(ids)),
        }
    }
}
