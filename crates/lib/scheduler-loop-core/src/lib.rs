use std::future::Future;
use std::sync::Arc;

use uuid::Uuid;
use waymark_dag::DAG;

#[derive(Clone)]
pub struct WorkflowDag {
    pub version_id: Uuid,
    pub dag: Arc<DAG>,
}

pub trait DagResolver {
    type Error;

    fn resolve_dag<'a>(
        &'a self,
        workflow_name: &'a str,
    ) -> impl Future<Output = Result<Option<WorkflowDag>, Self::Error>> + Send + 'a;
}

impl<F, Fut, E> DagResolver for F
where
    F: Fn(&str) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Option<WorkflowDag>, E>> + Send + 'static,
    E: 'static,
{
    type Error = E;

    async fn resolve_dag<'a>(
        &'a self,
        workflow_name: &'a str,
    ) -> Result<Option<WorkflowDag>, Self::Error> {
        (self)(workflow_name).await
    }
}
