//! Expression evaluator for DAG-related operations.

use std::sync::Arc;

use waymark_dag::DAG;

pub trait SideEffectApplicator: waymark_runner_eval_core::SideEffectApplicator {}

#[derive(Debug)]
pub struct DagEvaluator<SideEffectApplicator> {
    /// The DAG to use.
    pub dag: Arc<DAG>,

    /// The applicator to apply side effects with.
    pub applicator: SideEffectApplicator,
}

impl<SideEffectApplicator> DagEvaluator<SideEffectApplicator> where
    SideEffectApplicator: self::SideEffectApplicator
{
}
