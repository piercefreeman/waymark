//! Background scheduler task.
//!
//! This task periodically polls for due schedules and queues new workflow instances.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tracing::{debug, error, info};
use uuid::Uuid;

use super::types::{ScheduleId, WorkflowSchedule};
use crate::backends::{CoreBackend, QueuedInstance, SchedulerBackend};
use crate::messages;
use crate::messages::ast as ir;
use waymark_dag::DAG;

#[derive(Clone)]
pub struct WorkflowDag {
    pub version_id: Uuid,
    pub dag: Arc<DAG>,
}

pub type DagResolver = Arc<dyn Fn(&str) -> Option<WorkflowDag> + Send + Sync>;

/// Configuration for the scheduler task.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// How often to poll for due schedules.
    pub poll_interval: Duration,
    /// Maximum number of schedules to process per poll.
    pub batch_size: i32,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            batch_size: 100,
        }
    }
}

/// Background scheduler task.
pub struct SchedulerTask<B> {
    pub backend: B,
    pub config: SchedulerConfig,
    /// Function to get the DAG for a workflow.
    /// This should look up the workflow definition and return its DAG.
    pub dag_resolver: DagResolver,
}

impl<B> SchedulerTask<B>
where
    B: CoreBackend + SchedulerBackend + Clone + Send + Sync + 'static,
{
    /// Run the scheduler loop.
    pub async fn run(self, shutdown: tokio_util::sync::WaitForCancellationFutureOwned) {
        info!(
            poll_interval_ms = self.config.poll_interval.as_millis(),
            batch_size = self.config.batch_size,
            "scheduler task started"
        );

        let mut shutdown = std::pin::pin!(shutdown);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("scheduler task shutting down");
                    break;
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    if let Err(e) = self.poll_and_fire().await {
                        error!(error = ?e, "scheduler poll failed");
                    }
                }
            }
        }
    }

    /// Poll for due schedules and fire them.
    async fn poll_and_fire(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let schedules = self
            .backend
            .find_due_schedules(self.config.batch_size)
            .await?;

        if schedules.is_empty() {
            return Ok(());
        }

        debug!(count = schedules.len(), "found due schedules");

        for schedule in schedules {
            if let Err(e) = self.fire_schedule(&schedule).await {
                error!(
                    schedule_id = %schedule.id,
                    workflow_name = %schedule.workflow_name,
                    error = ?e,
                    "failed to fire schedule"
                );
                // Skip to next run time to avoid retrying immediately
                if let Err(skip_err) = self
                    .backend
                    .skip_schedule_run(ScheduleId(schedule.id))
                    .await
                {
                    error!(error = ?skip_err, "failed to skip schedule run");
                }
            }
        }

        Ok(())
    }

    /// Fire a single schedule by creating a workflow instance.
    async fn fire_schedule(
        &self,
        schedule: &WorkflowSchedule,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Check for duplicates if not allowed
        if !schedule.allow_duplicate
            && self
                .backend
                .has_running_instance(ScheduleId(schedule.id))
                .await?
        {
            debug!(
                schedule_id = %schedule.id,
                "skipping schedule due to running instance"
            );
            return self
                .backend
                .skip_schedule_run(ScheduleId(schedule.id))
                .await
                .map_err(|e| e.into());
        }

        // Get the DAG for this workflow
        let workflow = (self.dag_resolver)(&schedule.workflow_name).ok_or_else(|| {
            format!(
                "no workflow version found for workflow: {}",
                schedule.workflow_name
            )
        })?;
        let dag = workflow.dag;

        // Find the entry node
        let entry_node_str = dag
            .entry_node
            .as_ref()
            .ok_or_else(|| "DAG has no entry node".to_string())?;

        let mut state = crate::waymark_core::runner::RunnerState::new(
            Some(Arc::clone(&dag)),
            None,
            None,
            false,
        );
        if let Some(input_payload) = schedule.input_payload.as_deref() {
            let inputs = messages::workflow_arguments_to_json(input_payload)
                .ok_or_else(|| "failed to decode schedule input payload".to_string())?;
            let Value::Object(input_map) = inputs else {
                return Err("schedule input payload must decode to an object".into());
            };
            for (name, value) in input_map {
                let expr = literal_from_json_value(&value);
                let label = format!("input {name} = {value}");
                state
                    .record_assignment(vec![name.clone()], &expr, None, Some(label))
                    .map_err(|err| err.0)?;
            }
        }
        let entry_exec = state
            .queue_template_node(entry_node_str, None)
            .map_err(|err| err.0)?;

        // Create a queued instance
        let instance_id = Uuid::new_v4();
        let queued = QueuedInstance {
            workflow_version_id: workflow.version_id,
            schedule_id: Some(schedule.id),
            dag: None,
            entry_node: entry_exec.node_id,
            state: Some(state),
            action_results: HashMap::new(),
            instance_id,
            scheduled_at: None,
        };

        // Queue the instance
        self.backend.queue_instances(&[queued]).await?;

        // Mark the schedule as executed
        self.backend
            .mark_schedule_executed(ScheduleId(schedule.id), instance_id)
            .await?;

        info!(
            schedule_id = %schedule.id,
            instance_id = %instance_id,
            workflow_name = %schedule.workflow_name,
            "fired scheduled workflow"
        );

        Ok(())
    }
}

fn literal_from_json_value(value: &Value) -> ir::Expr {
    match value {
        Value::Bool(value) => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::BoolValue(*value)),
            })),
            span: None,
        },
        Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::IntValue(value)),
                    })),
                    span: None,
                }
            } else {
                ir::Expr {
                    kind: Some(ir::expr::Kind::Literal(ir::Literal {
                        value: Some(ir::literal::Value::FloatValue(
                            number.as_f64().unwrap_or(0.0),
                        )),
                    })),
                    span: None,
                }
            }
        }
        Value::String(value) => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::StringValue(value.clone())),
            })),
            span: None,
        },
        Value::Array(items) => ir::Expr {
            kind: Some(ir::expr::Kind::List(ir::ListExpr {
                elements: items.iter().map(literal_from_json_value).collect(),
            })),
            span: None,
        },
        Value::Object(map) => {
            let entries = map
                .iter()
                .map(|(key, value)| ir::DictEntry {
                    key: Some(literal_from_json_value(&Value::String(key.clone()))),
                    value: Some(literal_from_json_value(value)),
                })
                .collect();
            ir::Expr {
                kind: Some(ir::expr::Kind::Dict(ir::DictExpr { entries })),
                span: None,
            }
        }
        Value::Null => ir::Expr {
            kind: Some(ir::expr::Kind::Literal(ir::Literal {
                value: Some(ir::literal::Value::IsNone(true)),
            })),
            span: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use chrono::{Duration as ChronoDuration, Utc};
    use prost::Message;
    use serde_json::Value;

    use super::*;
    use crate::backends::{CoreBackend, LockClaim, MemoryBackend, SchedulerBackend};
    use crate::messages::proto;
    use crate::scheduler::{CreateScheduleParams, ScheduleType};
    use crate::waymark_core::ir_parser::parse_program;
    use crate::waymark_core::runner::RunnerExecutor;
    use waymark_dag::convert_to_dag;

    fn workflow_args_payload(key: &str, value: i64) -> Vec<u8> {
        proto::WorkflowArguments {
            arguments: vec![proto::WorkflowArgument {
                key: key.to_string(),
                value: Some(proto::WorkflowArgumentValue {
                    kind: Some(proto::workflow_argument_value::Kind::Primitive(
                        proto::PrimitiveWorkflowArgument {
                            kind: Some(proto::primitive_workflow_argument::Kind::IntValue(value)),
                        },
                    )),
                }),
            }],
        }
        .encode_to_vec()
    }

    #[tokio::test]
    async fn scheduler_fire_schedule_applies_input_payload_to_state() {
        let queue = Arc::new(Mutex::new(VecDeque::new()));
        let backend = MemoryBackend::with_queue(queue);

        let source = r#"
fn main(input: [number], output: [result]):
    result = @double(value=number)
    return result
"#;
        let program = parse_program(source.trim()).expect("parse program");
        let dag = Arc::new(convert_to_dag(&program).expect("convert dag"));
        let workflow_name = "scheduled_math".to_string();
        let resolver_name = workflow_name.clone();
        let resolver_dag = Arc::clone(&dag);
        let dag_resolver: DagResolver = Arc::new(move |name: &str| {
            if name == resolver_name {
                Some(WorkflowDag {
                    version_id: Uuid::new_v4(),
                    dag: Arc::clone(&resolver_dag),
                })
            } else {
                None
            }
        });

        let scheduler = SchedulerTask {
            backend: backend.clone(),
            config: SchedulerConfig::default(),
            dag_resolver,
        };
        SchedulerBackend::upsert_schedule(
            &backend,
            &CreateScheduleParams {
                workflow_name: workflow_name.clone(),
                schedule_name: "default".to_string(),
                schedule_type: ScheduleType::Interval,
                cron_expression: None,
                interval_seconds: Some(60),
                jitter_seconds: 0,
                input_payload: Some(workflow_args_payload("number", 7)),
                priority: 0,
                allow_duplicate: false,
            },
        )
        .await
        .expect("upsert schedule");
        let schedule = SchedulerBackend::get_schedule_by_name(&backend, &workflow_name, "default")
            .await
            .expect("get schedule by name")
            .expect("schedule exists");

        scheduler
            .fire_schedule(&schedule)
            .await
            .expect("fire schedule");

        let claim = LockClaim {
            lock_uuid: Uuid::new_v4(),
            lock_expires_at: Utc::now() + ChronoDuration::seconds(30),
        };
        let batch = CoreBackend::get_queued_instances(&backend, 1, claim)
            .await
            .expect("claim queued");
        assert_eq!(batch.instances.len(), 1);

        let queued = &batch.instances[0];
        assert_eq!(queued.schedule_id, Some(schedule.id));
        let state = queued.state.clone().expect("queued state");
        let mut executor =
            RunnerExecutor::new(Arc::clone(&dag), state, queued.action_results.clone(), None);
        let replay = crate::waymark_core::runner::replay_variables(
            executor.state(),
            executor.action_results(),
        )
        .expect("replay inputs");
        assert_eq!(
            replay.variables.get("number"),
            Some(&Value::Number(7.into()))
        );

        let step = executor.increment(&[queued.entry_node]).expect("increment");
        assert_eq!(step.actions.len(), 1);
    }
}
