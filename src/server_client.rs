//! Server/Client infrastructure for the Rappel workflow engine.
//!
//! This module provides:
//! - gRPC server for workflow registration and status (WorkflowService)
//! - gRPC health check for singleton discovery
//! - Configuration and server lifecycle management

use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use prost::Message;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic_health::server::health_reporter;
use tracing::{error, info};

use crate::{
    db::{Database, ScheduleType},
    ir_printer,
    messages::ast as ir_ast,
    messages::proto,
    schedule::{next_cron_run, next_interval_run},
};

/// Service name for health checks
pub const SERVICE_NAME: &str = "rappel";

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub grpc_addr: SocketAddr,
    pub database_url: String,
}

/// Run the gRPC server with health check and workflow services
pub async fn run_server(config: ServerConfig) -> Result<()> {
    let ServerConfig {
        grpc_addr,
        database_url,
    } = config;

    let database = Database::connect(&database_url)
        .await
        .with_context(|| format!("failed to connect to database at {}", database_url))?;

    let grpc_listener = TcpListener::bind(grpc_addr)
        .await
        .with_context(|| format!("failed to bind grpc listener on {grpc_addr}"))?;

    info!(?grpc_addr, "rappel bridge server listening");

    run_grpc_server(grpc_listener, database).await
}

// ============================================================================
// gRPC Server
// ============================================================================

async fn run_grpc_server(listener: TcpListener, database: Database) -> Result<()> {
    use proto::workflow_service_server::WorkflowServiceServer;

    // Set up health reporting
    let (mut health_reporter, health_service) = health_reporter();
    health_reporter
        .set_serving::<WorkflowServiceServer<WorkflowGrpcService>>()
        .await;

    let incoming = TcpListenerStream::new(listener);
    let service = WorkflowGrpcService::new(database);

    Server::builder()
        .add_service(health_service)
        .add_service(WorkflowServiceServer::new(service))
        .serve_with_incoming(incoming)
        .await?;

    Ok(())
}

#[derive(Clone)]
struct WorkflowGrpcService {
    database: Database,
}

impl WorkflowGrpcService {
    fn new(database: Database) -> Self {
        Self { database }
    }
}

/// Log a registered workflow's IR in a pretty-printed format
fn log_workflow_ir(registration: &proto::WorkflowRegistration) {
    // Try to decode the IR from the registration
    match ir_ast::Program::decode(&registration.ir[..]) {
        Ok(program) => {
            let ir_str = ir_printer::print_program(&program);
            info!(
                workflow_name = %registration.workflow_name,
                ir_hash = %registration.ir_hash,
                "Registered workflow IR:\n{}",
                ir_str
            );
        }
        Err(e) => {
            error!(
                workflow_name = %registration.workflow_name,
                error = %e,
                "Failed to decode workflow IR"
            );
        }
    }
}

pub(crate) fn sanitize_interval(value: Option<f64>) -> Duration {
    let raw = value.unwrap_or(1.0);
    let clamped = raw.clamp(0.1, 30.0);
    Duration::from_secs_f64(clamped)
}

#[tonic::async_trait]
impl proto::workflow_service_server::WorkflowService for WorkflowGrpcService {
    async fn register_workflow(
        &self,
        request: tonic::Request<proto::RegisterWorkflowRequest>,
    ) -> Result<tonic::Response<proto::RegisterWorkflowResponse>, tonic::Status> {
        let inner = request.into_inner();
        let registration = inner
            .registration
            .ok_or_else(|| tonic::Status::invalid_argument("registration missing"))?;

        // Log the registered workflow IR
        log_workflow_ir(&registration);

        // Register the workflow version
        let version_id = self
            .database
            .upsert_workflow_version(
                &registration.workflow_name,
                &registration.ir_hash,
                &registration.ir,
                registration.concurrent,
            )
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        // Create an instance with initial context if provided
        let initial_input = registration.initial_context.map(|ctx| ctx.encode_to_vec());
        let instance_id = self
            .database
            .create_instance(
                &registration.workflow_name,
                version_id,
                initial_input.as_deref(),
            )
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        Ok(tonic::Response::new(proto::RegisterWorkflowResponse {
            workflow_version_id: version_id.to_string(),
            workflow_instance_id: instance_id.to_string(),
        }))
    }

    async fn wait_for_instance(
        &self,
        request: tonic::Request<proto::WaitForInstanceRequest>,
    ) -> Result<tonic::Response<proto::WaitForInstanceResponse>, tonic::Status> {
        use crate::db::{DbError, WorkflowInstanceId};

        let inner = request.into_inner();
        let instance_id: uuid::Uuid = inner.instance_id.parse().map_err(|err| {
            tonic::Status::invalid_argument(format!("invalid instance_id: {err}"))
        })?;

        let interval = sanitize_interval(Some(inner.poll_interval_secs));

        // Poll for instance completion
        loop {
            let result = self
                .database
                .get_instance(WorkflowInstanceId(instance_id))
                .await;

            match result {
                Ok(inst) if inst.status == "completed" => {
                    let payload = inst.result_payload.unwrap_or_default();
                    return Ok(tonic::Response::new(proto::WaitForInstanceResponse {
                        payload,
                    }));
                }
                Ok(inst) if inst.status == "failed" => {
                    return Err(tonic::Status::internal("workflow instance failed"));
                }
                Ok(_) => {
                    // Still running, wait and poll again
                    tokio::time::sleep(interval).await;
                }
                Err(DbError::NotFound(_)) => {
                    return Err(tonic::Status::not_found("instance not found"));
                }
                Err(e) => {
                    return Err(tonic::Status::internal(format!("database error: {e}")));
                }
            }
        }
    }

    async fn register_schedule(
        &self,
        request: tonic::Request<proto::RegisterScheduleRequest>,
    ) -> Result<tonic::Response<proto::RegisterScheduleResponse>, tonic::Status> {
        let inner = request.into_inner();

        // If a workflow registration is provided, register the workflow version first.
        // This ensures the workflow DAG exists when the schedule fires.
        if let Some(ref registration) = inner.registration {
            log_workflow_ir(registration);
            self.database
                .upsert_workflow_version(
                    &registration.workflow_name,
                    &registration.ir_hash,
                    &registration.ir,
                    registration.concurrent,
                )
                .await
                .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

            info!(
                workflow_name = %registration.workflow_name,
                ir_hash = %registration.ir_hash,
                "registered workflow version for scheduled execution"
            );
        }

        let schedule = inner
            .schedule
            .ok_or_else(|| tonic::Status::invalid_argument("schedule required"))?;

        // Validate and compute next_run_at
        let (schedule_type, cron_expr, interval_secs, next_run) = match schedule.r#type() {
            proto::ScheduleType::Cron => {
                let expr = &schedule.cron_expression;
                if expr.is_empty() {
                    return Err(tonic::Status::invalid_argument(
                        "cron_expression required for cron schedule",
                    ));
                }
                let next = next_cron_run(expr).map_err(tonic::Status::invalid_argument)?;
                (ScheduleType::Cron, Some(expr.as_str()), None, next)
            }
            proto::ScheduleType::Interval => {
                let secs = schedule.interval_seconds;
                if secs <= 0 {
                    return Err(tonic::Status::invalid_argument(
                        "interval_seconds must be positive",
                    ));
                }
                let next = next_interval_run(secs, None);
                (ScheduleType::Interval, None, Some(secs), next)
            }
            proto::ScheduleType::Unspecified => {
                return Err(tonic::Status::invalid_argument("schedule type required"));
            }
        };

        let input_payload = inner.inputs.map(|i| i.encode_to_vec());

        let schedule_id = self
            .database
            .upsert_schedule(
                &inner.workflow_name,
                schedule_type,
                cron_expr,
                interval_secs,
                input_payload.as_deref(),
                next_run,
            )
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        info!(
            workflow_name = %inner.workflow_name,
            schedule_type = %schedule_type.as_str(),
            next_run_at = %next_run.to_rfc3339(),
            "registered workflow schedule"
        );

        Ok(tonic::Response::new(proto::RegisterScheduleResponse {
            schedule_id: schedule_id.to_string(),
            next_run_at: next_run.to_rfc3339(),
        }))
    }

    async fn update_schedule_status(
        &self,
        request: tonic::Request<proto::UpdateScheduleStatusRequest>,
    ) -> Result<tonic::Response<proto::UpdateScheduleStatusResponse>, tonic::Status> {
        let inner = request.into_inner();

        let status_str = match inner.status() {
            proto::ScheduleStatus::Active => "active",
            proto::ScheduleStatus::Paused => "paused",
            proto::ScheduleStatus::Unspecified => {
                return Err(tonic::Status::invalid_argument("status required"));
            }
        };

        let success = self
            .database
            .update_schedule_status(&inner.workflow_name, status_str)
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        info!(
            workflow_name = %inner.workflow_name,
            status = %status_str,
            success = success,
            "updated schedule status"
        );

        Ok(tonic::Response::new(proto::UpdateScheduleStatusResponse {
            success,
        }))
    }

    async fn delete_schedule(
        &self,
        request: tonic::Request<proto::DeleteScheduleRequest>,
    ) -> Result<tonic::Response<proto::DeleteScheduleResponse>, tonic::Status> {
        let inner = request.into_inner();

        let success = self
            .database
            .delete_schedule(&inner.workflow_name)
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        info!(
            workflow_name = %inner.workflow_name,
            success = success,
            "deleted schedule"
        );

        Ok(tonic::Response::new(proto::DeleteScheduleResponse {
            success,
        }))
    }

    async fn list_schedules(
        &self,
        request: tonic::Request<proto::ListSchedulesRequest>,
    ) -> Result<tonic::Response<proto::ListSchedulesResponse>, tonic::Status> {
        let inner = request.into_inner();

        let schedules = self
            .database
            .list_schedules(inner.status_filter.as_deref())
            .await
            .map_err(|e| tonic::Status::internal(format!("database error: {e}")))?;

        let schedule_infos: Vec<proto::ScheduleInfo> = schedules
            .into_iter()
            .map(|s| {
                let schedule_type = match s.schedule_type.as_str() {
                    "cron" => proto::ScheduleType::Cron,
                    "interval" => proto::ScheduleType::Interval,
                    _ => proto::ScheduleType::Unspecified,
                };
                let status = match s.status.as_str() {
                    "active" => proto::ScheduleStatus::Active,
                    "paused" => proto::ScheduleStatus::Paused,
                    _ => proto::ScheduleStatus::Unspecified,
                };
                proto::ScheduleInfo {
                    id: s.id.to_string(),
                    workflow_name: s.workflow_name,
                    schedule_type: schedule_type.into(),
                    cron_expression: s.cron_expression.unwrap_or_default(),
                    interval_seconds: s.interval_seconds.unwrap_or(0),
                    status: status.into(),
                    next_run_at: s.next_run_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
                    last_run_at: s.last_run_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
                    last_instance_id: s
                        .last_instance_id
                        .map(|id| id.to_string())
                        .unwrap_or_default(),
                    created_at: s.created_at.to_rfc3339(),
                    updated_at: s.updated_at.to_rfc3339(),
                }
            })
            .collect();

        info!(count = schedule_infos.len(), "listed schedules");

        Ok(tonic::Response::new(proto::ListSchedulesResponse {
            schedules: schedule_infos,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_interval() {
        // Test default
        assert_eq!(sanitize_interval(None), Duration::from_secs_f64(1.0));

        // Test clamping
        assert_eq!(sanitize_interval(Some(0.01)), Duration::from_secs_f64(0.1));
        assert_eq!(
            sanitize_interval(Some(100.0)),
            Duration::from_secs_f64(30.0)
        );
        assert_eq!(sanitize_interval(Some(5.0)), Duration::from_secs_f64(5.0));
    }
}
