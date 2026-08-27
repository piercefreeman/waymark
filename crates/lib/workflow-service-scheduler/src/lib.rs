//! Workflow-service scheduler — encapsulates schedule management for
//! the bridge: registration with the baked snapshot template, status
//! updates, deletion, and listing with decoded definitions.

#![warn(missing_docs)]

use waymark_vm_codec_core::{DeserializerProvider, SerializerProvider};

/// Errors returned by [`ScheduleService::register_schedule`].
#[derive(Debug, thiserror::Error)]
pub enum RegisterScheduleError<UpsertError, CodecError> {
    /// The initial runtime snapshot serialization failed.
    #[error("serialize initial runtime snapshot: {0:?}")]
    SerializeSnapshot(CodecError),

    /// The schedule definition serialization failed.
    #[error("serialize schedule definition: {0:?}")]
    SerializeDefinition(CodecError),

    /// The definition matches no instant, ever.
    #[error("the schedule definition matches no instant")]
    NoOccurrences,

    /// The first run could not be produced from the definition.
    #[error("no producible first run: {0}")]
    NextRun(#[source] waymark_scheduler_core::ComputeNextRunError),

    /// The schedule upsert failed.
    #[error("upsert schedule: {0:?}")]
    Upsert(UpsertError),
}

/// Errors returned by [`ScheduleService::list_schedules`].
#[derive(Debug, thiserror::Error)]
pub enum ListSchedulesError<ListError, CodecError> {
    /// The schedule listing failed.
    #[error("list schedules: {0:?}")]
    List(ListError),

    /// A stored definition blob failed to decode.
    #[error("decode schedule definition: {0:?}")]
    DecodeDefinition(CodecError),
}

/// A schedule as listed, with its definition decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedSchedule<VmId, Timestamp> {
    /// The schedule's name — its sole key.
    pub schedule_name: String,

    /// The pinned executable's workflow name.
    pub workflow_name: String,

    /// The decoded schedule definition.
    pub definition: waymark_scheduler_core::ScheduleDefinition,

    /// The schedule's lifecycle status.
    pub status: waymark_scheduler_core::ScheduleStatus,

    /// When the next run is due.
    pub next_run_at: Timestamp,

    /// The most recently spawned instance, if any run was ever spawned.
    pub last_instance_id: Option<VmId>,
}

/// High-level service for managing workflow schedules.
pub struct ScheduleService<Backend, Codec> {
    /// The schedule persistence backend.
    pub backend: Backend,

    /// Encodes definition blobs and initial runtime snapshots.
    pub codec: Codec,
}

impl<Backend, Codec> ScheduleService<Backend, Codec> {
    /// Create a new schedule service wrapping the given backend and codec.
    pub fn new(backend: Backend, codec: Codec) -> Self {
        Self { backend, codec }
    }
}

impl<Backend, Codec> ScheduleService<Backend, Codec>
where
    Backend: waymark_workflow_service_scheduler_backend::UpsertSchedule<
            Timestamp = chrono::DateTime<chrono::Utc>,
        >,
    Codec: SerializerProvider,
{
    /// Register (or re-point) a schedule: bake the initial runtime
    /// snapshot template, encode the definition, compute the first run
    /// strictly after `now`, and upsert the schedule row. Returns when
    /// the schedule will first fire.
    pub async fn register_schedule(
        &self,
        schedule_name: &str,
        executable_id: &Backend::ExecutableId,
        definition: &waymark_scheduler_core::ScheduleDefinition,
        now: chrono::DateTime<chrono::Utc>,
        snapshot_provider: impl for<'buf> FnOnce(
            Codec::Serializer<'buf>,
        )
            -> Result<(), <Codec as SerializerProvider>::Error>,
    ) -> Result<
        chrono::DateTime<chrono::Utc>,
        RegisterScheduleError<
            <Backend as waymark_workflow_service_scheduler_backend::UpsertSchedule>::Error,
            <Codec as SerializerProvider>::Error,
        >,
    > {
        let mut initial_snapshot = Vec::new();
        self.codec
            .with_serializer(&mut initial_snapshot, snapshot_provider)
            .map_err(RegisterScheduleError::SerializeSnapshot)?;

        let mut definition_bytes = Vec::new();
        self.codec
            .with_serializer(&mut definition_bytes, |serializer| {
                serde::Serialize::serialize(definition, serializer).map(|_| ())
            })
            .map_err(RegisterScheduleError::SerializeDefinition)?;

        let next_run_at = match waymark_scheduler_core::compute_next_run(definition, now) {
            Ok(Some(next_run_at)) => next_run_at,
            Ok(None) => return Err(RegisterScheduleError::NoOccurrences),
            Err(err) => return Err(RegisterScheduleError::NextRun(err)),
        };

        self.backend
            .upsert_schedule(
                waymark_workflow_service_scheduler_backend::upsert_schedule::Params {
                    schedule_name,
                    executable_id,
                    definition: &definition_bytes,
                    initial_snapshot: &initial_snapshot,
                    next_run_at: &next_run_at,
                },
            )
            .await
            .map_err(RegisterScheduleError::Upsert)?;

        Ok(next_run_at)
    }
}

impl<Backend, Codec> ScheduleService<Backend, Codec>
where
    Backend: waymark_workflow_service_scheduler_backend::UpdateScheduleStatus,
{
    /// Set a schedule's lifecycle status. Returns `false` when no such
    /// schedule exists.
    pub async fn update_schedule_status(
        &self,
        schedule_name: &str,
        status: waymark_scheduler_core::ScheduleStatus,
    ) -> Result<
        bool,
        <Backend as waymark_workflow_service_scheduler_backend::UpdateScheduleStatus>::Error,
    > {
        self.backend
            .update_schedule_status(schedule_name, status)
            .await
    }
}

impl<Backend, Codec> ScheduleService<Backend, Codec>
where
    Backend: waymark_workflow_service_scheduler_backend::DeleteSchedule,
{
    /// Hard-delete a schedule. Returns `false` when no such schedule
    /// exists.
    pub async fn delete_schedule(
        &self,
        schedule_name: &str,
    ) -> Result<bool, <Backend as waymark_workflow_service_scheduler_backend::DeleteSchedule>::Error>
    {
        self.backend.delete_schedule(schedule_name).await
    }
}

impl<Backend, Codec> ScheduleService<Backend, Codec>
where
    Backend: waymark_workflow_service_scheduler_backend::ListSchedules,
    Codec: DeserializerProvider,
{
    /// List schedules, optionally filtered by status, with their
    /// definitions decoded.
    pub async fn list_schedules(
        &self,
        status: Option<waymark_scheduler_core::ScheduleStatus>,
    ) -> Result<
        Vec<ListedSchedule<Backend::VmId, Backend::Timestamp>>,
        ListSchedulesError<
            <Backend as waymark_workflow_service_scheduler_backend::ListSchedules>::Error,
            <Codec as DeserializerProvider>::Error,
        >,
    > {
        let records = self
            .backend
            .list_schedules(status)
            .await
            .map_err(ListSchedulesError::List)?;

        records
            .into_iter()
            .map(|record| {
                let definition = self
                    .codec
                    .with_deserializer(&record.definition, |deserializer| {
                        serde::Deserialize::deserialize(deserializer)
                    })
                    .map_err(ListSchedulesError::DecodeDefinition)?;
                Ok(ListedSchedule {
                    schedule_name: record.schedule_name,
                    workflow_name: record.workflow_name,
                    definition,
                    status: record.status,
                    next_run_at: record.next_run_at,
                    last_instance_id: record.last_instance_id,
                })
            })
            .collect()
    }
}
