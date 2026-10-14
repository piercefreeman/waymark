//! The state reads: the instance view, kept by the database as the
//! events arrive and read as it stands.

use nonempty_collections::NEVec;
use sqlx::Row as _;
use waymark_observability_events_payload::vm_driver;
use waymark_observability_state_core::{
    Executable, ExecutableData, InstanceState, LastEvent, Outcome, OutcomeKind, Run, Stopped,
};
use waymark_observability_state_query_backend::list_instances;

use super::common::{
    HalfNullExecutableDataError, UnexpectedKindError, decode_kind, decode_node_sequence,
    decode_run_sequence,
};
use crate::Store;
use crate::common::to_bigint_saturating;

/// A position in the instance list — the instance last returned, by the
/// columns the order is over.
#[derive(Debug)]
pub struct InstanceCursor {
    /// The instance's last event's `at`.
    last_at: chrono::DateTime<chrono::Utc>,

    /// The instance's `vm_id`.
    vm_id: waymark_ids::InstanceId,
}

impl waymark_cursor_core::EncodeCursor for InstanceCursor {
    fn encode(&self) -> String {
        format!("{}/{}", self.last_at.timestamp_micros(), self.vm_id)
    }
}

impl waymark_cursor_core::DecodeCursor for InstanceCursor {
    type Error = super::ParseCursorError;

    fn decode(text: &str) -> Result<Self, super::ParseCursorError> {
        let not_a_cursor = || super::ParseCursorError {
            text: text.to_owned(),
        };

        let (last_at, vm_id) = text.split_once('/').ok_or_else(not_a_cursor)?;

        let last_at: i64 = last_at.parse().map_err(|_| not_a_cursor())?;
        let last_at = chrono::DateTime::from_timestamp_micros(last_at).ok_or_else(not_a_cursor)?;
        let vm_id = vm_id.parse().map_err(|_| not_a_cursor())?;

        Ok(Self { last_at, vm_id })
    }
}

/// The instance view's columns, as the instances table holds them, with
/// the run's executable joined in by its id.
const INSTANCE_COLUMNS: &str = r#"
    instance.vm_id,
    instance.last_at,
    instance.last_node_id,
    instance.last_node_sequence,
    instance.last_run_sequence,
    instance.last_kind,
    instance.run_node_id,
    instance.run_started_at,
    instance.executable_id,
    executable.name AS executable_name,
    executable.version AS executable_version,
    instance.stopped_at,
    instance.stop_kind,
    instance.outcome_at,
    instance.outcome_kind
"#;
// `stop_node_sequence` is left out on purpose: the trigger keeps it to
// order stops, the state does not serve it.

/// The tables the instance view reads: the instances, and for each the
/// executable its run is of — when the store holds it.
const INSTANCE_TABLES: &str = r#"
    observability_vm_instances AS instance
    LEFT JOIN observability_vm_executables AS executable
        ON executable.id = instance.executable_id
"#;

/// The VM driver kind a stored tag stands for; any other kind where a VM
/// driver kind is expected is a decode error.
fn vm_driver_kind(tag: &str) -> Result<vm_driver::Kind, sqlx::Error> {
    match decode_kind(tag)? {
        waymark_observability_events_payload::Kind::VmDriver(kind) => Ok(kind),
        _ => Err(sqlx::Error::Decode(Box::new(UnexpectedKindError {
            tag: tag.to_owned(),
        }))),
    }
}

/// Read one instance from a row shaped like [`INSTANCE_COLUMNS`].
fn decode_instance(row: &sqlx::postgres::PgRow) -> Result<InstanceState, sqlx::Error> {
    let vm_id = row.try_get("vm_id")?;

    let last_kind: String = row.try_get("last_kind")?;
    let last_event = LastEvent {
        at: row.try_get("last_at")?,
        node_id: row.try_get("last_node_id")?,
        node_sequence: decode_node_sequence(row.try_get("last_node_sequence")?)?,
        run_sequence: decode_run_sequence(row.try_get("last_run_sequence")?)?,
        kind: vm_driver_kind(&last_kind)?,
    };

    let run_node_id: Option<waymark_ids::NodeId> = row.try_get("run_node_id")?;
    let latest_run = match run_node_id {
        None => None,
        Some(node_id) => {
            let stop_kind: Option<String> = row.try_get("stop_kind")?;
            let stopped = match stop_kind {
                None => None,
                Some(tag) => {
                    let reason = match vm_driver_kind(&tag)? {
                        vm_driver::Kind::VmStopped(reason) => reason,
                        _ => {
                            return Err(sqlx::Error::Decode(Box::new(UnexpectedKindError { tag })));
                        }
                    };
                    Some(Stopped {
                        at: row.try_get("stopped_at")?,
                        reason,
                    })
                }
            };
            Some(Run {
                node_id,
                started_at: row.try_get("run_started_at")?,
                stopped,
            })
        }
    };

    let executable_id: Option<waymark_ids::WorkflowVersionId> = row.try_get("executable_id")?;
    let executable = match executable_id {
        None => None,
        Some(id) => {
            // The executables row has both or the join found no row: a
            // half-null pair is not a row this store wrote.
            let name: Option<String> = row.try_get("executable_name")?;
            let version: Option<String> = row.try_get("executable_version")?;
            let data = match (name, version) {
                (Some(name), Some(version)) => Some(ExecutableData { name, version }),
                (None, None) => None,
                (name, version) => {
                    return Err(sqlx::Error::Decode(Box::new(HalfNullExecutableDataError {
                        name,
                        version,
                    })));
                }
            };
            Some(Executable { id, data })
        }
    };

    let outcome_kind: Option<String> = row.try_get("outcome_kind")?;
    let outcome = match outcome_kind {
        None => None,
        Some(tag) => {
            let kind = match vm_driver_kind(&tag)? {
                vm_driver::Kind::EffectEmitted(vm_driver::EffectKind::Complete) => {
                    OutcomeKind::Complete
                }
                vm_driver::Kind::EffectEmitted(vm_driver::EffectKind::UnhandledException) => {
                    OutcomeKind::UnhandledException
                }
                _ => return Err(sqlx::Error::Decode(Box::new(UnexpectedKindError { tag }))),
            };
            Some(Outcome {
                at: row.try_get("outcome_at")?,
                kind,
            })
        }
    };

    Ok(InstanceState {
        vm_id,
        latest_run,
        executable,
        outcome,
        last_event,
    })
}

/// Append the list read to `query`: the instances whose last activity is
/// in the range, most recent first, past the cursor, one page — an index
/// walk whatever the tables hold.
pub(super) fn push_list_query(
    query: &mut sqlx::QueryBuilder<sqlx::Postgres>,
    params: &list_instances::Params<InstanceCursor>,
) {
    query.push(format!(
        r#"
        SELECT {INSTANCE_COLUMNS}
        FROM {INSTANCE_TABLES}
        WHERE instance.last_at >= "#
    ));
    query.push_bind(params.from);
    query.push(" AND instance.last_at < ");
    query.push_bind(params.to);
    // Keyset: strictly before the position in the (descending) order.
    if let Some(after) = &params.after {
        query.push(" AND (instance.last_at, instance.vm_id) < (");
        query.push_bind(after.last_at);
        query.push(", ");
        query.push_bind(after.vm_id);
        query.push(")");
    }
    query.push(" ORDER BY instance.last_at DESC, instance.vm_id DESC LIMIT ");
    query.push_bind(to_bigint_saturating(
        u64::try_from(params.limit.get()).unwrap_or(u64::MAX),
    ));
}

impl waymark_observability_state_query_backend::ListInstances for Store {
    type Cursor = InstanceCursor;

    type Error = sqlx::Error;

    async fn list_instances(
        &self,
        params: list_instances::Params<InstanceCursor>,
    ) -> Result<Option<waymark_observability_state_query_backend::PageFor<Self>>, sqlx::Error> {
        let mut query = sqlx::QueryBuilder::new("");
        push_list_query(&mut query, &params);

        let rows = query.build().fetch_all(&self.pool).await?;
        let instances = rows
            .iter()
            .map(decode_instance)
            .collect::<Result<Vec<_>, _>>()?;
        let Some(instances) = NEVec::try_from_vec(instances) else {
            return Ok(None);
        };
        let last = instances.last();
        let next = InstanceCursor {
            last_at: last.last_event.at,
            vm_id: last.vm_id,
        };

        Ok(Some(waymark_observability_state_query_backend::Page {
            instances,
            next,
        }))
    }
}

impl waymark_observability_state_query_backend::GetInstance for Store {
    type Error = sqlx::Error;

    async fn get_instance(
        &self,
        vm_id: waymark_ids::InstanceId,
    ) -> Result<Option<InstanceState>, sqlx::Error> {
        let row = sqlx::query(const_format::formatcp!(
            r#"
            SELECT {INSTANCE_COLUMNS}
            FROM {INSTANCE_TABLES}
            WHERE instance.vm_id = $1
            "#
        ))
        .bind(vm_id)
        .fetch_optional(&self.pool)
        .await?;

        row.as_ref().map(decode_instance).transpose()
    }
}

#[cfg(test)]
mod tests;
