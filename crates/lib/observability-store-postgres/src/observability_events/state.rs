//! The state reads: the instance view, kept by the database as the
//! events arrive and read as it stands.

use nonempty_collections::NEVec;
use sqlx::Row as _;
use waymark_observability_events_payload::vm_driver;
use waymark_observability_state_core::{
    InstanceState, LastEvent, Outcome, OutcomeKind, Run, Stopped,
};
use waymark_observability_state_query_backend::list_instances;

use super::common::{UnexpectedKindError, decode_kind, decode_node_sequence, decode_run_sequence};
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

        let mut parts = text.splitn(2, '/');
        let last_at = parts.next().ok_or_else(not_a_cursor)?;
        let vm_id = parts.next().ok_or_else(not_a_cursor)?;

        let last_at: i64 = last_at.parse().map_err(|_| not_a_cursor())?;
        let last_at = chrono::DateTime::from_timestamp_micros(last_at).ok_or_else(not_a_cursor)?;
        let vm_id = vm_id.parse().map_err(|_| not_a_cursor())?;

        Ok(Self { last_at, vm_id })
    }
}

/// The instance view's columns, as the instances table holds them.
const INSTANCE_COLUMNS: &str = r#"
    vm_id,
    last_at,
    last_node_id,
    last_node_sequence,
    last_run_sequence,
    last_kind,
    run_node_id,
    run_started_at,
    stopped_at,
    stop_kind,
    outcome_at,
    outcome_kind
"#;

/// The VM driver kind a stored tag names; any other kind where a VM
/// driver kind is expected is a decode error.
fn vm_driver_kind(tag: &str) -> Result<vm_driver::Kind, sqlx::Error> {
    match decode_kind(tag)? {
        waymark_observability_events_payload::Kind::VmDriver(kind) => Ok(kind),
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
        outcome,
        last_event,
    })
}

/// Append the list read to `query`: the instances whose last activity is
/// in the range, most recent first, past the cursor, one page — an index
/// walk whatever the tables hold.
pub(super) fn push_list_query(
    query: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>,
    params: &list_instances::Params<InstanceCursor>,
) {
    query.push(format!(
        r#"
        SELECT {INSTANCE_COLUMNS}
        FROM observability_vm_instances
        WHERE last_at >= "#
    ));
    query.push_bind(params.from);
    query.push(" AND last_at < ");
    query.push_bind(params.to);
    // Keyset: strictly before the position in the (descending) order.
    if let Some(after) = &params.after {
        query.push(" AND (last_at, vm_id) < (");
        query.push_bind(after.last_at);
        query.push(", ");
        query.push_bind(after.vm_id);
        query.push(")");
    }
    query.push(" ORDER BY last_at DESC, vm_id DESC LIMIT ");
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
        let row = sqlx::query(&format!(
            r#"
            SELECT {INSTANCE_COLUMNS}
            FROM observability_vm_instances
            WHERE vm_id = $1
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
