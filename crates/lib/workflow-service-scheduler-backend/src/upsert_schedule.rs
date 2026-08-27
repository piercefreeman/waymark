//! Schedule registration (upsert) trait.

use super::common::{HasExecutableId, HasTimestamp};

/// Parameters for registering (or re-pointing) a schedule, passed to
/// [`UpsertSchedule::upsert_schedule`].
#[derive(Debug)]
pub struct Params<'a, ExecutableId, Timestamp> {
    /// The schedule's name — its sole key.
    pub schedule_name: &'a str,

    /// The executable the schedule pins.
    pub executable_id: &'a ExecutableId,

    /// The encoded schedule definition, opaque to the backend.
    pub definition: &'a [u8],

    /// The serialized initial runtime snapshot — the template each
    /// spawned VM starts from.
    pub initial_snapshot: &'a [u8],

    /// When the first run is due. Callers compute it from the
    /// definition before the call.
    pub next_run_at: &'a Timestamp,
}

// All fields are references, so the params are copyable for any id
// types — no `Copy`/`Clone` bounds, unlike what `derive` would impose.
impl<ExecutableId, Timestamp> Clone for Params<'_, ExecutableId, Timestamp> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<ExecutableId, Timestamp> Copy for Params<'_, ExecutableId, Timestamp> {}

/// Backend capability for registering a schedule.
///
/// Registration is an upsert on the schedule name: an existing schedule
/// is fully re-pointed — executable, definition, snapshot template, and
/// run cursor all replaced, status reset to active. Re-pointing a name
/// across workflows is intentional. The last-spawned-instance marker is
/// left untouched, so overlap suppression still sees a run spawned under
/// the previous registration.
pub trait UpsertSchedule: HasExecutableId + HasTimestamp {
    /// The error type for upsert operations.
    type Error: std::fmt::Debug;

    /// Durably register (or re-point) the schedule.
    fn upsert_schedule<'a>(
        &'a self,
        params: Params<'a, Self::ExecutableId, Self::Timestamp>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'a;
}
