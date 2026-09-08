//! Wire types shared by the operations.

/// What the events show about one VM, as served on the wire.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct Instance {
    /// The VM's id.
    pub vm_id: waymark_http_api_types::UuidId<waymark_ids::InstanceId>,

    /// The VM's latest run: the last time a VM driver started it. Absent
    /// when the events know the VM without any run of it.
    pub latest_run: Option<Run>,

    /// The workflow's terminal outcome, once an effect carried one.
    pub outcome: Option<Outcome>,

    /// The VM's most recent event.
    pub last_event: LastEvent,
}

/// One run of a VM, as served on the wire.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct Run {
    /// The node the run is on: one identity per node boot.
    pub node_id: waymark_http_api_types::UuidId<waymark_ids::NodeId>,

    /// When the VM driver started the run.
    pub started_at: chrono::DateTime<chrono::Utc>,

    /// The run's stop, once observed; absent while no stop has been
    /// observed for it.
    pub stopped: Option<Stopped>,
}

/// A run's stop, as served on the wire.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct Stopped {
    /// When the VM driver stopped.
    pub at: chrono::DateTime<chrono::Utc>,

    /// Why: the stop event's kind.
    pub reason: waymark_api_observability_events_http_types::KindTag<
        waymark_observability_events_payload::vm_driver::StopKind,
        StopKind,
    >,
}

/// The workflow's terminal outcome, as served on the wire.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct Outcome {
    /// When the effect was emitted.
    pub at: chrono::DateTime<chrono::Utc>,

    /// Which terminal effect it was.
    pub kind: OutcomeKind,
}

/// The two effects that end a workflow.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeKind {
    /// The workflow completed with a value.
    Complete,

    /// The workflow raised an exception nothing caught.
    UnhandledException,
}

impl From<waymark_observability_state_core::OutcomeKind> for OutcomeKind {
    fn from(kind: waymark_observability_state_core::OutcomeKind) -> Self {
        match kind {
            waymark_observability_state_core::OutcomeKind::Complete => OutcomeKind::Complete,
            waymark_observability_state_core::OutcomeKind::UnhandledException => {
                OutcomeKind::UnhandledException
            }
        }
    }
}

/// A VM's most recent event, as served on the wire.
#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct LastEvent {
    /// When it was emitted.
    pub at: chrono::DateTime<chrono::Utc>,

    /// The node that emitted it.
    pub node_id: waymark_http_api_types::UuidId<waymark_ids::NodeId>,

    /// Its position in that node's stream.
    pub node_sequence: u64,

    /// Its position in its run.
    pub run_sequence: u64,

    /// What it was: its kind.
    pub kind: waymark_api_observability_events_http_types::KindTag<
        waymark_observability_events_payload::Kind,
        waymark_api_observability_events_http_types::EventKind,
    >,
}

impl From<waymark_observability_state_core::Stopped> for Stopped {
    fn from(stopped: waymark_observability_state_core::Stopped) -> Self {
        Self {
            at: stopped.at,
            reason: stopped.reason.into(),
        }
    }
}

impl From<waymark_observability_state_core::Run> for Run {
    fn from(run: waymark_observability_state_core::Run) -> Self {
        Self {
            node_id: run.node_id.into(),
            started_at: run.started_at,
            stopped: run.stopped.map(Into::into),
        }
    }
}

impl From<waymark_observability_state_core::Outcome> for Outcome {
    fn from(outcome: waymark_observability_state_core::Outcome) -> Self {
        Self {
            at: outcome.at,
            kind: outcome.kind.into(),
        }
    }
}

impl From<waymark_observability_state_core::LastEvent> for LastEvent {
    fn from(last_event: waymark_observability_state_core::LastEvent) -> Self {
        Self {
            at: last_event.at,
            node_id: last_event.node_id.into(),
            node_sequence: last_event.node_sequence.get(),
            run_sequence: last_event.run_sequence,
            kind: waymark_observability_events_payload::Kind::VmDriver(last_event.kind).into(),
        }
    }
}

impl From<waymark_observability_state_core::InstanceState> for Instance {
    fn from(state: waymark_observability_state_core::InstanceState) -> Self {
        Self {
            vm_id: state.vm_id.into(),
            latest_run: state.latest_run.map(Into::into),
            outcome: state.outcome.map(Into::into),
            last_event: state.last_event.into(),
        }
    }
}

/// The wire page of a read's outcome: an absent page is an empty one
/// with no cursor.
pub(crate) fn page<Cursor>(
    page: Option<waymark_observability_state_query_backend::Page<Cursor>>,
) -> waymark_http_api_types::Page<Instance, Cursor> {
    let Some(page) = page else {
        return waymark_http_api_types::Page {
            items: Vec::new(),
            next: None,
        };
    };

    let items = page.instances.into_iter().map(Into::into).collect();

    waymark_http_api_types::Page {
        items,
        next: Some(page.next.into()),
    }
}

/// The name the stop kinds are listed under.
#[derive(Debug)]
pub struct StopKind;

impl waymark_http_api_types::TypeName for StopKind {
    const NAME: &'static str = "StopKind";
}
