//! The observability state: views derived from the observability events.
//!
//! A view states what the events show, in the events' own terms — the
//! kinds are the payload's closed sets, and nothing here classifies
//! beyond them. The store derives a view on read; nothing maintains it.

#![warn(missing_docs)]

/// What the events show about one VM.
#[derive(Debug)]
pub struct InstanceState {
    /// The VM.
    pub vm_id: waymark_ids::InstanceId,

    /// The VM's latest run: the last time a VM driver started it. Absent
    /// when the events know the VM without any run of it.
    pub latest_run: Option<Run>,

    /// The workflow's terminal outcome, once an effect carried one.
    pub outcome: Option<Outcome>,

    /// The VM's most recent event.
    pub last_event: LastEvent,
}

/// One run of a VM: from a VM driver starting it to that driver stopping.
#[derive(Debug)]
pub struct Run {
    /// The node the run is on.
    pub node_id: waymark_ids::NodeId,

    /// When the VM driver started the run.
    pub started_at: chrono::DateTime<chrono::Utc>,

    /// The run's stop, once observed; absent while no stop has been
    /// observed for it.
    pub stopped: Option<Stopped>,
}

/// A run's stop.
#[derive(Debug)]
pub struct Stopped {
    /// When the VM driver stopped.
    pub at: chrono::DateTime<chrono::Utc>,

    /// Why, in the VM driver's own terms.
    pub reason: waymark_observability_events_payload::vm_driver::StopKind,
}

/// The workflow's terminal outcome, as the effect that carried it.
#[derive(Debug)]
pub struct Outcome {
    /// When the effect was emitted.
    pub at: chrono::DateTime<chrono::Utc>,

    /// Which of the two terminal effects it was.
    pub kind: OutcomeKind,
}

/// The two effects that end a workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutcomeKind {
    /// The workflow completed with a value.
    Complete,

    /// The workflow raised an exception nothing caught.
    UnhandledException,
}

/// A VM's most recent event.
#[derive(Debug)]
pub struct LastEvent {
    /// When it was emitted.
    pub at: chrono::DateTime<chrono::Utc>,

    /// The node that emitted it.
    pub node_id: waymark_ids::NodeId,

    /// Its position in that node's stream.
    pub node_sequence: waymark_node_sequence::NodeSequence,

    /// Its position in its run.
    pub run_sequence: u64,

    /// What it was.
    pub kind: waymark_observability_events_payload::vm_driver::Kind,
}
