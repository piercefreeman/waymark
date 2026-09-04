//! The event record: what is true of every emission.

/// One emission. Identity is `(node_id, node_sequence)`.
///
/// The record carries only what is true of every event — who, when, in
/// what order; everything source-specific, ids included, is the payload,
/// and the payload is the source's own typed event.
#[derive(Debug)]
pub struct Event<NodeId, Payload> {
    /// The emitting node (one identity per node boot).
    pub node_id: NodeId,

    /// The emission's position in its node's stream.
    pub node_sequence: waymark_node_sequence::NodeSequence,

    /// When the emitter stamped the event.
    pub at: chrono::DateTime<chrono::Utc>,

    /// The source's own typed event.
    pub payload: Payload,
}
