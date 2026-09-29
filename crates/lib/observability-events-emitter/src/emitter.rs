//! The emitter.

use waymark_node_sequence::NodeSequenceCounter;
use waymark_observability_events_core::Event;

/// The node's event stream: stamps each payload as the next event in
/// the stream and hands it to the batcher.
///
/// A node has ONE stream, so it has one counter, and one emitter owns
/// it: constructed once per node and shared by every producer (behind
/// an `Arc` at the wiring). The fields are private because the counter
/// must not be replaced mid-stream.
pub struct Emitter<NodeId, Payload> {
    node_id: NodeId,
    counter: NodeSequenceCounter,
    batcher: waymark_lossy_batcher::BatcherHandle<Event<NodeId, Payload>>,
}

impl<NodeId, Payload> Emitter<NodeId, Payload> {
    /// The emitter for `node_id`, at the start of its stream, pushing
    /// into `batcher`. Once per node.
    pub fn new(
        node_id: NodeId,
        batcher: waymark_lossy_batcher::BatcherHandle<Event<NodeId, Payload>>,
    ) -> Self {
        Self {
            node_id,
            counter: NodeSequenceCounter::new(),
            batcher,
        }
    }
}

impl<NodeId, Payload> Emitter<NodeId, Payload>
where
    NodeId: Clone,
{
    /// Stamp `payload` and hand it to the batcher: synchronous, never
    /// waits, never fails — the event is either flushed later or dropped
    /// and counted by the batcher.
    pub fn emit(&self, payload: Payload) {
        let event = Event {
            node_id: self.node_id.clone(),
            node_sequence: self.counter.next(),
            at: chrono::Utc::now(),
            payload,
        };
        self.batcher.push(event);
    }
}
