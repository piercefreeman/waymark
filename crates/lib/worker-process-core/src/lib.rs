use waymark_proto::messages as proto;

/// Channels for communicating with a connected worker.
/// Created when a worker successfully completes the handshake.
pub struct WorkerBridgeChannels {
    /// Send actions to the worker
    pub to_worker: tokio::sync::mpsc::Sender<proto::Envelope>,
    /// Receive results from the worker
    pub from_worker: tokio::sync::mpsc::Receiver<proto::Envelope>,
}
