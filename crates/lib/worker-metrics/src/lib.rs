//! Worker metrics runtime types.

use std::time::Duration;

/// Metrics from a single action round-trip.
#[derive(Debug, Clone)]
pub struct RoundTripMetrics<ResponsePayload> {
    /// Delivery ID used for correlation
    pub delivery_id: u64,
    /// Time from send to ACK receipt
    pub ack_latency: Duration,
    /// Time from send to result receipt
    pub round_trip: Duration,
    /// Time the worker spent executing (from worker's perspective)
    pub worker_duration: Duration,
    /// The outcome the worker reported
    pub response_payload: ResponsePayload,
}
