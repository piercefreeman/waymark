use waymark_worker_core::ExecutionProgress;
use waymark_worker_message_protocol::SendActionError;

use super::loss_progress;

#[test]
fn a_dispatch_the_protocol_refused_never_started() {
    assert!(matches!(
        loss_progress(SendActionError::WorkerProtocolClosed),
        ExecutionProgress::NotStarted
    ));
}

#[test]
fn a_dispatch_that_never_reached_the_worker_never_started() {
    assert!(matches!(
        loss_progress(SendActionError::NotSent),
        ExecutionProgress::NotStarted
    ));
}

#[test]
fn a_dispatch_sent_without_an_ack_is_unknown() {
    assert!(matches!(
        loss_progress(SendActionError::NoAck),
        ExecutionProgress::Unknown
    ));
}

#[test]
fn a_dispatch_acked_without_a_response_is_unknown() {
    assert!(matches!(
        loss_progress(SendActionError::NoResponse),
        ExecutionProgress::Unknown
    ));
}
