use waymark_proto::messages as proto;
use waymark_worker_core::ActionRequest;
use waymark_worker_message_protocol::ActionDispatchPayload;

pub fn to_dispatch_payload(
    request: ActionRequest,
) -> Result<ActionDispatchPayload, Box<proto::ActionResult>> {
    let ActionRequest {
        action_name,
        module_name,
        kwargs,
        metadata,
    } = request;

    let Some(module_name) = module_name else {
        return Err(Box::new(proto::ActionResult {
            success: false,
            error_type: Some("RemoteWorkerPoolError".to_owned()),
            error_message: Some("missing module name for action request".to_owned()),
            metadata,
            ..Default::default()
        }));
    };

    // The wire ids and the per-attempt token are the transport's own
    // vocabulary, not the caller's: filled with inert values, minted
    // here where a real one is needed.
    let dispatch = ActionDispatchPayload {
        action_id: String::new(),
        instance_id: String::new(),
        sequence: 0,
        action_name,
        module_name,
        kwargs,
        timeout_seconds: 0,
        max_retries: 0,
        attempt_number: 1,
        dispatch_token: uuid::Uuid::new_v4(),
        metadata,
    };

    Ok(dispatch)
}
