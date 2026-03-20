use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;

use serde_json::json;
use tokio::process::Child;

use prost::Message;
use serde_json::Value;
use tokio::sync::RwLock;
use tokio::{
    process::Command,
    sync::{Mutex, mpsc},
};
use uuid::Uuid;

use waymark_proto::messages as proto;
use waymark_worker_core::{ActionRequest, BaseWorkerPool};
use waymark_worker_metrics::WorkerPoolMetrics;

use super::*;

fn make_string_kwarg(key: &str, value: &str) -> proto::WorkflowArgument {
    proto::WorkflowArgument {
        key: key.to_string(),
        value: Some(proto::WorkflowArgumentValue {
            kind: Some(proto::workflow_argument_value::Kind::Primitive(
                proto::PrimitiveWorkflowArgument {
                    kind: Some(proto::primitive_workflow_argument::Kind::StringValue(
                        value.to_string(),
                    )),
                },
            )),
        }),
    }
}

fn spawn_stub_child() -> Child {
    #[cfg(windows)]
    {
        Command::new("cmd")
            .args(["/C", "timeout", "/T", "60", "/NOBREAK"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn windows stub child")
    }
    #[cfg(not(windows))]
    {
        Command::new("sleep")
            .arg("60")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn unix stub child")
    }
}

async fn test_bridge() -> Option<Arc<WorkerBridgeServer>> {
    match WorkerBridgeServer::start(None).await {
        Ok(server) => Some(server),
        Err(err) => {
            let message = format!("{err:?}");
            if message.contains("Operation not permitted") || message.contains("Permission denied")
            {
                None
            } else {
                panic!("start worker bridge: {err}");
            }
        }
    }
}

fn make_result_payload(value: Value) -> proto::WorkflowArguments {
    proto::WorkflowArguments {
        arguments: vec![proto::WorkflowArgument {
            key: "result".to_string(),
            value: Some(waymark_message_conversions::json_to_workflow_argument_value(&value)),
        }],
    }
}

fn make_test_worker(
    worker_id: u64,
) -> (
    PythonWorker,
    mpsc::Receiver<proto::Envelope>,
    mpsc::Sender<proto::Envelope>,
) {
    let (to_worker, from_runner) = mpsc::channel(16);
    let (to_runner, from_worker) = mpsc::channel(16);
    let shared = Arc::new(Mutex::new(SharedState::new()));
    let reader_shared = Arc::clone(&shared);
    let reader_handle = tokio::spawn(async move {
        let mut incoming = from_worker;
        let _ = PythonWorker::reader_loop(&mut incoming, reader_shared).await;
    });

    let worker = PythonWorker {
        child: spawn_stub_child(),
        sender: to_worker,
        shared,
        next_delivery: AtomicU64::new(1),
        reader_handle: Some(reader_handle),
        worker_id,
    };
    (worker, from_runner, to_runner)
}

async fn make_single_worker_pool() -> Option<(
    Arc<PythonWorkerPool>,
    mpsc::Receiver<proto::Envelope>,
    mpsc::Sender<proto::Envelope>,
)> {
    let bridge = test_bridge().await?;
    let (worker, outgoing, incoming) = make_test_worker(0);
    let pool = PythonWorkerPool {
        workers: RwLock::new(vec![Arc::new(worker)]),
        cursor: AtomicUsize::new(0),
        metrics: StdMutex::new(WorkerPoolMetrics::new(
            vec![0],
            Duration::from_secs(THROUGHPUT_WINDOW_SECS),
            LATENCY_SAMPLE_SIZE,
        )),
        action_counts: vec![AtomicU64::new(0)],
        in_flight_counts: vec![AtomicUsize::new(0)],
        max_concurrent_per_worker: 2,
        max_action_lifecycle: None,
        bridge,
        config: PythonWorkerConfig::new(),
    };
    Some((Arc::new(pool), outgoing, incoming))
}

#[tokio::test]
async fn test_send_action_roundtrip_happy_path() {
    let (worker, mut outgoing, incoming) = make_test_worker(7);
    let dispatch_token = Uuid::new_v4();

    let responder = tokio::spawn(async move {
        let envelope = outgoing.recv().await.expect("dispatch envelope");
        assert_eq!(
            proto::MessageKind::try_from(envelope.kind).ok(),
            Some(proto::MessageKind::ActionDispatch)
        );
        let dispatch =
            proto::ActionDispatch::decode(envelope.payload.as_slice()).expect("decode dispatch");
        assert_eq!(dispatch.action_name, "greet");

        incoming
            .send(proto::Envelope {
                delivery_id: envelope.delivery_id + 100,
                partition_id: 0,
                kind: proto::MessageKind::Ack as i32,
                payload: proto::Ack {
                    acked_delivery_id: envelope.delivery_id,
                }
                .encode_to_vec(),
            })
            .await
            .expect("send ack");
        incoming
            .send(proto::Envelope {
                delivery_id: envelope.delivery_id,
                partition_id: 0,
                kind: proto::MessageKind::ActionResult as i32,
                payload: proto::ActionResult {
                    action_id: dispatch.action_id,
                    success: true,
                    payload: Some(make_result_payload(json!("hello"))),
                    worker_start_ns: 10,
                    worker_end_ns: 42,
                    dispatch_token: Some(dispatch_token.to_string()),
                    error_type: None,
                    error_message: None,
                }
                .encode_to_vec(),
            })
            .await
            .expect("send action result");
    });

    let metrics = worker
        .send_action(ActionDispatchPayload {
            action_id: "action-1".to_string(),
            instance_id: "instance-1".to_string(),
            sequence: 1,
            action_name: "greet".to_string(),
            module_name: "tests.actions".to_string(),
            kwargs: proto::WorkflowArguments {
                arguments: vec![make_string_kwarg("name", "World")],
            },
            timeout_seconds: 30,
            max_retries: 0,
            attempt_number: 0,
            dispatch_token,
        })
        .await
        .expect("send action");

    responder.await.expect("responder task");
    assert!(metrics.success);
    assert_eq!(metrics.action_id, "action-1");
    assert_eq!(metrics.instance_id, "instance-1");
    assert_eq!(metrics.dispatch_token, Some(dispatch_token));
    assert_eq!(metrics.worker_duration, Duration::from_nanos(32));

    worker.shutdown().await.expect("shutdown worker");
}

#[test]
fn test_concurrency_slot_logic() {
    // Test the atomic slot logic without spawning workers
    let in_flight = AtomicUsize::new(0);
    let max_concurrent = 3;

    // Helper to try acquire
    let try_acquire = || {
        loop {
            let current = in_flight.load(Ordering::Acquire);
            if current >= max_concurrent {
                return false;
            }
            match in_flight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    };

    // Acquire up to max
    assert!(try_acquire());
    assert!(try_acquire());
    assert!(try_acquire());
    // At capacity
    assert!(!try_acquire());
    assert_eq!(in_flight.load(Ordering::Relaxed), 3);

    // Release one
    in_flight.fetch_sub(1, Ordering::Release);
    assert_eq!(in_flight.load(Ordering::Relaxed), 2);

    // Can acquire again
    assert!(try_acquire());
    assert!(!try_acquire());
}

#[test]
fn test_action_result_success_false_deserialize() {
    use prost::Message;

    // These are the bytes from Python when success=False is set
    // The success field is NOT included because it's the default value in proto3
    let success_false_bytes: &[u8] = &[0x0a, 0x04, 0x74, 0x65, 0x73, 0x74];

    // These are the bytes from Python when success=True is set
    let success_true_bytes: &[u8] = &[0x0a, 0x04, 0x74, 0x65, 0x73, 0x74, 0x10, 0x01];

    // Deserialize success=False case
    let result_false =
        proto::ActionResult::decode(success_false_bytes).expect("decode success=false");
    assert_eq!(result_false.action_id, "test");
    assert!(
        !result_false.success,
        "success should be false when field is omitted (proto3 default)"
    );

    // Deserialize success=True case
    let result_true = proto::ActionResult::decode(success_true_bytes).expect("decode success=true");
    assert_eq!(result_true.action_id, "test");
    assert!(
        result_true.success,
        "success should be true when field is 1"
    );
}
