use nonempty_collections::NESlice;
use waymark_cursor_core::{DecodeCursor as _, EncodeCursor as _};
use waymark_observability_events_payload::vm_driver::{Observation, StopKind, StopReason};
use waymark_observability_state_core::OutcomeKind;
use waymark_observability_state_query_backend::{GetInstance as _, ListInstances as _};

use super::InstanceCursor;
use crate::test_helpers::test_store;

type Event = waymark_observability_events_core::Event<
    waymark_ids::NodeId,
    waymark_observability_events_payload::Payload,
>;

/// A node's stream under construction: events get consecutive positions
/// and the times the test gives them.
struct Node {
    node_id: waymark_ids::NodeId,
    counter: waymark_node_sequence::NodeSequenceCounter,
}

impl Node {
    fn new() -> Self {
        Self {
            node_id: waymark_ids::NodeId::new_uuid_v4(),
            counter: waymark_node_sequence::NodeSequenceCounter::new(),
        }
    }

    fn event(
        &self,
        at_secs: i64,
        vm_id: waymark_ids::InstanceId,
        run_sequence: u64,
        observation: Observation,
    ) -> Event {
        Event {
            node_id: self.node_id,
            node_sequence: self.counter.next(),
            at: chrono::DateTime::from_timestamp_secs(at_secs).unwrap(),
            payload: waymark_observability_events_payload::Payload::VmDriver(
                waymark_observability_events_payload::vm_driver::Payload {
                    vm_id,
                    run_sequence,
                    observation,
                },
            ),
        }
    }
}

fn at(secs: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_secs(secs).unwrap()
}

fn complete() -> Observation {
    Observation::EffectEmitted {
        effect_number: waymark_vm_runtime_effect::EffectNumber(0),
        effect: waymark_observability_events_payload::vm_driver::EffectSummary::Complete,
    }
}

fn snapshot() -> Observation {
    Observation::SnapshotPersisted { size_in_bytes: 1 }
}

fn stopped(reason: StopReason) -> Observation {
    Observation::VmStopped { reason }
}

#[test]
fn instance_cursor_round_trips_through_its_wire_form() {
    let cursor = InstanceCursor {
        last_at: chrono::DateTime::from_timestamp_micros(1_700_000_000_000_001).unwrap(),
        vm_id: waymark_ids::InstanceId::new_uuid_v4(),
    };
    let text = cursor.encode();
    let back = InstanceCursor::decode(&text).expect("a written cursor reads back");
    assert_eq!(back.encode(), text);

    let error = InstanceCursor::decode("nope").expect_err("not a cursor");
    assert_eq!(error.text, "nope");
}

/// Four VMs in four situations, derived from their events: still running
/// on its node; run over and workflow complete; run failed; a run that
/// was cancelled on one node and restarted on another. Plus one known
/// without any run. Listed most recently active first, paged, and read
/// one by one.
#[tokio::test]
async fn instances_are_derived_from_the_vm_driver_events() {
    let store = test_store("observability_store_test_state_instances").await;
    let first = Node::new();
    let second = Node::new();
    let running = waymark_ids::InstanceId::new_uuid_v4();
    let completed = waymark_ids::InstanceId::new_uuid_v4();
    let failed = waymark_ids::InstanceId::new_uuid_v4();
    let hopped = waymark_ids::InstanceId::new_uuid_v4();
    let startless = waymark_ids::InstanceId::new_uuid_v4();

    let events = [
        // completed: start, complete, exhausted stop (t = 100..102)
        first.event(100, completed, 0, Observation::VmStarted),
        first.event(101, completed, 1, complete()),
        first.event(
            102,
            completed,
            2,
            stopped(StopReason::NoReadyFramesOrWaitingPromises),
        ),
        // failed: start, step failure (t = 200..201)
        first.event(200, failed, 0, Observation::VmStarted),
        first.event(
            201,
            failed,
            1,
            stopped(StopReason::Step {
                error: "boom".to_owned(),
            }),
        ),
        // hopped: start + cancel on the first node, restart on the second (t = 300..302)
        first.event(300, hopped, 0, Observation::VmStarted),
        first.event(301, hopped, 1, stopped(StopReason::Cancelled)),
        second.event(302, hopped, 0, Observation::VmStarted),
        // startless: known by a snapshot alone, no run (t = 350)
        second.event(350, startless, 3, snapshot()),
        // running: start + snapshot, no stop (t = 400..401)
        second.event(400, running, 0, Observation::VmStarted),
        second.event(401, running, 1, snapshot()),
    ];
    // Two batches, so the instances are built across appends: everything
    // up to the startless VM, then the running VM's events.
    let (first_batch, second_batch) = events.split_at(9);
    for batch in [first_batch, second_batch] {
        waymark_observability_events_sink_backend::AppendEvents::append_events(
            &store,
            NESlice::try_from_slice(batch).expect("non-empty"),
        )
        .await
        .expect("append events");
    }

    // The list over a range holding everything, most recently active
    // first, two per page.
    let list = |after| async {
        store
            .list_instances(
                waymark_observability_state_query_backend::list_instances::Params {
                    from: at(0),
                    to: at(1_000),
                    limit: waymark_query_limit::Limit::new(2).expect("within the cap"),
                    after,
                },
            )
            .await
            .expect("list instances")
    };
    let page_one = list(None).await.expect("a page");
    let page_two = list(Some(page_one.next)).await.expect("a second page");
    let page_three = list(Some(page_two.next)).await.expect("a third page");
    assert!(
        list(Some(page_three.next)).await.is_none(),
        "nothing past the last page"
    );
    let order: Vec<_> = page_one
        .instances
        .iter()
        .chain(page_two.instances.iter())
        .chain(page_three.instances.iter())
        .map(|instance| instance.vm_id)
        .collect();
    assert_eq!(order, [running, startless, hopped, failed, completed]);

    // The range is over the last activity: `startless` sits on the
    // exclusive end, `running` and `completed` lie outside.
    let store_ref = &store;
    let list_in = move |from_secs, to_secs| async move {
        store_ref
            .list_instances(
                waymark_observability_state_query_backend::list_instances::Params {
                    from: at(from_secs),
                    to: at(to_secs),
                    limit: waymark_query_limit::Limit::new(10).expect("within the cap"),
                    after: None,
                },
            )
            .await
            .expect("list instances")
    };
    let page = list_in(200, 350).await.expect("a page");
    let order: Vec<_> = page.instances.iter().map(|i| i.vm_id).collect();
    assert_eq!(order, [hopped, failed]);

    // `hopped` was last active at 302 on the second node, so a range
    // ending at 301 does not hold it, whatever it did on the first node.
    let page = list_in(0, 301).await.expect("a page");
    let order: Vec<_> = page.instances.iter().map(|i| i.vm_id).collect();
    assert_eq!(order, [failed, completed]);
    assert_eq!(
        page.next.encode(),
        format!("{}/{}", at(102).timestamp_micros(), completed),
        "the cursor is the page's last VM at its last event"
    );

    assert!(list_in(500, 1_000).await.is_none(), "nothing in range");

    // One by one.
    let store_ref = &store;
    let get =
        move |vm_id| async move { store_ref.get_instance(vm_id).await.expect("get instance") };

    let instance = get(running).await.expect("known");
    let run = instance
        .latest_run
        .as_ref()
        .expect("its start was observed");
    assert_eq!(run.node_id, second.node_id);
    assert_eq!(run.started_at.timestamp(), 400);
    assert!(run.stopped.is_none(), "no stop observed");
    assert!(instance.outcome.is_none());
    assert_eq!(instance.last_event.at.timestamp(), 401);
    assert_eq!(instance.last_event.run_sequence, 1);
    assert_eq!(
        instance.last_event.kind,
        waymark_observability_events_payload::vm_driver::Kind::SnapshotPersisted
    );

    let instance = get(completed).await.expect("known");
    let run = instance
        .latest_run
        .as_ref()
        .expect("its start was observed");
    let stop = run.stopped.as_ref().expect("stopped");
    assert_eq!(stop.reason, StopKind::NoReadyFramesOrWaitingPromises);
    assert_eq!(stop.at.timestamp(), 102);
    let outcome = instance.outcome.as_ref().expect("an outcome");
    assert_eq!(outcome.kind, OutcomeKind::Complete);
    assert_eq!(outcome.at.timestamp(), 101);

    let instance = get(failed).await.expect("known");
    let stop = instance
        .latest_run
        .as_ref()
        .unwrap()
        .stopped
        .as_ref()
        .expect("stopped");
    assert_eq!(stop.reason, StopKind::Step);
    assert!(instance.outcome.is_none());

    let instance = get(hopped).await.expect("known");
    let run = instance
        .latest_run
        .as_ref()
        .expect("its start was observed");
    assert_eq!(
        run.node_id, second.node_id,
        "the latest run is on the second node"
    );
    assert_eq!(run.started_at.timestamp(), 302);
    assert!(
        run.stopped.is_none(),
        "the cancel on the first node does not stop the run on the second"
    );

    let instance = get(startless).await.expect("known");
    assert!(instance.latest_run.is_none(), "no run of it");
    assert_eq!(instance.last_event.run_sequence, 3);

    assert!(get(waymark_ids::InstanceId::new_uuid_v4()).await.is_none());
}

/// The list is an index walk over the instances, never a scan.
#[tokio::test]
async fn instance_list_uses_its_index() {
    let store = test_store("observability_store_test_state_list_plan").await;

    // Discourage a sequential scan on the empty table so the plan shows
    // the choice the query shape allows, not the size heuristic.
    sqlx::query("SET enable_seqscan = off")
        .execute(&store.pool)
        .await
        .expect("planner setting");

    let params = waymark_observability_state_query_backend::list_instances::Params {
        from: at(0),
        to: at(1_000),
        limit: waymark_query_limit::Limit::new(10).expect("within the cap"),
        after: Some(InstanceCursor {
            last_at: at(500),
            vm_id: waymark_ids::InstanceId::new_uuid_v4(),
        }),
    };
    let mut query = sqlx::QueryBuilder::new("EXPLAIN ");
    super::push_list_query(&mut query, &params);
    let plan: Vec<String> = query
        .build()
        .fetch_all(&store.pool)
        .await
        .expect("explain")
        .iter()
        .map(|row| sqlx::Row::get(row, 0))
        .collect();

    let plan = plan.join("\n");
    assert!(
        !plan.contains("Seq Scan"),
        "the list must not scan the instances, plan was:\n{plan}"
    );
    assert!(
        plan.contains("observability_vm_instances_last_at_idx"),
        "the list must walk its index, plan was:\n{plan}"
    );
}
