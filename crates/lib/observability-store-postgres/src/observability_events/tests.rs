use nonempty_collections::NESlice;
use sqlx::Row as _;

use crate::test_helpers::test_store;

/// An empty store: the table migrates, the reads answer "nothing", and
/// retention on nothing deletes nothing.
#[tokio::test]
async fn events_empty_store_reads_nothing_and_retains_nothing() {
    let store = test_store("observability_store_test_events").await;
    let limit = waymark_query_limit::Limit::new(10).expect("within the cap");

    let page = waymark_observability_events_query_backend::ListEvents::list_events(
        &store,
        waymark_observability_events_query_backend::list_events::Params {
            from: chrono::DateTime::from_timestamp_secs(0).unwrap(),
            to: chrono::DateTime::from_timestamp_secs(10_000).unwrap(),
            limit,
            after: None,
        },
    )
    .await
    .expect("list events");
    assert!(page.is_none(), "an empty range lists no page");

    let page = waymark_observability_events_query_backend::Tail::tail(
        &store,
        waymark_observability_events_query_backend::tail::Params {
            node_id: waymark_ids::NodeId::new_uuid_v4(),
            limit,
            after: None,
        },
    )
    .await
    .expect("tail events");
    assert!(page.is_none(), "an unknown node tails no page");

    let deleted = waymark_observability_events_retention_backend::ApplyRetention::apply_retention(
        &store,
        chrono::DateTime::from_timestamp_secs(10_000).unwrap(),
    )
    .await
    .expect("apply retention");
    assert_eq!(deleted, 0);
}

/// Two VM driver events go in and come back out through both reads,
/// field for field, with the payload's kind in the `kind` column.
#[tokio::test]
async fn events_round_trip_through_the_store() {
    let store = test_store("observability_store_test_events_round_trip").await;
    let limit = waymark_query_limit::Limit::new(10).expect("within the cap");

    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let counter = waymark_node_sequence::NodeSequenceCounter::new();
    let started_at = chrono::DateTime::from_timestamp_secs(1_000).unwrap();
    let emitted_at = chrono::DateTime::from_timestamp_secs(1_001).unwrap();

    let events = [
        waymark_observability_events_core::Event {
            node_id,
            node_sequence: counter.next(),
            at: started_at,
            payload: waymark_observability_events_payload::Payload::VmDriver(
                waymark_observability_events_payload::vm_driver::Payload {
                    vm_id,
                    run_sequence: 0,
                    observation: waymark_observability_events_payload::vm_driver::Observation::VmStarted,
                },
            ),
        },
        waymark_observability_events_core::Event {
            node_id,
            node_sequence: counter.next(),
            at: emitted_at,
            payload: waymark_observability_events_payload::Payload::VmDriver(
                waymark_observability_events_payload::vm_driver::Payload {
                    vm_id,
                    run_sequence: 1,
                    observation: waymark_observability_events_payload::vm_driver::Observation::EffectEmitted {
                        effect_number: waymark_vm_runtime_effect::EffectNumber(0),
                        effect:
                            waymark_observability_events_payload::vm_driver::EffectSummary::ActionCall {
                                promise_state_id: waymark_vm_runtime_promise_core::PromiseStateId(0),
                                action_name: "fetch".to_owned(),
                                module_name: Some("app.tasks".to_owned()),
                            },
                    },
                },
            ),
        },
    ];

    waymark_observability_events_sink_backend::AppendEvents::append_events(
        &store,
        NESlice::try_from_slice(&events).expect("non-empty"),
    )
    .await
    .expect("append events");

    // The tail: the node's stream, oldest first.
    let page = waymark_observability_events_query_backend::Tail::tail(
        &store,
        waymark_observability_events_query_backend::tail::Params {
            node_id,
            limit,
            after: None,
        },
    )
    .await
    .expect("tail events")
    .expect("a page");
    assert_eq!(page.events.len().get(), 2);

    let first = page.events.first();
    assert_eq!(first.node_id, node_id);
    assert_eq!(first.node_sequence, events[0].node_sequence);
    assert_eq!(first.at, started_at);
    let waymark_observability_events_payload::Payload::VmDriver(payload) = &first.payload;
    assert_eq!(payload.vm_id, vm_id);
    assert_eq!(payload.run_sequence, 0);
    assert!(matches!(
        payload.observation,
        waymark_observability_events_payload::vm_driver::Observation::VmStarted
    ));

    let second = page.events.last();
    assert_eq!(second.node_sequence, events[1].node_sequence);
    assert_eq!(second.at, emitted_at);
    let waymark_observability_events_payload::Payload::VmDriver(payload) = &second.payload;
    assert_eq!(payload.run_sequence, 1);
    let waymark_observability_events_payload::vm_driver::Observation::EffectEmitted {
        effect_number,
        effect,
    } = &payload.observation
    else {
        panic!("expected an effect, got {:?}", payload.observation);
    };
    assert_eq!(*effect_number, waymark_vm_runtime_effect::EffectNumber(0));
    let waymark_observability_events_payload::vm_driver::EffectSummary::ActionCall {
        promise_state_id,
        action_name,
        module_name,
    } = effect
    else {
        panic!("expected an action call, got {effect:?}");
    };
    assert_eq!(
        *promise_state_id,
        waymark_vm_runtime_promise_core::PromiseStateId(0)
    );
    assert_eq!(action_name, "fetch");
    assert_eq!(module_name.as_deref(), Some("app.tasks"));

    // The list: time-merged, newest first.
    let page = waymark_observability_events_query_backend::ListEvents::list_events(
        &store,
        waymark_observability_events_query_backend::list_events::Params {
            from: chrono::DateTime::from_timestamp_secs(0).unwrap(),
            to: chrono::DateTime::from_timestamp_secs(10_000).unwrap(),
            limit,
            after: None,
        },
    )
    .await
    .expect("list events")
    .expect("a page");
    assert_eq!(page.events.len().get(), 2);
    assert_eq!(page.events.first().at, emitted_at);
    assert_eq!(page.events.last().at, started_at);

    // The column holds the tag, as stored.
    let kinds: Vec<String> = sqlx::query(
        r#"
        SELECT kind
        FROM observability_events
        ORDER BY node_sequence
        "#,
    )
    .fetch_all(&store.pool)
    .await
    .expect("select kinds")
    .into_iter()
    .map(|row| row.get("kind"))
    .collect();
    assert_eq!(
        kinds,
        [
            "vm_driver.vm_started",
            "vm_driver.effect_emitted.action_call",
        ]
    );
}

/// A VM's events, made from one hook per call: the same shape the hooks
/// emit, with the positions and times the test chooses.
fn vm_driver_event(
    node_id: waymark_ids::NodeId,
    counter: &waymark_node_sequence::NodeSequenceCounter,
    at_secs: i64,
    vm_id: waymark_ids::InstanceId,
    run_sequence: u64,
    observation: waymark_observability_events_payload::vm_driver::Observation,
) -> waymark_observability_events_core::Event<
    waymark_ids::NodeId,
    waymark_observability_events_payload::Payload,
> {
    waymark_observability_events_core::Event {
        node_id,
        node_sequence: counter.next(),
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

/// One VM's timeline is time-merged across the nodes it ran on, oldest
/// first, and pages by position; other VMs' events never appear in it.
#[tokio::test]
async fn vm_timeline_merges_the_nodes_a_vm_ran_on_oldest_first() {
    let store = test_store("observability_store_test_events_vm_timeline").await;
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let other_vm = waymark_ids::InstanceId::new_uuid_v4();
    let first_node = waymark_ids::NodeId::new_uuid_v4();
    let second_node = waymark_ids::NodeId::new_uuid_v4();
    let first_counter = waymark_node_sequence::NodeSequenceCounter::new();
    let second_counter = waymark_node_sequence::NodeSequenceCounter::new();

    // The VM ran on the first node (positions 0..2), then on the second
    // (positions 0..1), with another VM interleaved on the first node.
    let events = [
        vm_driver_event(first_node, &first_counter, 1_000, vm_id, 0,
            waymark_observability_events_payload::vm_driver::Observation::VmStarted),
        vm_driver_event(first_node, &first_counter, 1_001, other_vm, 0,
            waymark_observability_events_payload::vm_driver::Observation::VmStarted),
        vm_driver_event(first_node, &first_counter, 1_002, vm_id, 1,
            waymark_observability_events_payload::vm_driver::Observation::SnapshotPersisted { size_in_bytes: 10 }),
        vm_driver_event(first_node, &first_counter, 1_003, vm_id, 2,
            waymark_observability_events_payload::vm_driver::Observation::VmStopped {
                reason: waymark_observability_events_payload::vm_driver::StopReason::Cancelled,
            }),
        vm_driver_event(second_node, &second_counter, 1_004, vm_id, 0,
            waymark_observability_events_payload::vm_driver::Observation::VmStarted),
        vm_driver_event(second_node, &second_counter, 1_005, vm_id, 1,
            waymark_observability_events_payload::vm_driver::Observation::VmStopped {
                reason: waymark_observability_events_payload::vm_driver::StopReason::NoReadyFramesOrWaitingPromises,
            }),
    ];
    waymark_observability_events_sink_backend::AppendEvents::append_events(
        &store,
        NESlice::try_from_slice(&events).expect("non-empty"),
    )
    .await
    .expect("append events");

    let read = |after| async {
        waymark_observability_events_query_backend::VmTimeline::vm_timeline(
            &store,
            waymark_observability_events_query_backend::vm_timeline::Params {
                vm_id,
                limit: waymark_query_limit::Limit::new(3).expect("within the cap"),
                after,
            },
        )
        .await
        .expect("timeline")
    };

    let first_page = read(None).await.expect("a page");
    let second_page = read(Some(first_page.next)).await.expect("a second page");
    assert!(
        read(Some(second_page.next)).await.is_none(),
        "nothing past the last page"
    );

    let seen: Vec<_> = first_page
        .events
        .iter()
        .chain(second_page.events.iter())
        .map(|event| {
            let waymark_observability_events_payload::Payload::VmDriver(payload) = &event.payload;
            assert_eq!(payload.vm_id, vm_id, "only this VM's events");
            (
                event.node_id == first_node,
                payload.run_sequence,
                event.at.timestamp(),
            )
        })
        .collect();
    assert_eq!(
        seen,
        [
            (true, 0, 1_000),
            (true, 1, 1_002),
            (true, 2, 1_003),
            (false, 0, 1_004),
            (false, 1, 1_005),
        ]
    );

    assert!(
        read_for(&store, other_vm).await.is_some(),
        "the other VM has its own timeline"
    );
    assert!(
        read_for(&store, waymark_ids::InstanceId::new_uuid_v4())
            .await
            .is_none(),
        "an unknown VM has none"
    );
}

async fn read_for(
    store: &crate::Store,
    vm_id: waymark_ids::InstanceId,
) -> Option<
    waymark_observability_events_query_backend::PageFor<crate::Store, crate::VmTimelineCursor>,
> {
    waymark_observability_events_query_backend::VmTimeline::vm_timeline(
        store,
        waymark_observability_events_query_backend::vm_timeline::Params {
            vm_id,
            limit: waymark_query_limit::Limit::new(10).expect("within the cap"),
            after: None,
        },
    )
    .await
    .expect("timeline")
}

/// The timeline read spells the index's expression and predicate, so the
/// planner takes the index: a query that drifted from them would scan.
#[tokio::test]
async fn vm_timeline_read_uses_its_index() {
    let store = test_store("observability_store_test_events_vm_timeline_plan").await;
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();

    // Discourage a sequential scan on the empty table so the plan shows
    // the choice the query shape allows, not the size heuristic.
    sqlx::query("SET enable_seqscan = off")
        .execute(&store.pool)
        .await
        .expect("planner setting");
    let plan: Vec<String> = sqlx::query_scalar(&format!(
        r#"
        EXPLAIN SELECT {}
        FROM observability_events
        WHERE {} AND {} = $1
        ORDER BY at, node_id, node_sequence
        "#,
        super::common::EVENT_COLUMNS,
        super::common::VM_ID_PRESENT,
        super::common::VM_ID_EXPRESSION,
    ))
    .bind(vm_id)
    .fetch_all(&store.pool)
    .await
    .expect("explain");

    let plan = plan.join("\n");
    assert!(
        plan.contains("observability_events_vm_timeline_idx"),
        "the timeline read must use its index, plan was:\n{plan}"
    );
}
