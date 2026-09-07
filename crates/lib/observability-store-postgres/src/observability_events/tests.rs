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
