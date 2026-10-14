use nonempty_collections::NESlice;
use sqlx::Row as _;

use crate::test_helpers::test_store;

/// A run's start, of a fresh executable.
fn vm_started() -> waymark_observability_events_payload::vm_driver::Observation {
    waymark_observability_events_payload::vm_driver::Observation::VmStarted {
        executable_id: waymark_ids::WorkflowVersionId::new_uuid_v4(),
        executable_name: "test_wf".to_owned(),
        executable_version: "v1".to_owned(),
    }
}

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

    // The same reads past a cursor: the keyset conditions run too.
    let list_cursor = <super::query::ListCursor as waymark_cursor_core::DecodeCursor>::decode(
        &format!("1000000/{}/7", waymark_ids::NodeId::new_uuid_v4()),
    )
    .expect("a list cursor");
    let page = waymark_observability_events_query_backend::ListEvents::list_events(
        &store,
        waymark_observability_events_query_backend::list_events::Params {
            from: chrono::DateTime::from_timestamp_secs(0).unwrap(),
            to: chrono::DateTime::from_timestamp_secs(10_000).unwrap(),
            limit,
            after: Some(list_cursor),
        },
    )
    .await
    .expect("list events past a cursor");
    assert!(page.is_none(), "an empty range lists no page past a cursor");

    let tail_cursor = <super::query::TailCursor as waymark_cursor_core::DecodeCursor>::decode("7")
        .expect("a tail cursor");
    let page = waymark_observability_events_query_backend::Tail::tail(
        &store,
        waymark_observability_events_query_backend::tail::Params {
            node_id: waymark_ids::NodeId::new_uuid_v4(),
            limit,
            after: Some(tail_cursor),
        },
    )
    .await
    .expect("tail events past a cursor");
    assert!(
        page.is_none(),
        "an unknown node tails no page past a cursor"
    );

    let deleted = waymark_observability_events_retention_backend::ApplyRetention::apply_retention(
        &store,
        chrono::DateTime::from_timestamp_secs(10_000).unwrap(),
    )
    .await
    .expect("apply retention");
    assert_eq!(deleted, 0);
}

/// Two VM driver events go in and come back out: field for field
/// through `tail`, newest first through `list_events`, with the
/// payload's kind in the `kind` column.
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
                    observation: vm_started(),
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
    let waymark_observability_events_payload::Payload::VmDriver(payload) = &first.payload else {
        panic!("a VM driver event: {:?}", first.payload);
    };
    assert_eq!(payload.vm_id, vm_id);
    assert_eq!(payload.run_sequence, 0);
    assert!(matches!(
        payload.observation,
        waymark_observability_events_payload::vm_driver::Observation::VmStarted { .. }
    ));

    let second = page.events.last();
    assert_eq!(second.node_sequence, events[1].node_sequence);
    assert_eq!(second.at, emitted_at);
    let waymark_observability_events_payload::Payload::VmDriver(payload) = &second.payload else {
        panic!("a VM driver event: {:?}", second.payload);
    };
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
/// first, and pages by cursor; other VMs' events never appear in it.
#[tokio::test]
async fn vm_timeline_merges_the_nodes_a_vm_ran_on_oldest_first() {
    let store = test_store("observability_store_test_events_vm_timeline").await;
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let other_vm = waymark_ids::InstanceId::new_uuid_v4();
    // The two nodes in the store's order, so a tie on the time falls to
    // the first node.
    let one_node = waymark_ids::NodeId::new_uuid_v4();
    let other_node = waymark_ids::NodeId::new_uuid_v4();
    let first_node = one_node.min(other_node);
    let second_node = one_node.max(other_node);
    let first_counter = waymark_node_sequence::NodeSequenceCounter::new();
    let second_counter = waymark_node_sequence::NodeSequenceCounter::new();

    // The VM ran on the first node (positions 0..2), then on the second
    // (positions 0..1), with another VM interleaved on the first node.
    // Three of its events share one instant — two on the first node, one
    // on the second — so the order falls to the node, then to the stream
    // position, and the first page ends inside the tie.
    let events = [
        vm_driver_event(first_node, &first_counter, 1_000, vm_id, 0,
            vm_started()),
        vm_driver_event(first_node, &first_counter, 1_001, other_vm, 0,
            vm_started()),
        vm_driver_event(first_node, &first_counter, 1_002, vm_id, 1,
            waymark_observability_events_payload::vm_driver::Observation::SnapshotPersisted { size_in_bytes: 10 }),
        vm_driver_event(first_node, &first_counter, 1_002, vm_id, 2,
            waymark_observability_events_payload::vm_driver::Observation::VmStopped {
                reason: waymark_observability_events_payload::vm_driver::StopReason::Cancelled,
            }),
        vm_driver_event(second_node, &second_counter, 1_002, vm_id, 0,
            vm_started()),
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
            let waymark_observability_events_payload::Payload::VmDriver(payload) = &event.payload
            else {
                panic!("a VM driver event: {:?}", event.payload);
            };
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
            (true, 2, 1_002),
            (false, 0, 1_002),
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
    // the choice the query shape allows, not the size heuristic. The
    // setting is per session, so the EXPLAIN runs on the same connection,
    // and `test_store` gives this test a pool of its own, so no other
    // test sees it.
    let mut connection = store.pool.acquire().await.expect("connection");
    sqlx::query("SET enable_seqscan = off")
        .execute(&mut *connection)
        .await
        .expect("planner setting");
    let plan: Vec<String> = sqlx::query_scalar(const_format::formatcp!(
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
    .fetch_all(&mut *connection)
    .await
    .expect("explain");

    let plan = plan.join("\n");
    assert!(
        plan.contains("observability_events_vm_timeline_idx"),
        "the timeline read must use its index, plan was:\n{plan}"
    );
    assert!(
        !plan.contains("Sort"),
        "the index must serve the order too, plan was:\n{plan}"
    );
}

/// A sweep deletes in chunks: with a chunk of three, all seven rows
/// before the cutoff go, and the two rows past the cutoff stay.
#[tokio::test]
async fn retention_deletes_in_chunks() {
    let store = test_store("observability_store_test_events_retention_chunks").await;
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let counter = waymark_node_sequence::NodeSequenceCounter::new();
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let events: Vec<_> = (0..9)
        .map(|second| waymark_observability_events_core::Event {
            node_id,
            node_sequence: counter.next(),
            at: chrono::DateTime::from_timestamp_secs(second).unwrap(),
            payload: waymark_observability_events_payload::Payload::VmDriver(
                waymark_observability_events_payload::vm_driver::Payload {
                    vm_id,
                    run_sequence: 0,
                    observation: vm_started(),
                },
            ),
        })
        .collect();
    waymark_observability_events_sink_backend::AppendEvents::append_events(
        &store,
        NESlice::try_from_slice(&events).expect("non-empty"),
    )
    .await
    .expect("append events");

    let deleted = crate::common::delete_before_in_chunks!(
        &store.pool,
        "observability_events",
        "at",
        chrono::DateTime::from_timestamp_secs(7).unwrap(),
        std::num::NonZeroU32::new(3).unwrap(),
    )
    .await
    .expect("delete in chunks");
    assert_eq!(deleted, 7);

    let (remaining,): (i64,) = sqlx::query_as("SELECT count(*) FROM observability_events")
        .fetch_one(&store.pool)
        .await
        .expect("count");
    assert_eq!(remaining, 2);
}

/// Retention sweeps the instances with the events: a VM whose every event
/// is before the cutoff loses its instance, a VM with an event past it
/// keeps its own, and the count returned is the events'.
#[tokio::test]
async fn retention_sweeps_the_instances_the_events_no_longer_speak_for() {
    let store = test_store("observability_store_test_events_retention_instances").await;
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let counter = waymark_node_sequence::NodeSequenceCounter::new();
    let swept_vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let kept_vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let events: Vec<_> = [
        (swept_vm_id, 1),
        (swept_vm_id, 2),
        (kept_vm_id, 3),
        (kept_vm_id, 20),
    ]
    .into_iter()
    .map(|(vm_id, second)| vm_driver_event(node_id, &counter, second, vm_id, 0, vm_started()))
    .collect();
    waymark_observability_events_sink_backend::AppendEvents::append_events(
        &store,
        NESlice::try_from_slice(&events).expect("non-empty"),
    )
    .await
    .expect("append events");

    let deleted = waymark_observability_events_retention_backend::ApplyRetention::apply_retention(
        &store,
        chrono::DateTime::from_timestamp_secs(10).unwrap(),
    )
    .await
    .expect("apply retention");
    assert_eq!(deleted, 3);

    let swept =
        waymark_observability_state_query_backend::GetInstance::get_instance(&store, swept_vm_id)
            .await
            .expect("get the swept instance");
    assert!(swept.is_none(), "the swept VM's instance goes");

    let kept =
        waymark_observability_state_query_backend::GetInstance::get_instance(&store, kept_vm_id)
            .await
            .expect("get the kept instance");
    assert!(kept.is_some(), "the kept VM's instance stays");
}

/// A run's start of the given executable.
fn vm_started_with(
    executable_id: waymark_ids::WorkflowVersionId,
    name: &str,
) -> waymark_observability_events_payload::vm_driver::Observation {
    waymark_observability_events_payload::vm_driver::Observation::VmStarted {
        executable_id,
        executable_name: name.to_owned(),
        executable_version: "v1".to_owned(),
    }
}

/// The bridge's registration of the given executable.
fn executable_registered(
    node_id: waymark_ids::NodeId,
    counter: &waymark_node_sequence::NodeSequenceCounter,
    at_secs: i64,
    executable_id: waymark_ids::WorkflowVersionId,
    name: &str,
) -> waymark_observability_events_core::Event<
    waymark_ids::NodeId,
    waymark_observability_events_payload::Payload,
> {
    waymark_observability_events_core::Event {
        node_id,
        node_sequence: counter.next(),
        at: chrono::DateTime::from_timestamp_secs(at_secs).unwrap(),
        payload: waymark_observability_events_payload::Payload::Bridge(
            waymark_observability_events_payload::bridge::Payload {
                observation:
                    waymark_observability_events_payload::bridge::Observation::ExecutableRegistered {
                        executable_id,
                        executable_name: name.to_owned(),
                        executable_version: "v1".to_owned(),
                    },
            },
        ),
    }
}

/// The executables table: `(id, name, first_at, last_at)` by id text.
async fn executables(store: &crate::Store) -> Vec<(String, String, i64, i64)> {
    sqlx::query(
        "SELECT id::text, name, first_at, last_at FROM observability_vm_executables ORDER BY id::text",
    )
    .fetch_all(&store.pool)
    .await
    .expect("read the executables")
    .into_iter()
    .map(|row| {
        let first_at: chrono::DateTime<chrono::Utc> = row.get("first_at");
        let last_at: chrono::DateTime<chrono::Utc> = row.get("last_at");
        (
            row.get("id"),
            row.get("name"),
            first_at.timestamp(),
            last_at.timestamp(),
        )
    })
    .collect()
}

/// An instance's executable, as the instances table has it: `None` when
/// the instance has no row.
async fn instance_executable(
    store: &crate::Store,
    vm_id: waymark_ids::InstanceId,
) -> Option<Option<String>> {
    sqlx::query("SELECT executable_id::text FROM observability_vm_instances WHERE vm_id::text = $1")
        .bind(vm_id.to_string())
        .fetch_optional(&store.pool)
        .await
        .expect("read the instance")
        .map(|row| row.get("executable_id"))
}

/// The executables are kept from the events that carry them, from both
/// sources: a row per executable with `first_at`/`last_at` widening over
/// the batches, a late older batch included; the run step's executable
/// follows the newest start and never an older one arriving later; and
/// the sweep clears an executable no event has carried since the cutoff
/// only once no instance refers to it.
#[tokio::test]
async fn executables_are_kept_from_the_events_that_carry_them() {
    let store = test_store("observability_store_test_events_executables").await;
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let counter = waymark_node_sequence::NodeSequenceCounter::new();
    let [x, y, z, w] = std::array::from_fn(|_| waymark_ids::WorkflowVersionId::new_uuid_v4());
    let [a, b, c, d] = std::array::from_fn(|_| waymark_ids::InstanceId::new_uuid_v4());
    let append = |events: Vec<_>| {
        let store = &store;
        async move {
            waymark_observability_events_sink_backend::AppendEvents::append_events(
                store,
                NESlice::try_from_slice(&events).expect("non-empty"),
            )
            .await
            .expect("append events");
        }
    };
    let row = |id: waymark_ids::WorkflowVersionId, name: &str, first: i64, last: i64| {
        (id.to_string(), name.to_owned(), first, last)
    };
    let sorted = |mut rows: Vec<(String, String, i64, i64)>| {
        rows.sort();
        rows
    };

    // Four starts, four executables: one row each, at the start's time.
    append(vec![
        vm_driver_event(node_id, &counter, 1, c, 0, vm_started_with(z, "z")),
        vm_driver_event(node_id, &counter, 2, d, 0, vm_started_with(w, "w")),
        vm_driver_event(node_id, &counter, 10, a, 0, vm_started_with(x, "x")),
        vm_driver_event(node_id, &counter, 12, b, 0, vm_started_with(y, "y")),
    ])
    .await;
    assert_eq!(
        executables(&store).await,
        sorted(vec![
            row(x, "x", 10, 10),
            row(y, "y", 12, 12),
            row(z, "z", 1, 1),
            row(w, "w", 2, 2),
        ]),
    );

    // A registration through the bridge widens `last_at`; the event reads
    // back under its own source and makes no instance.
    append(vec![executable_registered(node_id, &counter, 20, x, "x")]).await;
    assert_eq!(
        executables(&store).await,
        sorted(vec![
            row(x, "x", 10, 20),
            row(y, "y", 12, 12),
            row(z, "z", 1, 1),
            row(w, "w", 2, 2),
        ]),
    );
    let page = waymark_observability_events_query_backend::ListEvents::list_events(
        &store,
        waymark_observability_events_query_backend::list_events::Params {
            from: chrono::DateTime::from_timestamp_secs(20).unwrap(),
            to: chrono::DateTime::from_timestamp_secs(21).unwrap(),
            limit: waymark_query_limit::Limit::new(10).expect("within the cap"),
            after: None,
        },
    )
    .await
    .expect("list events")
    .expect("the registration lists");
    assert!(matches!(
        page.events.first().payload,
        waymark_observability_events_payload::Payload::Bridge(_)
    ));
    let (instances,): (i64,) = sqlx::query_as("SELECT count(*) FROM observability_vm_instances")
        .fetch_one(&store.pool)
        .await
        .expect("count the instances");
    assert_eq!(instances, 4, "a registration makes no instance");

    // A late older batch widens `first_at` only.
    append(vec![executable_registered(node_id, &counter, 5, x, "x")]).await;
    assert_eq!(
        executables(&store).await,
        sorted(vec![
            row(x, "x", 5, 20),
            row(y, "y", 12, 12),
            row(z, "z", 1, 1),
            row(w, "w", 2, 2),
        ]),
    );

    // The run step takes the newest start of the batch: `a` ran `x` again
    // and then `y`, so its executable is `y`. `c` goes on without a start,
    // so its instance outlives the last event carrying `z`.
    append(vec![
        vm_driver_event(node_id, &counter, 25, a, 0, vm_started_with(x, "x")),
        vm_driver_event(node_id, &counter, 30, a, 0, vm_started_with(y, "y")),
        vm_driver_event(
            node_id,
            &counter,
            50,
            c,
            1,
            waymark_observability_events_payload::vm_driver::Observation::VmStopped {
                reason: waymark_observability_events_payload::vm_driver::StopReason::Cancelled,
            },
        ),
    ])
    .await;
    assert_eq!(
        instance_executable(&store, a).await,
        Some(Some(y.to_string()))
    );

    // An older start arriving later does not take the run step back.
    append(vec![vm_driver_event(
        node_id,
        &counter,
        28,
        a,
        0,
        vm_started_with(x, "x"),
    )])
    .await;
    assert_eq!(
        instance_executable(&store, a).await,
        Some(Some(y.to_string()))
    );
    assert_eq!(
        executables(&store).await,
        sorted(vec![
            row(x, "x", 5, 28),
            row(y, "y", 12, 30),
            row(z, "z", 1, 1),
            row(w, "w", 2, 2),
        ]),
    );

    // The sweep: `w`'s only carrier and `d`'s only event are before the
    // cutoff, so `d` goes and then `w`; `z`'s only carrier is too, but
    // `c` still refers to it, so it stays.
    let deleted = waymark_observability_events_retention_backend::ApplyRetention::apply_retention(
        &store,
        chrono::DateTime::from_timestamp_secs(15).unwrap(),
    )
    .await
    .expect("apply retention");
    assert_eq!(deleted, 5, "the events before the cutoff");
    assert_eq!(instance_executable(&store, b).await, None, "`b` is swept");
    assert_eq!(instance_executable(&store, d).await, None, "`d` is swept");
    assert_eq!(
        instance_executable(&store, c).await,
        Some(Some(z.to_string()))
    );
    assert_eq!(
        executables(&store).await,
        sorted(vec![
            row(x, "x", 5, 28),
            row(y, "y", 12, 30),
            row(z, "z", 1, 1),
        ]),
    );
}

/// Rows sharing one `at` across a chunk boundary all go: with a chunk of
/// three, all seven rows at the same second before the cutoff go, and
/// the two rows past the cutoff stay.
#[tokio::test]
async fn retention_chunks_split_rows_sharing_a_timestamp() {
    let store = test_store("observability_store_test_events_retention_chunk_ties").await;
    let node_id = waymark_ids::NodeId::new_uuid_v4();
    let counter = waymark_node_sequence::NodeSequenceCounter::new();
    let vm_id = waymark_ids::InstanceId::new_uuid_v4();
    let events: Vec<_> = std::iter::repeat_n(1, 7)
        .chain(std::iter::repeat_n(9, 2))
        .map(|second| waymark_observability_events_core::Event {
            node_id,
            node_sequence: counter.next(),
            at: chrono::DateTime::from_timestamp_secs(second).unwrap(),
            payload: waymark_observability_events_payload::Payload::VmDriver(
                waymark_observability_events_payload::vm_driver::Payload {
                    vm_id,
                    run_sequence: 0,
                    observation: vm_started(),
                },
            ),
        })
        .collect();
    waymark_observability_events_sink_backend::AppendEvents::append_events(
        &store,
        NESlice::try_from_slice(&events).expect("non-empty"),
    )
    .await
    .expect("append events");

    let deleted = crate::common::delete_before_in_chunks!(
        &store.pool,
        "observability_events",
        "at",
        chrono::DateTime::from_timestamp_secs(5).unwrap(),
        std::num::NonZeroU32::new(3).unwrap(),
    )
    .await
    .expect("delete in chunks");
    assert_eq!(deleted, 7);

    let (remaining,): (i64,) = sqlx::query_as("SELECT count(*) FROM observability_events")
        .fetch_one(&store.pool)
        .await
        .expect("count");
    assert_eq!(remaining, 2);
}
