use crate::test_helpers::test_store;

/// The production payload is uninhabited until the first source lands,
/// so no row can be written yet: what the store can prove is that the
/// table migrates, the reads answer "nothing", and retention on nothing
/// deletes nothing. The row round trip arrives with the first kind.
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
