use std::num::NonZeroUsize;

use crate::test_helpers::test_store;

/// The production payload is uninhabited until the first source lands,
/// so no row can be written yet: what the store can prove is that the
/// table migrates, the reads answer "nothing", and retention on nothing
/// deletes nothing. The row round trip arrives with the first kind.
#[tokio::test]
async fn events_empty_store_reads_nothing_and_retains_nothing() {
    let store = test_store("observability_store_test_events").await;
    let limit = NonZeroUsize::new(10).expect("non-zero");

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
