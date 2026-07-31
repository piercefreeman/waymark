use std::time::Duration;

use tokio_util::sync::CancellationToken;

use super::ActiveSet;

fn instant() -> tokio::time::Instant {
    tokio::time::Instant::now()
}

#[tokio::test]
async fn tracking_counts_fenced_and_unfenced_alike() {
    let mut active = ActiveSet::new();
    assert_eq!(active.tracked_count(), 0);

    let fence = CancellationToken::new();
    active.track_newly_pinned(1u64, instant(), fence.clone());
    active.track_newly_pinned(2u64, instant(), CancellationToken::new());
    assert_eq!(active.tracked_count(), 2);

    // Fencing alone must not stop tracking.
    active.fence_lost_pinning(&1u64);
    assert!(fence.is_cancelled());
    assert_eq!(active.tracked_count(), 2);
}

#[tokio::test]
async fn stopping_tracking_fences_the_workload() {
    let mut active = ActiveSet::new();
    let fence = CancellationToken::new();
    active.track_newly_pinned(1u64, instant(), fence.clone());

    active.fence_and_stop_tracking(&1u64);

    assert!(
        fence.is_cancelled(),
        "dropping a workload must fence it — it is no longer refreshed"
    );
    assert_eq!(active.tracked_count(), 0);
}

#[tokio::test]
async fn surrendering_the_ids_fences_every_workload() {
    let mut active = ActiveSet::new();
    let fence1 = CancellationToken::new();
    let fence2 = CancellationToken::new();
    active.track_newly_pinned(1u64, instant(), fence1.clone());
    active.track_newly_pinned(2u64, instant(), fence2.clone());

    let ids = active.fence_all_and_into_ids();

    assert!(
        fence1.is_cancelled() && fence2.is_cancelled(),
        "every holder must be fenced before the caller releases the pinnings"
    );
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&1u64) && ids.contains(&2u64));
}

#[tokio::test]
async fn fenced_workloads_are_left_out_of_refresh() {
    let mut active = ActiveSet::new();
    active.track_newly_pinned(1u64, instant(), CancellationToken::new());
    active.track_newly_pinned(2u64, instant(), CancellationToken::new());

    active.fence_lost_pinning(&1u64);

    let ids = active.ids_needing_refresh().expect("id 2 still needs it");
    let ids: Vec<u64> = ids.into_iter().collect();
    assert_eq!(ids, vec![2u64], "the fenced pinning is left to lapse");
}

#[tokio::test]
async fn refresh_ids_are_none_when_everything_is_fenced() {
    let mut active = ActiveSet::new();
    active.track_newly_pinned(1u64, instant(), CancellationToken::new());
    active.fence_lost_pinning(&1u64);

    assert!(active.ids_needing_refresh().is_none());
    assert_eq!(
        active.tracked_count(),
        1,
        "still tracked, just not refreshed"
    );
}

#[tokio::test]
async fn the_earliest_deadline_skips_fenced_workloads() {
    let mut active = ActiveSet::new();
    let now = instant();
    let soon = now + Duration::from_secs(1);
    let later = now + Duration::from_secs(10);

    active.track_newly_pinned(1u64, soon, CancellationToken::new());
    active.track_newly_pinned(2u64, later, CancellationToken::new());
    assert_eq!(active.earliest_lapse_deadline(), Some(soon));

    // Once the soonest one is fenced its deadline no longer counts.
    active.fence_lost_pinning(&1u64);
    assert_eq!(active.earliest_lapse_deadline(), Some(later));

    active.fence_lost_pinning(&2u64);
    assert_eq!(active.earliest_lapse_deadline(), None);
}

#[tokio::test]
async fn a_confirmed_refresh_pushes_the_deadline_out() {
    let mut active = ActiveSet::new();
    let now = instant();
    active.track_newly_pinned(1u64, now, CancellationToken::new());

    let extended = now + Duration::from_secs(5);
    active.extend_after_confirmed_refresh(&1u64, extended);

    assert_eq!(active.earliest_lapse_deadline(), Some(extended));
}

#[tokio::test]
async fn extending_an_untracked_workload_is_a_no_op() {
    let mut active = ActiveSet::new();

    // Evicted while the refresh was in flight — must not resurrect it.
    active.extend_after_confirmed_refresh(&1u64, instant());

    assert_eq!(active.tracked_count(), 0);
}

#[tokio::test]
async fn only_lapsed_pinnings_are_fenced() {
    let mut active = ActiveSet::new();
    let now = instant();
    let lapsed = CancellationToken::new();
    let standing = CancellationToken::new();

    active.track_newly_pinned(1u64, now, lapsed.clone());
    active.track_newly_pinned(2u64, now + Duration::from_secs(10), standing.clone());

    active.fence_lapsed_pinnings(now);

    assert!(lapsed.is_cancelled(), "the deadline passed");
    assert!(!standing.is_cancelled(), "this one still has time");
    assert_eq!(active.tracked_count(), 2, "fencing keeps them tracked");
}
