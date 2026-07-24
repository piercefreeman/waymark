//! Unit tests for the deduplicating write: fold semantics (keep, replace,
//! content-chosen), pairwise fold order, key-first-seen flush order,
//! post-flush delivery of folded-out outputs, and settling folded
//! outputs against their winner's flush output.

use std::sync::Arc;

use nonempty_collections::NEVec;

use super::{ConflictResolvedToken, ConflictedSlot};
use crate::test_helpers::{nz, secs};
use crate::{BatcherHandle, Policy, deduplicating_write_batcher};

/// Items are `(key, value)`; outputs are `i64`: a flushed representative
/// gets `value * 10`, a folded-out item gets `-value` — so every output
/// names the item it belongs to.
type Item = (u32, i64);

fn key(item: &Item) -> u32 {
    item.0
}

fn folded_out(value: i64) -> i64 {
    -value
}

fn flushed(value: i64) -> i64 {
    value * 10
}

/// First-wins: the incumbent always stays.
struct FirstWins;

impl super::ConflictResolver<Item, i64> for FirstWins {
    type Placeholder = i64;

    fn resolve_conflict<'a>(
        &self,
        slot: ConflictedSlot<'a, Item, i64, i64>,
        newcomer: Item,
    ) -> ConflictResolvedToken<'a> {
        slot.keep(folded_out(newcomer.1))
    }

    fn settle_conflict(&self, conflicted_out: i64, _winner_out: &i64) -> i64 {
        conflicted_out
    }
}

/// Last-wins: the newcomer always takes the slot.
struct LastWins;

impl super::ConflictResolver<Item, i64> for LastWins {
    type Placeholder = i64;

    fn resolve_conflict<'a>(
        &self,
        slot: ConflictedSlot<'a, Item, i64, i64>,
        newcomer: Item,
    ) -> ConflictResolvedToken<'a> {
        let (incumbent, resolving) = slot.replace(newcomer);
        resolving.resolve(folded_out(incumbent.1))
    }

    fn settle_conflict(&self, conflicted_out: i64, _winner_out: &i64) -> i64 {
        conflicted_out
    }
}

/// Content-chosen: the larger value wins, regardless of arrival order.
struct LargestWins;

impl super::ConflictResolver<Item, i64> for LargestWins {
    type Placeholder = i64;

    fn resolve_conflict<'a>(
        &self,
        slot: ConflictedSlot<'a, Item, i64, i64>,
        newcomer: Item,
    ) -> ConflictResolvedToken<'a> {
        if slot.incumbent().1 >= newcomer.1 {
            slot.keep(folded_out(newcomer.1))
        } else {
            let (incumbent, resolving) = slot.replace(newcomer);
            resolving.resolve(folded_out(incumbent.1))
        }
    }

    fn settle_conflict(&self, conflicted_out: i64, _winner_out: &i64) -> i64 {
        conflicted_out
    }
}

/// First-wins whose settle adds the winner's flush output to the folded
/// output — so a test can assert exactly which winner a folded item was
/// settled against.
struct FirstWinsSettling;

impl super::ConflictResolver<Item, i64> for FirstWinsSettling {
    type Placeholder = i64;

    fn resolve_conflict<'a>(
        &self,
        slot: ConflictedSlot<'a, Item, i64, i64>,
        newcomer: Item,
    ) -> ConflictResolvedToken<'a> {
        slot.keep(folded_out(newcomer.1))
    }

    fn settle_conflict(&self, conflicted_out: i64, winner_out: &i64) -> i64 {
        conflicted_out + *winner_out
    }
}

/// Last-wins with the same settling arithmetic as [`FirstWinsSettling`].
struct LastWinsSettling;

impl super::ConflictResolver<Item, i64> for LastWinsSettling {
    type Placeholder = i64;

    fn resolve_conflict<'a>(
        &self,
        slot: ConflictedSlot<'a, Item, i64, i64>,
        newcomer: Item,
    ) -> ConflictResolvedToken<'a> {
        let (incumbent, resolving) = slot.replace(newcomer);
        resolving.resolve(folded_out(incumbent.1))
    }

    fn settle_conflict(&self, conflicted_out: i64, winner_out: &i64) -> i64 {
        conflicted_out + *winner_out
    }
}

/// A batcher that only ever flushes on the size trigger, recording each
/// flush input's `(key, value)` pairs.
fn spawn_size_gated<ConflictResolver>(
    resolver: ConflictResolver,
    max_batch: usize,
    statements: &Arc<std::sync::Mutex<Vec<Vec<Item>>>>,
) -> BatcherHandle<Item, i64>
where
    ConflictResolver: super::ConflictResolver<Item, i64> + Send + 'static,
    ConflictResolver::Placeholder: Send,
{
    let statements = Arc::clone(statements);
    let (handle, batcher) = deduplicating_write_batcher(
        Policy {
            max_batch: nz(max_batch),
            max_delay: secs(3600),
        },
        key,
        resolver,
        move |batch: NEVec<Item>| {
            statements
                .lock()
                .expect("lock")
                .push(batch.iter().copied().collect());
            let outputs: Vec<i64> = batch.into_iter().map(|(_, value)| flushed(value)).collect();
            std::future::ready(NEVec::try_from_vec(outputs).expect("batch was non-empty"))
        },
        std::future::pending(),
    );
    tokio::spawn(batcher);
    handle
}

/// Submit items concurrently from one task, preserving listing order.
macro_rules! submit_all {
    ($handle:expr, $($item:expr),+ $(,)?) => {
        tokio::join!($($handle.submit($item)),+)
    };
}

#[tokio::test]
async fn distinct_keys_behave_positionally() {
    let statements = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handle = spawn_size_gated(FirstWins, 3, &statements);

    let (a, b, c) = submit_all!(handle, (1, 10), (2, 20), (3, 30));
    assert_eq!(a.expect("not closed"), flushed(10));
    assert_eq!(b.expect("not closed"), flushed(20));
    assert_eq!(c.expect("not closed"), flushed(30));

    assert_eq!(
        *statements.lock().expect("lock"),
        vec![vec![(1, 10), (2, 20), (3, 30)]],
        "no folds: every item reaches the statement, in submission order"
    );
}

#[tokio::test]
async fn first_wins_folds_the_newcomer_out() {
    let statements = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handle = spawn_size_gated(FirstWins, 2, &statements);

    let (first, duplicate) = submit_all!(handle, (1, 10), (1, 20));
    assert_eq!(first.expect("not closed"), flushed(10));
    assert_eq!(duplicate.expect("not closed"), folded_out(20));

    assert_eq!(
        *statements.lock().expect("lock"),
        vec![vec![(1, 10)]],
        "only the incumbent reached the statement"
    );
}

#[tokio::test]
async fn last_wins_replaces_in_place() {
    let statements = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handle = spawn_size_gated(LastWins, 3, &statements);

    // The same-key newcomer takes the slot; the slot's position in the
    // statement stays where the key was first seen.
    let (ousted, unrelated, winner) = submit_all!(handle, (1, 10), (2, 20), (1, 30));
    assert_eq!(ousted.expect("not closed"), folded_out(10));
    assert_eq!(unrelated.expect("not closed"), flushed(20));
    assert_eq!(winner.expect("not closed"), flushed(30));

    assert_eq!(
        *statements.lock().expect("lock"),
        vec![vec![(1, 30), (2, 20)]],
        "the winner sits at the key's first-seen position"
    );
}

#[tokio::test]
async fn three_occurrences_fold_pairwise_in_order() {
    let statements = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handle = spawn_size_gated(LargestWins, 3, &statements);

    // (1,20) beats (1,10); then (1,15) loses to the interim winner.
    let (small, large, middle) = submit_all!(handle, (1, 10), (1, 20), (1, 15));
    assert_eq!(small.expect("not closed"), folded_out(10));
    assert_eq!(large.expect("not closed"), flushed(20));
    assert_eq!(middle.expect("not closed"), folded_out(15));

    assert_eq!(*statements.lock().expect("lock"), vec![vec![(1, 20)]]);
}

#[tokio::test]
async fn folded_outputs_settle_against_their_own_winners_output() {
    let statements = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handle = spawn_size_gated(FirstWinsSettling, 3, &statements);

    // The key-1 duplicate must settle against key 1's flush output
    // (`flushed(10)`), not key 2's.
    let (winner, duplicate, unrelated) = submit_all!(handle, (1, 10), (1, 20), (2, 30));
    assert_eq!(winner.expect("not closed"), flushed(10));
    assert_eq!(
        duplicate.expect("not closed"),
        folded_out(20) + flushed(10),
        "settled against its own winner's output"
    );
    assert_eq!(unrelated.expect("not closed"), flushed(30));
}

#[tokio::test]
async fn settling_after_replace_uses_the_final_winners_output() {
    let statements = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handle = spawn_size_gated(LastWinsSettling, 2, &statements);

    // The ousted incumbent settles against the output of the newcomer
    // that replaced it — the slot's final winner.
    let (ousted, winner) = submit_all!(handle, (1, 10), (1, 30));
    assert_eq!(winner.expect("not closed"), flushed(30));
    assert_eq!(
        ousted.expect("not closed"),
        folded_out(10) + flushed(30),
        "settled against the final winner's output"
    );
}

#[tokio::test]
async fn folded_outputs_are_delivered_only_after_the_flush() {
    let statements: Arc<std::sync::Mutex<Vec<Vec<Item>>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let (handle, batcher) = deduplicating_write_batcher(
        Policy {
            max_batch: nz(2),
            max_delay: secs(3600),
        },
        key,
        FirstWins,
        {
            let statements = Arc::clone(&statements);
            let gate = Arc::clone(&gate);
            move |batch: NEVec<Item>| {
                statements
                    .lock()
                    .expect("lock")
                    .push(batch.iter().copied().collect());
                let outputs: Vec<i64> = batch.iter().map(|(_, value)| flushed(*value)).collect();
                let gate = Arc::clone(&gate);
                async move {
                    let _permit = gate.acquire().await.expect("gate open");
                    NEVec::try_from_vec(outputs).expect("batch was non-empty")
                }
            }
        },
        std::future::pending(),
    );
    tokio::spawn(batcher);

    let duplicate = {
        let handle = handle.clone();
        tokio::spawn(async move {
            let (_first, duplicate) = tokio::join!(handle.submit((1, 10)), handle.submit((1, 20)));
            duplicate
        })
    };

    // Let the batcher accumulate, fold, and enter the gated flush; the
    // folded-out waiter must still be pending even though its output was
    // decided at fold time.
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
    assert!(
        !statements.lock().expect("lock").is_empty(),
        "the flush was entered"
    );
    assert!(
        !duplicate.is_finished(),
        "folded-out output must not be released before the flush completes"
    );

    gate.add_permits(1);
    let duplicate = duplicate.await.expect("join").expect("not closed");
    assert_eq!(duplicate, folded_out(20));
}
