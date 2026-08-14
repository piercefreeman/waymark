//! Conflict resolution for the deduplicating-write batcher: how same-key
//! items fold before the flush.
//!
//! A [`deduplicating_write_batcher`](crate::deduplicating_write_batcher)
//! groups a window's items by key; when a key is seen again, the caller's
//! [`ConflictResolver`] folds the newcomer against the incumbent —
//! pairwise, in submission order.  A folded-out item never reaches the
//! statement; its fold produces a *provisional* output, which is settled
//! against its winner's actual flush output
//! ([`ConflictResolver::settle_conflict`]) before delivery — so a folded
//! waiter can never be told an outcome the write attempt then failed to
//! produce.
//!
//! The fold is expressed through a typestate API, so its obligations hold
//! at the type level:
//!
//! - the resolver receives a [`ConflictedSlot`] — a view of the
//!   incumbent's slot — and the newcomer by value;
//! - it must consume the slot via [`ConflictedSlot::keep`] (the incumbent
//!   stays; the newcomer is folded out) or [`ConflictedSlot::replace`]
//!   (the newcomer takes the slot in place; the ousted incumbent comes
//!   back by value, to be folded out via [`ResolvingSlot::resolve`]);
//! - either path is the only source of the [`ConflictResolvedToken`] the
//!   fold must return, so a fold cannot be left unresolved, and the
//!   token's lifetime pins it to its own fold, so a token cannot be
//!   stashed and returned for a different one.
//!
//! Waiters are in the machinery's custody: every submitted item's waiter
//! is answered exactly once, from the fold or from the flush.  A
//! folded-out *item*'s ownership passes to the resolver
//! ([`keep`](ConflictedSlot::keep) already owns the newcomer;
//! [`replace`](ConflictedSlot::replace) hands the incumbent back), so a
//! resolver may move the item's data into the folded output instead of
//! cloning.
//!
//! Folded-out outputs are settled and *delivered* together with the flush
//! outputs, after the flush: the fold proposes, the flush's per-winner
//! result disposes.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::marker::PhantomData;

use nonempty_collections::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};
use tokio::sync::oneshot;

use crate::Job;

/// The conflict resolver of one deduplicating-write batcher: what happens
/// when two same-key items meet in one window.
///
/// Implemented on named types only — a resolver states both halves of
/// the fold story: what a fold records, and how it settles.
pub trait ConflictResolver<In, Out> {
    /// The resolver's fold-time record for a folded-out item: what
    /// [`resolve_conflict`](Self::resolve_conflict) leaves behind, and
    /// what [`settle_conflict`](Self::settle_conflict) consumes against
    /// the winner's actual flush output.
    type Placeholder;

    /// Fold a same-key conflict: the incumbent (in the slot) against the
    /// newcomer, which arrived later in submission order.  Three or more
    /// occurrences fold pairwise, left to right.
    ///
    /// Consume the slot via [`ConflictedSlot::keep`] or
    /// [`ConflictedSlot::replace`]; the returned token proves the fold
    /// was resolved.
    fn resolve_conflict<'a>(
        &self,
        slot: ConflictedSlot<'a, In, Out, Self::Placeholder>,
        newcomer: In,
    ) -> ConflictResolvedToken<'a>;

    /// Settle a folded-out item's placeholder against its winner's
    /// actual flush output, producing the folded waiter's final output.
    /// Called once per folded item, after the flush and before delivery.
    fn settle_conflict(&self, conflicted_out: Self::Placeholder, winner_out: &Out) -> Out;
}

/// A same-key conflict, viewed from the incumbent's slot.  Must be
/// consumed via [`keep`](Self::keep) or [`replace`](Self::replace).
pub struct ConflictedSlot<'a, In, Out, Placeholder> {
    incumbent: &'a mut In,
    resolving: ResolvingSlot<'a, Out, Placeholder>,
}

impl<'a, In, Out, Placeholder> ConflictedSlot<'a, In, Out, Placeholder> {
    /// The current incumbent, for inspection before deciding.
    pub fn incumbent(&self) -> &In {
        self.incumbent
    }

    /// The incumbent stays; the newcomer is folded out with
    /// `newcomer_placeholder`.  The resolver owns the newcomer and may
    /// move its data into the placeholder.
    pub fn keep(self, newcomer_placeholder: Placeholder) -> ConflictResolvedToken<'a> {
        self.resolving.folded.push((
            self.resolving.newcomer_waiter,
            newcomer_placeholder,
            self.resolving.winner_slot,
        ));
        ConflictResolvedToken { _fold: PhantomData }
    }

    /// The newcomer takes the slot in place; the ousted incumbent comes
    /// back by value, to be folded out via [`ResolvingSlot::resolve`].
    pub fn replace(self, newcomer: In) -> (In, ResolvingSlot<'a, Out, Placeholder>) {
        (std::mem::replace(self.incumbent, newcomer), self.resolving)
    }
}

/// A fold whose incumbent was ousted, awaiting the ousted side's
/// placeholder.
pub struct ResolvingSlot<'a, Out, Placeholder> {
    /// Folded-out waiters, each with its placeholder and the winner slot
    /// it folded against.
    folded: &'a mut Vec<(oneshot::Sender<Out>, Placeholder, usize)>,
    /// The one record of who awaits the winner slot's flush output.
    owner: &'a mut oneshot::Sender<Out>,
    /// The newcomer's waiter.
    newcomer_waiter: oneshot::Sender<Out>,
    /// The winner slot this fold happened against.
    winner_slot: usize,
}

impl<'a, Out, Placeholder> ResolvingSlot<'a, Out, Placeholder> {
    /// The ousted incumbent is folded out with `incumbent_placeholder`.
    ///
    /// The slot's flush output moves from the incumbent's waiter to the
    /// newcomer's in one swap — a slot has exactly one awaiting waiter
    /// at all times.
    pub fn resolve(self, incumbent_placeholder: Placeholder) -> ConflictResolvedToken<'a> {
        let ousted_waiter = std::mem::replace(self.owner, self.newcomer_waiter);
        self.folded
            .push((ousted_waiter, incumbent_placeholder, self.winner_slot));
        ConflictResolvedToken { _fold: PhantomData }
    }
}

/// Proof that a fold was resolved — only [`ConflictedSlot::keep`] and
/// [`ResolvingSlot::resolve`] mint one.  Its lifetime is the fold's, so
/// it cannot be stashed and returned for another fold.
pub struct ConflictResolvedToken<'a> {
    // Invariant in `'a`: closes even theoretical lifetime coercions.
    _fold: PhantomData<fn(&'a ()) -> &'a ()>,
}

/// The deduplicating-write [`BatchStrategy`](super::core::BatchStrategy):
/// fold same-key jobs down to winners, flush the winners, answer
/// folded-out waiters from their folds.
pub(crate) struct BatchStrategy<KeyFn, ConflictResolver> {
    pub key_fn: KeyFn,
    pub resolver: ConflictResolver,
}

impl<In, Key, Out, KeyFn, ConflictResolver> super::core::BatchStrategy<In, Out>
    for BatchStrategy<KeyFn, ConflictResolver>
where
    KeyFn: Fn(&In) -> Key,
    Key: Hash + Eq,
    ConflictResolver: self::ConflictResolver<In, Out>,
{
    type Plan = (
        // The winners' waiters, positionally aligned with the flush input.
        NEVec<oneshot::Sender<Out>>,
        // The folded-out waiters, each with its placeholder and the
        // winner slot to settle it against.
        Vec<(oneshot::Sender<Out>, ConflictResolver::Placeholder, usize)>,
    );

    fn prepare(&self, jobs: NEVec<Job<In, Out>>) -> (NEVec<In>, Self::Plan) {
        let capacity = jobs.len();
        let ((first_item, first_waiter), rest) = jobs.into_nonempty_iter().next();
        let mut index_of: HashMap<Key, usize> = HashMap::new();
        index_of.insert((self.key_fn)(&first_item), 0);
        let mut winners = NEVec::with_capacity(capacity, first_item);
        let mut winner_waiters = NEVec::with_capacity(capacity, first_waiter);
        let mut folded: Vec<(oneshot::Sender<Out>, ConflictResolver::Placeholder, usize)> =
            Vec::new();

        for (item, waiter) in rest {
            match index_of.entry((self.key_fn)(&item)) {
                Entry::Vacant(vacant) => {
                    vacant.insert(winner_waiters.len().get());
                    winners.push(item);
                    winner_waiters.push(waiter);
                }
                Entry::Occupied(occupied) => {
                    let slot = *occupied.get();
                    self.resolver.resolve_conflict(
                        ConflictedSlot {
                            incumbent: &mut winners[slot],
                            resolving: ResolvingSlot {
                                folded: &mut folded,
                                owner: &mut winner_waiters[slot],
                                newcomer_waiter: waiter,
                                winner_slot: slot,
                            },
                        },
                        item,
                    );
                }
            }
        }

        (winners, (winner_waiters, folded))
    }

    fn deliver(&self, (winner_waiters, folded): Self::Plan, outputs: NEVec<Out>) {
        // A dropped waiter (producer gave up / was cancelled) is fine;
        // its output is simply discarded.
        for (waiter, placeholder, winner_slot) in folded {
            let output = self
                .resolver
                .settle_conflict(placeholder, &outputs[winner_slot]);
            let _ = waiter.send(output);
        }
        for (output, waiter) in outputs.into_iter().zip(winner_waiters) {
            let _ = waiter.send(output);
        }
    }
}

#[cfg(test)]
mod tests;
