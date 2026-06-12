//! Generic state manager with delayed eviction control.
//!
//! See [`State`] and [`Sweeper`].

#![warn(missing_docs)]

pub mod provider;

pub use self::provider::Provider;

use core::hash::Hash;
use std::sync::Arc;

use dashmap::DashMap;
use waymark_nonzero_duration::NonZeroDuration;

/// The state that holds `Key`/`Value` pairs.
///
/// `Value`s are only exposed as read-only to the consumers.
///
/// After the last [`Handle`] to a value is dropped, the value is guaranteed
/// to remain in the state for a specified retention period, after which
/// the [`Sweeper`] can pick it up and clear it out.
///
/// If the value is not in the state (either it has never been there, or it
/// was unused and got removed) - a new value is created when obtaining
/// a [`Handle`]; see [`State::get`].
pub struct State<Key, Value, Factory> {
    /// The underlying state maps.
    maps: Arc<Maps<Key, Value>>,

    /// A factory used to produce the values for this state manager.
    factory: Factory,
}

struct Maps<Key, Value> {
    /// A map of keys to value-entries.
    entries: DashMap<Key, Entry<Value>>,

    /// The pending evictions map.
    pending_evictions: DashMap<Key, std::time::Instant>,
}

struct Entry<Value> {
    value: Arc<tokio::sync::OnceCell<Value>>,
    refs: usize,
}

/// The sweeper for a particular state that performs the eviction of stale
/// entries.
pub struct Sweeper<Key, Value> {
    retention: NonZeroDuration,
    maps: std::sync::Weak<Maps<Key, Value>>,
}

/// A handle to an entry in the state.
///
/// See [`State::get`].
#[must_use]
pub struct Handle<Key, Value>
where
    Key: Eq + Hash,
{
    /// The key.
    ///
    /// SAFETY: there are safety invariants that depend on this field.
    key: Option<Key>,

    /// The value.
    value: Value,

    maps: std::sync::Weak<Maps<Key, Value>>,
}

impl<Key, Value> std::ops::Deref for Handle<Key, Value>
where
    Key: Eq + Hash,
{
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<Key, Value> Handle<Key, Value>
where
    Key: Eq + Hash,
{
    /// Return a ref to the key for this entry.
    pub fn key(this: &Self) -> &Key {
        // SAFETY: the key option is initialized on construction and
        // only consumed at `drop`.
        unsafe { this.key.as_ref().unwrap_unchecked() }
    }

    /// Return a ref to the value for this entry.
    pub fn value(this: &Self) -> &Value {
        &this.value
    }
}

impl<Key, Value> Drop for Handle<Key, Value>
where
    Key: Eq + Hash,
{
    fn drop(&mut self) {
        let Some(maps) = self.maps.upgrade() else {
            // Maps are gone, no need to clear do the delayed cleanup.
            return;
        };

        let key = self.key.take();

        // SAFETY: key is initialized at `Handle` creation and
        // only consumed on `drop`; `drop` can't be called twice.
        let key = unsafe { key.unwrap_unchecked() };

        let no_more_refs = {
            let Some(mut entry) = maps.entries.get_mut(&key) else {
                // The entry is gone, no need to do the delayed cleanup for it.
                return;
            };

            let Some(new_refs) = entry.refs.checked_sub(1) else {
                unreachable!("refs underflow"); // should be impossible
            };

            entry.refs = new_refs;
            drop(entry);

            new_refs == 0
        };

        if no_more_refs {
            // If there was another entry at the pending evictions map due
            // to some sort of a race - the last one wins, so overwrite
            // the preexisting one with a new one to delay the eviction further.
            let previous = maps
                .pending_evictions
                .insert(key, std::time::Instant::now());

            if previous.is_some() {
                // Found a race! Nothing special to do really, but we'll log
                // it just in case - if users see a lot of these it would
                // warrant an investigation, as we should be able to make this
                // even safer and more reliable.
                // Or it could be caused by a newly introduced bug.
                tracing::warn!(
                    "detected race at state handle drop; \
                    this is probably safe as it anticipated in the design - \
                    but still logged cause it should be rare and notable \
                    occurrence"
                );
            }
        }
    }
}

impl<Key, Value, Factory> State<Key, Value, Factory>
where
    Key: Eq + Hash,
{
    /// Create a new [`State`] and [`Sweeper`] with the specified
    /// `retention` and `factory`.
    pub fn new(retention: NonZeroDuration, factory: Factory) -> (Self, Sweeper<Key, Value>) {
        let maps = Maps {
            entries: DashMap::new(),
            pending_evictions: DashMap::new(),
        };
        let maps = Arc::new(maps);

        let sweeper = Sweeper {
            retention,
            maps: Arc::downgrade(&maps),
        };

        let state = Self { maps, factory };

        (state, sweeper)
    }
}

impl<Key, Value, Factory> State<Key, Value, Factory>
where
    Key: Eq + Hash + Clone,
    Factory: waymark_state_manager_core::Factory<Key = Key, Value = Value>,
    Value: Clone,
{
    /// Get or create a new value in the store, and return a [`Handle`] to it.
    ///
    /// [`Handle`] provides read-only access to the value, and while
    /// the [`Handle`] is held the entry will not be removed from the store.
    ///
    /// After the last [`Handle`] to the entry is dropped, it is marked for
    /// eviction, and (unless another [`Handle`] to it is obtained) will
    /// be removed from the store by the [`Sweeper`] after the corresponding
    /// retention period.
    pub async fn get(&self, key: Key) -> Result<Handle<Key, Value>, Factory::Error> {
        let oncecell = {
            match self.maps.entries.entry(key.clone()) {
                dashmap::Entry::Occupied(mut occupied) => {
                    let Entry { refs, value } = occupied.get_mut();
                    *refs += 1;
                    Arc::clone(value)
                }
                dashmap::Entry::Vacant(vacant) => {
                    let oncecell = Arc::new(tokio::sync::OnceCell::new());
                    vacant.insert(Entry {
                        value: Arc::clone(&oncecell),
                        refs: 1,
                    });
                    oncecell
                }
            }
        };

        self.maps.pending_evictions.remove(&key);

        let value = oncecell
            .get_or_try_init(|| self.factory.produce(&key))
            .await?;

        Ok(Handle {
            key: Some(key),
            value: value.clone(),
            maps: Arc::downgrade(&self.maps),
        })
    }
}

impl<Key, Value> Sweeper<Key, Value>
where
    Key: Eq + Hash + Clone,
{
    /// Perform one sweep on the pending evictions maps, evicting all
    /// the entries that have not been held on to via a [`Handle`] for longer
    /// than the configured retention duration.
    ///
    /// Passes the evicted entries to the `on_eviction` callback.
    pub fn sweep_with_handler(
        &mut self,
        mut on_eviction: impl FnMut(Key, Arc<tokio::sync::OnceCell<Value>>),
    ) {
        let Some(maps) = self.maps.upgrade() else {
            return;
        };

        let retention = self.retention.get();
        let now = std::time::Instant::now();

        let mut cleanup_queue = Vec::with_capacity(maps.pending_evictions.len());
        maps.pending_evictions.retain(|key, orphaned_since| {
            let retain = now.duration_since(*orphaned_since) <= retention;
            if !retain {
                cleanup_queue.push((*key).clone())
            }
            retain
        });

        for key in cleanup_queue {
            let Some((key, entry)) = maps.entries.remove_if(&key, |_, entry| entry.refs == 0)
            else {
                // One possibility to get into this branch is to have
                // an entry that has non-zero refs. This value has some
                // `Handle`s to it still, and *they* will re-queue this entry
                // once they drop. We can safely ignore this.
                //
                // Another reason is value was already evicted - this can
                // happen due to a legitimate race between `sweep` and
                // `Handle::drop`/`get`:
                //
                // 1. `retain` above removes key K from the pending evictions and
                //    adds it to `cleanup_queue`.
                // 2. `get(K)` runs concurrently: the occupied
                //    branch bumps the `refs` and calls
                //    `pending_evictions.remove(&K)` - a no-op since K was
                //    already stripped in step 1. A fresh `Handle` is
                //    returned.
                // 3. The `Handle` is dropped. The `refs` is now 0
                //    so `Handle::drop` inserts K *back* into the pending
                //    evictions map with a new timestamp.
                // 4. The sweep loop reaches K and removes it from
                //    `maps.entries`.
                // 5. On a subsequent sweep, K is discovered in the
                //    pending evictions map again but is no longer in
                //    `maps.entries` - we hit this branch.
                //
                // This is harmless: the value has already been evicted,
                // so we simply skip it.
                continue;
            };

            let Entry { value, refs: _ } = entry;

            on_eviction(key, value);
        }
    }

    /// Perform one sweep on the pending evictions maps, evicting all
    /// the entries that have not been held on to via a [`Handle`] for longer
    /// than the configured retention duration.
    pub fn sweep(&mut self) {
        self.sweep_with_handler(|_, _| {});
    }

    /// Returns whether this [`Sweeper`]'s associated [`State`] still
    /// exists.
    ///
    /// Returns `false` if the [`State`] has been dropped.
    pub fn associated_state_exists(&self) -> bool {
        self.maps.strong_count() > 0
    }
}
