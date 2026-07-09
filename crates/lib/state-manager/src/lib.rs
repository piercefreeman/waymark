//! Generic state manager with delayed eviction control.
//!
//! See [`State`] and [`Sweeper`].

#![warn(missing_docs)]

mod guard;
mod handle;
pub mod provider;
mod storage;
mod sweeper;

pub use self::handle::Handle;
pub use self::provider::Provider;
pub use self::sweeper::Sweeper;

use core::hash::Hash;
use std::sync::Arc;

use waymark_nonzero_duration::NonZeroDuration;

use self::guard::Guard;
use self::storage::Maps;

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

impl<Key, Value, Factory> State<Key, Value, Factory>
where
    Key: Eq + Hash,
{
    /// Create a new [`State`] and [`Sweeper`] with the specified
    /// `retention` and `factory`.
    pub fn new(retention: NonZeroDuration, factory: Factory) -> (Self, Sweeper<Key, Value>) {
        let maps = Arc::new(Maps::new());

        let sweeper = Sweeper::new(retention, Arc::downgrade(&maps));

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
        let oncecell = self.maps.acquire(&key);

        // The reference acquired above is owned by `guard` from here on. If the
        // factory errors or this future is cancelled at the `.await` below,
        // dropping `guard` releases the reference; on success `guard` moves
        // into the returned `Handle` and releases it when the last handle is
        // dropped.
        let guard = Guard::new(Arc::downgrade(&self.maps), key);

        let value = oncecell
            .get_or_try_init(|| self.factory.produce(guard.key()))
            .await?
            .clone();

        Ok(Handle::new(guard, value))
    }
}
