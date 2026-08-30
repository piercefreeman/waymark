//! Utilities for [`metrics`] crate.

pub struct Val<T>(pub T);

impl metrics::IntoF64 for Val<usize> {
    fn into_f64(self) -> f64 {
        self.0 as _
    }
}

/// Open a scope whose two edges are counted: `entered` is incremented
/// now, `exited` when the returned [`CountedScopeGuard`] drops — whichever way
/// the scope ends. `entered − exited` is the number of scopes currently
/// open.
///
/// Two monotonic counters rather than one gauge moved up and down: the
/// pair cannot drift, survives restarts and scrape gaps, and gives
/// `rate()` on both edges.
pub fn counted_scope(entered: metrics::Counter, exited: metrics::Counter) -> CountedScopeGuard {
    entered.increment(1);
    CountedScopeGuard { exited }
}

/// The lifetime of a scope opened with [`counted_scope`]; counts the exit
/// when dropped.
#[must_use = "dropping the scope immediately counts the exit at once"]
pub struct CountedScopeGuard {
    exited: metrics::Counter,
}

impl Drop for CountedScopeGuard {
    fn drop(&mut self) {
        self.exited.increment(1);
    }
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    type Entry = (
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    );

    fn counter(snapshot: &[Entry], name: &str) -> u64 {
        snapshot
            .iter()
            .find_map(|(key, _, _, value)| match value {
                DebugValue::Counter(value) if key.key().name() == name => Some(*value),
                _ => None,
            })
            .unwrap_or_else(|| panic!("counter {name} not in snapshot"))
    }

    #[test]
    fn exit_is_counted_when_the_scope_drops() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let scope = super::counted_scope(
                metrics::counter!("test_entered_total"),
                metrics::counter!("test_exited_total"),
            );

            let snapshot = snapshotter.snapshot().into_vec();
            assert_eq!(counter(&snapshot, "test_entered_total"), 1);
            assert_eq!(counter(&snapshot, "test_exited_total"), 0);

            drop(scope);

            // The debugging recorder reports deltas since the previous
            // snapshot: nothing new entered, one exit.
            let snapshot = snapshotter.snapshot().into_vec();
            assert_eq!(counter(&snapshot, "test_entered_total"), 0);
            assert_eq!(counter(&snapshot, "test_exited_total"), 1);
        });
    }
}
