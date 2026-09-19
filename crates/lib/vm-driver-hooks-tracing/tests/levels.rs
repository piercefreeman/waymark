use std::sync::{Arc, Mutex};

use tracing_subscriber::layer::SubscriberExt as _;
use waymark_vm_driver_hooks::{SnapshotPersisted as _, VmStarted as _, VmStopped as _};
use waymark_vm_driver_hooks_tracing::Tracing;

/// One captured event: its level and its message.
type Seen = (tracing::Level, String);

/// Records every event's level and message.
struct Recording(Arc<Mutex<Vec<Seen>>>);

struct Message(Option<String>);

impl tracing::field::Visit for Message {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

impl<Subscriber> tracing_subscriber::Layer<Subscriber> for Recording
where
    Subscriber: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, Subscriber>,
    ) {
        let mut message = Message(None);
        event.record(&mut message);
        self.0
            .lock()
            .unwrap()
            .push((*event.metadata().level(), message.0.unwrap_or_default()));
    }
}

/// Run `drive` under a recording subscriber and hand back what it logged.
fn captured(drive: impl FnOnce()) -> Vec<Seen> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(Recording(Arc::clone(&seen)));

    tracing::subscriber::with_default(subscriber, drive);

    seen.lock().unwrap().clone()
}

type TestTracing = Tracing<(), (), waymark_vm_driver::Error<(), (), &'static str, (), ()>>;

#[test]
fn expected_exits_log_at_debug() {
    let seen = captured(|| {
        let hooks = TestTracing::new();
        hooks.vm_started();
        hooks.snapshot_persisted(4096);
        hooks.vm_stopped(&waymark_vm_driver::Error::NoReadyFramesOrWaitingPromises);
        hooks.vm_stopped(&waymark_vm_driver::Error::Cancelled);
    });

    let levels: Vec<_> = seen.iter().map(|(level, _)| *level).collect();
    assert_eq!(levels, [tracing::Level::DEBUG; 4]);
    assert_eq!(
        seen[2].1,
        "vm driver exhausted: no ready frames or waiting promises"
    );
    assert_eq!(seen[3].1, "vm driver cancelled");
}

#[test]
fn failures_log_at_error() {
    let seen = captured(|| {
        TestTracing::new().vm_stopped(&waymark_vm_driver::Error::SnapshotPersistence("disk full"));
    });

    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0, tracing::Level::ERROR);
    assert_eq!(seen[0].1, "vm driver failed");
}
