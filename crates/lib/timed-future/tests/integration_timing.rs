use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use metrics::with_local_recorder;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use waymark_timed_future::Timed;

#[test]
fn does_not_count_time_after_ready_before_drop() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();

    with_local_recorder(&recorder, || {
        let histogram = metrics::histogram!("waymark_timed_future_test_seconds");
        {
            let mut timed = std::pin::pin!(Timed::new(std::future::ready(()), histogram));

            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);

            assert!(matches!(timed.as_mut().poll(&mut cx), Poll::Ready(())));

            // If timing were recorded on drop, this delay would dominate the sample.
            std::thread::sleep(Duration::from_millis(120));
        }
    });

    let samples = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .find_map(|(key, _unit, _desc, value)| {
            if key.key().name() == "waymark_timed_future_test_seconds" {
                return match value {
                    DebugValue::Histogram(values) => Some(values),
                    _ => None,
                };
            }
            None
        })
        .expect("expected histogram samples for waymark_timed_future_test_seconds");

    assert_eq!(samples.len(), 1);
    let elapsed_seconds = samples[0].into_inner();

    assert!(
        elapsed_seconds < 0.06,
        "expected sample to exclude post-ready delay; got {elapsed_seconds:.6}s"
    );
}
