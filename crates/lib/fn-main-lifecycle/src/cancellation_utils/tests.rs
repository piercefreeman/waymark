use futures_util::FutureExt as _;

use super::*;

#[tokio::test]
async fn cancels_to_once_from_is_cancelled() {
    let from = tokio_util::sync::CancellationToken::new();
    let to = tokio_util::sync::CancellationToken::new();

    let mut propagation = std::pin::pin!(cancel(&to).after(from.cancelled()));
    assert!((&mut propagation).now_or_never().is_none());
    assert!(!to.is_cancelled());

    from.cancel();
    assert!(propagation.now_or_never().is_some());
    assert!(to.is_cancelled());
}

#[tokio::test]
async fn ends_once_to_is_cancelled_by_anyone_else() {
    let from = tokio_util::sync::CancellationToken::new();
    let to = tokio_util::sync::CancellationToken::new();

    let mut propagation = std::pin::pin!(cancel(&to).after(from.cancelled()));
    assert!((&mut propagation).now_or_never().is_none());

    to.cancel();
    assert!(propagation.now_or_never().is_some());
    assert!(!from.is_cancelled());
}

#[tokio::test]
async fn is_immediate_when_from_is_already_cancelled() {
    let from = tokio_util::sync::CancellationToken::new();
    let to = tokio_util::sync::CancellationToken::new();
    from.cancel();

    assert!(cancel(&to).after(from.cancelled()).now_or_never().is_some());
    assert!(to.is_cancelled());
}
