use futures_util::{FutureExt as _, StreamExt};
use nonempty_collections::NEVec;
use waymark_worker_core::ActionCompletion;

#[derive(Debug, Default)]
pub struct CompletionQueue {
    queue: tokio_util::time::DelayQueue<ActionCompletion>,
}

impl CompletionQueue {
    pub fn queue(&mut self, completion: ActionCompletion, delay: std::time::Duration) {
        self.queue.insert(completion, delay);
    }

    pub async fn poll_completions(&mut self) -> Option<NEVec<ActionCompletion>> {
        let first = self.queue.next().await?;
        let first = first.into_inner();

        let mut vec = NEVec::new(first);

        loop {
            let maybe_now = self.queue.next().now_or_never();

            let Some(maybe_next) = maybe_now else {
                break;
            };

            let Some(next) = maybe_next else {
                break;
            };

            let next = next.into_inner();

            vec.push(next);
        }

        Some(vec)
    }
}
