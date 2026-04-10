use std::num::NonZeroUsize;

use nonempty_collections::NEVec;
use waymark_worker_core::ActionCompletion;

#[derive(Debug)]
pub struct IngestHandle {
    tx: tokio::sync::mpsc::Sender<(ActionCompletion, tokio::time::Instant)>,
}

#[derive(Debug)]
pub struct PollHandle {
    rx: tokio::sync::mpsc::Receiver<(ActionCompletion, tokio::time::Instant)>,
    queue: super::completion_queue::CompletionQueue,
}

impl IngestHandle {
    pub fn queue(&self, completion: ActionCompletion, delay: std::time::Duration) {
        let at = tokio::time::Instant::now()
            .checked_add(delay)
            .expect("instant overflow");

        self.tx
            .try_send((completion, at))
            .expect("the ingest buffer capacity exceeded");
    }
}

impl PollHandle {
    pub async fn poll_completions(&mut self) -> Option<NEVec<ActionCompletion>> {
        let mut poll_ingest_items = true;

        loop {
            let ingest_item = {
                let inner_poll_fut = self.queue.poll_completions();
                let inner_poll_fut = std::pin::pin!(inner_poll_fut);

                let ingest_fut = self.rx.recv();
                let ingest_fut = std::pin::pin!(ingest_fut);

                tokio::select! {
                    polled_completions = inner_poll_fut => {
                        return polled_completions;
                    }
                    maybe_ingest_item = ingest_fut, if poll_ingest_items => {
                        let Some(ingest_item) = maybe_ingest_item else {
                            poll_ingest_items = false;
                            continue;
                        };
                        ingest_item
                    }
                }
            };

            let (completion_to_queue, at) = ingest_item;

            let delay = at.saturating_duration_since(tokio::time::Instant::now());

            self.queue.queue(completion_to_queue, delay);
        }
    }
}

pub fn new(
    ingest_buffer: NonZeroUsize,
    queue: super::completion_queue::CompletionQueue,
) -> (IngestHandle, PollHandle) {
    let (tx, rx) = tokio::sync::mpsc::channel(ingest_buffer.get());
    (IngestHandle { tx }, PollHandle { rx, queue })
}
