mod action_id;

#[derive(Debug, Clone)]
pub struct Handle {
    // TODO: we have no capacity for backpressure at the workers side;
    // switch to a proper backpressure-capable way of accepting
    // the actions here when the APIs for it become available.
    pub tx: tokio::sync::mpsc::UnboundedSender<Command>,
}

pub enum Command {
    Request(waymark_worker_core::ActionRequest),
    Completion(waymark_worker_core::ActionCompletion),
}

pub struct Params {
    pub writer: waymark_worker_vcr_file::Writer,
    pub rx: tokio::sync::mpsc::UnboundedReceiver<Command>,
}

#[derive(Debug)]
pub enum Error {
    Write(waymark_jsonlines::WriteError),
    Flush(std::io::Error),
}

pub async fn r#loop(params: Params) -> Result<(), Error> {
    let Params { mut writer, mut rx } = params;

    let mut correlator = waymark_action_correlator::HashMap {
        correlator: ActionCorrelator,
        incomplete_action_contexts: Default::default(),
    };

    let mut commands = Vec::with_capacity(1024);
    let mut prepared_log_items = Vec::with_capacity(commands.capacity());

    loop {
        commands.clear();
        let limit = commands.capacity();
        let read = rx.recv_many(&mut commands, limit).await;
        if read == 0 {
            break;
        }

        for command in commands.drain(..) {
            match command {
                Command::Request(action_request) => {
                    correlator.insert_request(action_request);
                }
                Command::Completion(action_completion) => {
                    let result = correlator.correlate_completion(action_completion);
                    let correlated_item = match result {
                        Ok(val) => val,
                        Err(id) => {
                            tracing::warn!(
                                ?id,
                                "unable to correlate completion with a missing request"
                            );
                            continue;
                        }
                    };

                    prepared_log_items.push(correlated_item);
                }
            }
        }

        for item in prepared_log_items.drain(..) {
            writer.write_value(&item).await.map_err(Error::Write)?;
        }

        writer.flush().await.map_err(Error::Flush)?;
    }

    writer.flush().await.map_err(Error::Flush)?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("recorder is dropped")]
pub struct RecorderDroppedError;

impl Handle {
    pub fn request(
        &self,
        request: waymark_worker_core::ActionRequest,
    ) -> Result<(), RecorderDroppedError> {
        self.tx
            .send(Command::Request(request))
            .map_err(|_| RecorderDroppedError)
    }

    pub fn completion(
        &self,
        completion: waymark_worker_core::ActionCompletion,
    ) -> Result<(), RecorderDroppedError> {
        self.tx
            .send(Command::Completion(completion))
            .map_err(|_| RecorderDroppedError)
    }
}

struct IncompleteActionContext {
    pub queued_at: std::time::Instant,
    pub action_params: waymark_worker_vcr_file::ActionParams,
}

struct ActionCorrelator;

impl waymark_action_correlator_core::ActionCorrelator for ActionCorrelator {
    type ActionId = action_id::ActionId;
    type ActionContext = IncompleteActionContext;
    type CorrelatedItem = waymark_worker_vcr_file::LogItem;

    fn capture_request_context(
        &mut self,
        request: waymark_worker_core::ActionRequest,
    ) -> Self::ActionContext {
        IncompleteActionContext {
            queued_at: std::time::Instant::now(),
            action_params: request.into(),
        }
    }

    fn combine(
        &mut self,
        context: Self::ActionContext,
        completion: waymark_worker_core::ActionCompletion,
    ) -> Self::CorrelatedItem {
        waymark_worker_vcr_file::LogItem {
            execution_time: context.queued_at.elapsed(),
            params: context.action_params,
            result: completion.result,
        }
    }
}
