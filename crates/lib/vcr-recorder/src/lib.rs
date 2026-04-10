use tokio::io::AsyncWriteExt as _;

pub mod action;
pub mod backend;
pub mod instance;
pub mod pool;

enum Command {
    OpenInstanceLog(waymark_core_backend::QueuedInstance),
    RecordActionRequest(waymark_worker_core::ActionRequest),
    RecordActionCompletion(waymark_worker_core::ActionCompletion),
    CompleteInstanceLog(waymark_ids::InstanceId),
}

#[derive(Debug)]
pub struct Handle {
    tx: tokio::sync::mpsc::Sender<Command>,
    rx: tokio::sync::mpsc::Receiver<Command>,
}

pub struct Params {
    pub writer: waymark_vcr_file::Writer,
    pub handle: Handle,
}

#[derive(Debug)]
pub enum Error {
    Write(waymark_jsonlines::WriteError),
    Flush(std::io::Error),
}

pub async fn r#loop(params: Params) -> Result<(), Error> {
    let Params {
        mut writer,
        handle: Handle { mut rx, tx: _ },
    } = params;

    let mut instance_buferrer = instance::Bufferrer::default();

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
            let log_item = match command {
                Command::OpenInstanceLog(queued_instance) => {
                    instance_buferrer.open_instance_log(queued_instance);
                    continue;
                }
                Command::RecordActionRequest(action_request) => {
                    let result = instance_buferrer.record_action_request(action_request);
                    if let Err(error) = result {
                        tracing::warn!(?error, "unable to record action request");
                    };
                    continue;
                }
                Command::RecordActionCompletion(action_completion) => {
                    let result = instance_buferrer.record_action_completion(action_completion);
                    if let Err(error) = result {
                        tracing::warn!(?error, "unable to record action completion");
                    };
                    continue;
                }
                Command::CompleteInstanceLog(instance_id) => {
                    match instance_buferrer.complete_instance_log(instance_id) {
                        Ok(val) => val,
                        Err(error) => {
                            tracing::warn!(?error, "unable to complete instance log");
                            continue;
                        }
                    }
                }
            };
            prepared_log_items.push(log_item);
        }

        for item in prepared_log_items.drain(..) {
            writer.write_value(&item).await.map_err(Error::Write)?;
        }

        writer.writer.flush().await.map_err(Error::Flush)?;
    }

    writer.writer.flush().await.map_err(Error::Flush)?;

    Ok(())
}

impl Handle {
    pub fn new(command_buffer: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(command_buffer);
        Self { tx, rx }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HandleError {
    #[error("recorder is dropped")]
    RecorderDropped,

    #[error("no buffer capacity")]
    NoBufferCapacity,
}
