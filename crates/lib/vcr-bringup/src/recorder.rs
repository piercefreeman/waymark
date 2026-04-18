use std::num::NonZeroUsize;

#[must_use]
pub struct Bringup {
    pub recorder_task: tokio::task::JoinHandle<Result<(), waymark_vcr_recorder::Error>>,
    pub pool_handle: waymark_vcr_recorder::pool::Handle,
    pub backend_handle: waymark_vcr_recorder::backend::Handle,
}

pub async fn setup(
    log_file_path: impl AsRef<std::path::Path>,
    command_buffer_size: NonZeroUsize,
) -> Result<Bringup, std::io::Error> {
    let recorder_handle = waymark_vcr_recorder::Handle::new(command_buffer_size);

    let file = tokio::fs::File::create_new(log_file_path).await?;
    let writer = waymark_vcr_file::Writer::from(file);

    let pool_handle = recorder_handle.pool_handle();
    let backend_handle = recorder_handle.backend_handle();

    let params = waymark_vcr_recorder::Params {
        writer,
        handle: recorder_handle,
    };

    let recorder_task = tokio::spawn(waymark_vcr_recorder::r#loop(params));

    Ok(Bringup {
        recorder_task,
        pool_handle,
        backend_handle,
    })
}
