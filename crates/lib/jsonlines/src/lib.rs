use std::marker::PhantomData;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

/// Writes newline-delimited JSON values to a file.
#[derive(Debug)]
pub struct Writer<W, T: ?Sized> {
    pub writer: W,
    pub phantom_data: PhantomData<T>,
}

impl<W, T> From<W> for Writer<W, T>
where
    W: tokio::io::AsyncWrite,
    T: ?Sized,
{
    fn from(writer: W) -> Self {
        Self {
            writer,
            phantom_data: PhantomData,
        }
    }
}

impl<W, T> Writer<W, T>
where
    W: tokio::io::AsyncWrite,
    W: Unpin,
    T: ?Sized,
    T: serde::Serialize,
{
    pub async fn write_value(&mut self, value: &T) -> Result<(), WriteError> {
        let mut encoded = serde_json::to_vec(value).map_err(WriteError::Serialize)?;
        encoded.push(b'\n');
        self.writer
            .write_all(&encoded)
            .await
            .map_err(WriteError::Io)
    }
}

/// Reads newline-delimited JSON values from a file.
#[derive(Debug)]
pub struct Reader<R, T> {
    pub lines: tokio::io::Lines<R>,
    pub phantom_data: PhantomData<T>,
}

impl<R, T> From<R> for Reader<R, T>
where
    R: tokio::io::AsyncBufRead,
{
    fn from(reader: R) -> Self {
        Self {
            lines: reader.lines(),
            phantom_data: PhantomData,
        }
    }
}

impl<R, T> Reader<R, T>
where
    R: tokio::io::AsyncBufRead,
    R: Unpin,
    T: for<'de> serde::Deserialize<'de>,
{
    pub async fn next_value(&mut self) -> Result<Option<T>, ReadError> {
        let maybe_line = self.lines.next_line().await.map_err(ReadError::Io)?;
        let Some(line) = maybe_line else {
            return Ok(None);
        };

        let value = serde_json::from_str(&line).map_err(ReadError::Deserialize)?;
        Ok(Some(value))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error(transparent)]
    Io(std::io::Error),

    #[error(transparent)]
    Deserialize(serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error(transparent)]
    Io(std::io::Error),

    #[error(transparent)]
    Serialize(serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::{Reader, Writer};
    use serde_json::json;
    use tokio::io::AsyncWriteExt as _;

    #[tokio::test]
    async fn writes_and_reads_json_lines() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join(format!(
            "waymark-jsonl-{}.jsonl",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before UNIX_EPOCH")
                .as_nanos(),
        ));

        let file = tokio::fs::File::create(&path)
            .await
            .expect("create file should succeed");

        let mut writer: Writer<_, serde_json::Value> = file.into();

        writer
            .write_value(&json!({"kind": "start", "id": 1}))
            .await
            .expect("first write should succeed");
        writer
            .write_value(&json!({"kind": "stop", "id": 2}))
            .await
            .expect("second write should succeed");
        writer.writer.flush().await.expect("flush should succeed");

        let file = tokio::fs::File::open(&path)
            .await
            .expect("open reader should succeed");

        let mut reader: Reader<_, serde_json::Value> = tokio::io::BufReader::new(file).into();
        let first = reader
            .next_value()
            .await
            .expect("first read should succeed")
            .expect("first value should exist");
        let second = reader
            .next_value()
            .await
            .expect("second read should succeed")
            .expect("second value should exist");
        let end = reader
            .next_value()
            .await
            .expect("third read should succeed");

        assert_eq!(first, json!({"kind": "start", "id": 1}));
        assert_eq!(second, json!({"kind": "stop", "id": 2}));
        assert!(end.is_none());

        tokio::fs::remove_file(path)
            .await
            .expect("temp file cleanup should succeed");
    }
}
