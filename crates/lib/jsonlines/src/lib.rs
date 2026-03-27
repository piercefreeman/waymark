use std::io;
use std::marker::PhantomData;

use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

/// Writes newline-delimited JSON values to a file.
#[derive(Debug)]
pub struct Writer<T: ?Sized> {
    pub writer: BufWriter<File>,
    pub phantom_data: PhantomData<T>,
}

impl<T: ?Sized> Writer<T> {
    pub async fn create(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        let file = File::create(path).await?;
        Ok(Self::from(file))
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
    }
}

impl<T: ?Sized> From<File> for Writer<T> {
    fn from(value: File) -> Self {
        Self {
            writer: BufWriter::new(value),
            phantom_data: PhantomData,
        }
    }
}

impl<T> Writer<T>
where
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
pub struct Reader<T> {
    pub lines: tokio::io::Lines<BufReader<File>>,
    pub phantom_data: PhantomData<T>,
}

impl<T> Reader<T> {
    pub async fn open(path: impl AsRef<std::path::Path>) -> io::Result<Self> {
        let file = File::open(path).await?;
        Ok(Self::from(file))
    }
}

impl<T> From<File> for Reader<T> {
    fn from(value: File) -> Self {
        let reader = BufReader::new(value);
        Self {
            lines: reader.lines(),
            phantom_data: PhantomData,
        }
    }
}

impl<T> Reader<T>
where
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

        let mut writer = Writer::<serde_json::Value>::create(&path)
            .await
            .expect("create writer should succeed");
        writer
            .write_value(&json!({"kind": "start", "id": 1}))
            .await
            .expect("first write should succeed");
        writer
            .write_value(&json!({"kind": "stop", "id": 2}))
            .await
            .expect("second write should succeed");
        writer.flush().await.expect("flush should succeed");

        let mut reader = Reader::<serde_json::Value>::open(&path)
            .await
            .expect("open reader should succeed");
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
