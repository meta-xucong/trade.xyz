use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::domain::{CoordinatorSignal, WorkerRegistration, WorkerReport};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CoordinatorMessage {
    Signal(Box<CoordinatorSignal>),
    Shutdown,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerMessage {
    Register(WorkerRegistration),
    Report(WorkerReport),
}

pub async fn write_json_line<T: Serialize>(writer: &mut OwnedWriteHalf, value: &T) -> Result<()> {
    let mut encoded = serde_json::to_vec(value).context("failed to serialize IPC message")?;
    encoded.push(b'\n');
    writer
        .write_all(&encoded)
        .await
        .context("failed to write IPC message")?;
    writer
        .flush()
        .await
        .context("failed to flush IPC message")?;
    Ok(())
}

pub async fn read_json_line<T: DeserializeOwned>(
    reader: &mut BufReader<OwnedReadHalf>,
) -> Result<Option<T>> {
    let mut line = String::new();
    let bytes = reader
        .read_line(&mut line)
        .await
        .context("failed to read IPC message")?;
    if bytes == 0 {
        return Ok(None);
    }
    let message = serde_json::from_str::<T>(line.trim()).context("failed to parse IPC message")?;
    Ok(Some(message))
}
