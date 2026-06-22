// TCP file transfer server and sender

use anyhow::Context;
use bytes::{BufMut, BytesMut};
use futures_util::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_util::codec::Framed;

use crate::codec::TransferCodec;
use crate::protocol::{FileMetadata, FileToken, TransferCommand, TransferFrame};
use crate::state::{AppState, FileTransferInfo, ProgressEvent, TransferState, TransferStatus};

const CHUNK_SIZE: usize = 64 * 1024; // 64KB chunks

pub struct TransferServer {
    listener: TcpListener,
    state: Arc<AppState>,
}

impl TransferServer {
    pub async fn new(port: u16, state: Arc<AppState>) -> anyhow::Result<Self> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
            .await
            .with_context(|| format!("Failed to bind TCP server on port {}", port))?;
        Ok(Self { listener, state })
    }

    pub async fn run(self) {
        loop {
            match self.listener.accept().await {
                Ok((stream, peer_addr)) => {
                    tracing::info!("New transfer connection from {}", peer_addr);
                    let state = Arc::clone(&self.state);
                    tokio::spawn(async move {
                        if let Err(e) = handle_transfer_connection(stream, state).await {
                            tracing::error!("Transfer error from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("Accept error: {}", e);
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }
}

async fn handle_transfer_connection(
    stream: tokio::net::TcpStream,
    state: Arc<AppState>,
) -> anyhow::Result<()> {
    stream.set_nodelay(true)?;
    let mut framed = Framed::new(stream, TransferCodec);

    // Receive first message: must be PrepareTransfer
    let first_msg = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("Connection closed"))??;

    let (session_id, files) = match first_msg {
        TransferFrame::Message(TransferCommand::PrepareTransfer { session_id, files }) => {
            (session_id, files)
        }
        _ => return Err(anyhow::anyhow!("Expected PrepareTransfer as first message")),
    };

    tracing::info!(
        "Incoming transfer from peer: {} files",
        files.len()
    );

    // Auto-accept all files
    let accepted_files: HashMap<String, FileToken> = files
        .iter()
        .map(|f| {
            (
                f.id.clone(),
                FileToken {
                    token: uuid::Uuid::new_v4().to_string(),
                    accepted: true,
                },
            )
        })
        .collect();

    // Send response
    let response = TransferCommand::TransferResponse {
        session_id: session_id.clone(),
        accepted_files: accepted_files.clone(),
    };
    framed.send(TransferFrame::Message(response)).await?;

    // Create transfer state
    let transfer_files: Vec<FileTransferInfo> = files
        .iter()
        .map(|f| {
            FileTransferInfo {
                file_id: f.id.clone(),
                name: f.name.clone(),
                size: f.size,
                bytes_transferred: 0,
                mime_type: f.mime_type.clone(),
                sha256: f.sha256.clone(),
            }
        })
        .collect();

    let transfer = TransferState {
        session_id: session_id.clone(),
        peer_alias: "peer".to_string(),
        files: transfer_files,
        status: TransferStatus::Waiting,
    };
    state.add_transfer(transfer).await;
    state
        .update_transfer_status(&session_id, TransferStatus::InProgress)
        .await;

    // Receive chunks
    let mut active_file: Option<ActiveReceive> = None;

    while let Some(frame) = framed.next().await {
        match frame? {
            TransferFrame::Message(TransferCommand::FileChunk {
                file_id,
                offset,
                data_len,
                token,
                ..
            }) => {
                // Verify token
                if let Some(ft) = accepted_files.get(&file_id) {
                    if ft.token != token {
                        return Err(anyhow::anyhow!("Invalid token for file {}", file_id));
                    }
                }

                // Initialize file handle if needed
                if active_file.as_ref().map(|f| &f.file_id) != Some(&file_id) {
                    let meta = files.iter().find(|f| f.id == file_id).ok_or_else(|| {
                        anyhow::anyhow!("Unknown file {}", file_id)
                    })?;

                    let save_path = state.download_dir.join(&meta.name);
                    let file = tokio::fs::File::create(&save_path).await?;

                    active_file = Some(ActiveReceive {
                        file_id: file_id.clone(),
                        file,
                        path: save_path,
                        bytes_received: 0,
                        total_size: meta.size,
                        hasher: Sha256::new(),
                    });
                }

                // Next frame should be the data
                if let Some(Ok(TransferFrame::Data(data))) = framed.next().await {
                    let active = active_file.as_mut().unwrap();
                    use tokio::io::AsyncWriteExt;
                    active.file.write_all(&data).await?;
                    active.hasher.update(&data);
                    active.bytes_received += data.len() as u64;

                    // Update progress
                    state
                        .broadcast_progress(ProgressEvent {
                            session_id: session_id.clone(),
                            file_id: file_id.clone(),
                            file_name: active.path.file_name()
                                .unwrap_or_default()
                                .to_string_lossy()
                                .to_string(),
                            bytes: active.bytes_received,
                            total: active.total_size,
                        });
                }
            }

            TransferFrame::Message(TransferCommand::FileComplete {
                file_id,
                sha256,
                ..
            }) => {
                if let Some(active) = active_file.take() {
                    let computed_hash = hex::encode(active.hasher.finalize());
                    if computed_hash != sha256 {
                        tracing::error!(
                            "SHA-256 mismatch for {}: expected {}, got {}",
                            active.path.display(),
                            sha256,
                            computed_hash
                        );
                    } else {
                        tracing::info!(
                            "File received: {} ({} bytes)",
                            active.path.display(),
                            active.bytes_received
                        );
                    }
                }
            }

            TransferFrame::Message(TransferCommand::CancelTransfer { .. }) => {
                tracing::info!("Transfer cancelled");
                break;
            }

            _ => {}
        }
    }

    state
        .update_transfer_status(&session_id, TransferStatus::Completed)
        .await;

    Ok(())
}

struct ActiveReceive {
    file_id: String,
    file: tokio::fs::File,
    path: PathBuf,
    bytes_received: u64,
    total_size: u64,
    hasher: Sha256,
}

// Sender side
pub struct FileSender {
    pub state: Arc<AppState>,
}

impl FileSender {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub async fn send_file(
        &self,
        file_path: &std::path::Path,
        target_addr: std::net::SocketAddr,
    ) -> anyhow::Result<()> {
        let stream = tokio::net::TcpStream::connect(target_addr)
            .await
            .with_context(|| format!("Failed to connect to {}", target_addr))?;
        stream.set_nodelay(true)?;

        let metadata = tokio::fs::metadata(file_path).await?;
        let file_size = metadata.len();

        // Compute SHA-256 hash
        let sha256 = self.compute_sha256(file_path).await?;

        let file_meta = FileMetadata {
            id: uuid::Uuid::new_v4().to_string(),
            name: file_path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            size: file_size,
            mime_type: mime_guess::from_path(file_path)
                .first_or_octet_stream()
                .to_string(),
            sha256: sha256.clone(),
        };

        let session_id = uuid::Uuid::new_v4().to_string();

        let mut framed = Framed::new(stream, TransferCodec);

        // Send prepare transfer
        let prepare = TransferCommand::PrepareTransfer {
            session_id: session_id.clone(),
            files: vec![file_meta.clone()],
        };
        framed.send(TransferFrame::Message(prepare)).await?;

        // Wait for response
        let response = framed
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("No response from receiver"))??;

        let tokens = match response {
            TransferFrame::Message(TransferCommand::TransferResponse {
                accepted_files, ..
            }) => accepted_files,
            _ => return Err(anyhow::anyhow!("Unexpected response from receiver")),
        };

        let file_id = &file_meta.id;
        let token = tokens
            .get(file_id)
            .ok_or_else(|| anyhow::anyhow!("File not accepted by receiver"))?;

        if !token.accepted {
            return Err(anyhow::anyhow!("File rejected by receiver"));
        }

        // Send file in chunks
        let mut file = tokio::fs::File::open(file_path).await?;
        let mut offset = 0u64;
        let mut buf = vec![0u8; CHUNK_SIZE];

        loop {
            let bytes_read = tokio::io::AsyncReadExt::read(&mut file, &mut buf).await?;
            if bytes_read == 0 {
                break;
            }

            let chunk = buf[..bytes_read].to_vec();
            let chunk_len = chunk.len() as u32;

            let cmd = TransferCommand::FileChunk {
                session_id: session_id.clone(),
                file_id: file_id.clone(),
                token: token.token.clone(),
                offset,
                data_len: chunk_len,
            };

            framed.send(TransferFrame::Message(cmd)).await?;
            framed.send(TransferFrame::Data(chunk)).await?;

            offset += bytes_read as u64;

            // Report progress
            self.state
                .broadcast_progress(ProgressEvent {
                    session_id: session_id.clone(),
                    file_id: file_id.clone(),
                    file_name: file_meta.name.clone(),
                    bytes: offset,
                    total: file_size,
                });
        }

        // Send completion
        let complete = TransferCommand::FileComplete {
            session_id: session_id.clone(),
            file_id: file_id.clone(),
            sha256,
        };
        framed.send(TransferFrame::Message(complete)).await?;

        tracing::info!("File sent: {} ({} bytes)", file_meta.name, file_size);

        Ok(())
    }

    async fn compute_sha256(&self, path: &std::path::Path) -> anyhow::Result<String> {
        let mut file = tokio::fs::File::open(path).await?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 8192];

        loop {
            let n = tokio::io::AsyncReadExt::read(&mut file, &mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }

        Ok(hex::encode(hasher.finalize()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_sender_creation() {
        let state = Arc::new(
            crate::state::AppState::new(
                "TestPC".to_string(),
                45678,
                45679,
                8080,
                PathBuf::from("/tmp"),
            ),
        );
        let sender = FileSender::new(state);
        assert_eq!(sender.state.alias, "TestPC");
    }

    #[test]
    fn test_chunk_size() {
        assert_eq!(CHUNK_SIZE, 64 * 1024);
    }
}
