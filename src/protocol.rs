// Protocol types for TCP file transfer
//
// Frame layout on the wire: [u8 kind][u32 be len][payload]
//   kind 0 = JSON TransferCommand, kind 1 = raw file bytes.
//
// A session is: PrepareTransfer -> TransferResponse -> (StartFile, Data*, FileComplete)* -> SessionComplete

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransferCommand {
    /// Sender announces the whole batch. Receiver asks the user to accept.
    PrepareTransfer {
        session_id: String,
        peer_alias: String,
        files: Vec<FileMetadata>,
    },
    /// Receiver's verdict, one token per accepted file.
    TransferResponse {
        session_id: String,
        accepted_files: HashMap<String, FileToken>,
    },
    /// All Data frames until FileComplete belong to this file.
    StartFile {
        session_id: String,
        file_id: String,
        token: String,
    },
    FileComplete {
        session_id: String,
        file_id: String,
        sha256: String,
    },
    SessionComplete {
        session_id: String,
    },
    CancelTransfer {
        session_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub id: String,
    pub name: String,
    pub size: u64,
    pub mime_type: String,
    /// Path relative to the dropped folder root, if this came from a folder.
    pub relative_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileToken {
    pub token: String,
    pub accepted: bool,
}

#[derive(Debug, Clone)]
pub enum TransferFrame {
    Message(TransferCommand),
    Data(Bytes),
}
