// Protocol types for TCP file transfer

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransferCommand {
    PrepareTransfer {
        session_id: String,
        files: Vec<FileMetadata>,
    },
    TransferResponse {
        session_id: String,
        accepted_files: HashMap<String, FileToken>,
    },
    FileChunk {
        session_id: String,
        file_id: String,
        token: String,
        offset: u64,
        data_len: u32,
    },
    FileComplete {
        session_id: String,
        file_id: String,
        sha256: String,
    },
    CancelTransfer {
        session_id: String,
    },
    ResumeRequest {
        session_id: String,
        file_id: String,
        from_offset: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub id: String,
    pub name: String,
    pub size: u64,
    pub mime_type: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileToken {
    pub token: String,
    pub accepted: bool,
}

#[derive(Debug, Clone)]
pub enum TransferFrame {
    Message(TransferCommand),
    Data(Vec<u8>),
}
