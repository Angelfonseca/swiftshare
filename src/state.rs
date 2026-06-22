// Shared application state

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{broadcast, RwLock};



#[derive(Clone)]
pub struct AppState {
    pub alias: String,
    pub tcp_port: u16,
    pub udp_port: u16,
    pub http_port: u16,
    pub download_dir: PathBuf,
    pub peers: Arc<RwLock<HashMap<String, PeerInfo>>>,
    pub transfers: Arc<RwLock<HashMap<String, TransferState>>>,
    pub progress_tx: broadcast::Sender<ProgressEvent>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerInfo {
    pub alias: String,
    pub fingerprint: String,
    pub ip: String,
    pub tcp_port: u16,
    pub udp_port: u16,
    pub last_seen: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TransferState {
    pub session_id: String,
    pub peer_alias: String,
    pub files: Vec<FileTransferInfo>,
    pub status: TransferStatus,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FileTransferInfo {
    pub file_id: String,
    pub name: String,
    pub size: u64,
    pub bytes_transferred: u64,
    pub mime_type: String,
    pub sha256: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub enum TransferStatus {
    Waiting,
    InProgress,
    Completed,
    Failed(String),
    Cancelled,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProgressEvent {
    pub session_id: String,
    pub file_id: String,
    pub file_name: String,
    pub bytes: u64,
    pub total: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IncomingTransfer {
    pub session_id: String,
    pub peer_alias: String,
    pub files: Vec<FileMetadata>,
    pub accepted: Option<bool>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FileMetadata {
    pub id: String,
    pub name: String,
    pub size: u64,
    pub mime_type: String,
    pub sha256: String,
}

impl AppState {
    pub fn new(
        alias: String,
        tcp_port: u16,
        udp_port: u16,
        http_port: u16,
        download_dir: PathBuf,
    ) -> Self {
        let (progress_tx, _) = broadcast::channel(100);

        Self {
            alias,
            tcp_port,
            udp_port,
            http_port,
            download_dir,
            peers: Arc::new(RwLock::new(HashMap::new())),
            transfers: Arc::new(RwLock::new(HashMap::new())),
            progress_tx,
        }
    }

    pub fn local_peer_info(&self) -> PeerInfo {
        PeerInfo {
            alias: self.alias.clone(),
            fingerprint: self.generate_fingerprint(),
            ip: "127.0.0.1".to_string(),
            tcp_port: self.tcp_port,
            udp_port: self.udp_port,
            last_seen: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    fn generate_fingerprint(&self) -> String {
        use sha2::{Sha256, Digest};
        let data = format!("{}:{}:{}", self.alias, self.tcp_port, self.udp_port);
        let hash = Sha256::digest(data);
        hex::encode(hash)
    }

    pub async fn add_peer(&self, peer: PeerInfo) {
        let mut peers = self.peers.write().await;
        peers.insert(peer.fingerprint.clone(), peer);
    }

    pub async fn get_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        peers
            .values()
            .filter(|p| now - p.last_seen < 30)
            .cloned()
            .collect()
    }

    pub async fn add_transfer(&self, transfer: TransferState) {
        let mut transfers = self.transfers.write().await;
        transfers.insert(transfer.session_id.clone(), transfer);
    }

    pub async fn update_transfer_status(&self, session_id: &str, status: TransferStatus) {
        let mut transfers = self.transfers.write().await;
        if let Some(t) = transfers.get_mut(session_id) {
            t.status = status;
        }
    }

    pub async fn get_active_transfers(&self) -> Vec<TransferState> {
        let transfers = self.transfers.read().await;
        transfers
            .values()
            .filter(|t| {
                matches!(
                    t.status,
                    TransferStatus::Waiting | TransferStatus::InProgress
                )
            })
            .cloned()
            .collect()
    }

    pub fn broadcast_progress(&self, event: ProgressEvent) {
        let _ = self.progress_tx.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_and_get_peers() {
        let state = AppState::new(
            "TestPC".to_string(),
            45678,
            45679,
            8080,
            PathBuf::from("/tmp"),
        );

        let peer = PeerInfo {
            alias: "OtherPC".to_string(),
            fingerprint: "abc123".to_string(),
            ip: "192.168.1.10".to_string(),
            tcp_port: 45678,
            udp_port: 45679,
            last_seen: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        state.add_peer(peer).await;
        let peers = state.get_peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].alias, "OtherPC");
        assert_eq!(peers[0].ip, "192.168.1.10");
    }

    #[tokio::test]
    async fn test_prune_stale_peers() {
        let state = AppState::new(
            "TestPC".to_string(),
            45678,
            45679,
            8080,
            PathBuf::from("/tmp"),
        );

        let peer = PeerInfo {
            alias: "OldPC".to_string(),
            fingerprint: "old".to_string(),
            ip: "192.168.1.20".to_string(),
            tcp_port: 45678,
            udp_port: 45679,
            last_seen: 0,
        };

        state.add_peer(peer).await;
        let peers = state.get_peers().await;
        assert_eq!(peers.len(), 0);
    }

    #[tokio::test]
    async fn test_add_transfer() {
        let state = AppState::new(
            "TestPC".to_string(),
            45678,
            45679,
            8080,
            PathBuf::from("/tmp"),
        );

        let transfer = TransferState {
            session_id: "sess1".to_string(),
            peer_alias: "OtherPC".to_string(),
            files: vec![],
            status: TransferStatus::Waiting,
        };

        state.add_transfer(transfer).await;
        let active = state.get_active_transfers().await;
        assert_eq!(active.len(), 1);
    }
}
