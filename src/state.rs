// Shared application state

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{broadcast, oneshot, RwLock};
use tokio_util::sync::CancellationToken;

use crate::history::History;

/// How long an incoming transfer waits for the user to click accept/reject.
pub const DECISION_TIMEOUT: Duration = Duration::from_secs(120);

/// Finished transfers stay in the list this long so the UI can show them.
const HISTORY_TTL_SECS: u64 = 600;

pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub struct AppState {
    pub alias: String,
    pub tcp_port: u16,
    pub udp_port: u16,
    pub http_port: u16,
    pub download_dir: PathBuf,
    pub history: History,
    pub peers: RwLock<HashMap<String, PeerInfo>>,
    pub transfers: RwLock<HashMap<String, TransferState>>,
    /// Sessions blocked waiting for the user's verdict.
    pending: RwLock<HashMap<String, oneshot::Sender<bool>>>,
    /// One token per active session. Cancelling it is how a "Cancel" click in
    /// the UI reaches the actual send/receive loop, which may be blocked deep
    /// in a socket write with no other way to interrupt it.
    cancel_tokens: RwLock<HashMap<String, CancellationToken>>,
    pub events: broadcast::Sender<Event>,
    /// Discovery's own socket. Manual probes must go out from the port peers
    /// reply to, otherwise the answer lands on an ephemeral port nobody reads.
    pub discovery_socket: RwLock<Option<Arc<tokio::net::UdpSocket>>>,
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

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Send,
    Recv,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum TransferStatus {
    /// Waiting for the remote user to accept.
    Pending,
    Active,
    Completed,
    Rejected,
    /// Stopped mid-transfer by either side clicking "Cancel", as opposed to
    /// `Rejected` (never started) or `Failed` (an actual error).
    Cancelled,
    Failed { error: String },
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TransferState {
    pub session_id: String,
    pub peer: String,
    pub direction: Direction,
    pub files: Vec<FileProgress>,
    pub status: TransferStatus,
    pub started_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FileProgress {
    pub file_id: String,
    pub name: String,
    pub size: u64,
    pub bytes: u64,
    pub done: bool,
    pub error: Option<String>,
}

/// Everything pushed over the WebSocket. Tagged so the UI can switch on `type`.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Event {
    /// Someone wants to send us files — the UI must prompt.
    Incoming {
        session_id: String,
        peer: String,
        files: Vec<FileProgress>,
        total_size: u64,
    },
    Decided {
        session_id: String,
        accepted: bool,
    },
    Progress {
        session_id: String,
        direction: Direction,
        file_id: String,
        name: String,
        bytes: u64,
        total: u64,
    },
    FileDone {
        session_id: String,
        file_id: String,
        name: String,
        error: Option<String>,
    },
    SessionDone {
        session_id: String,
        direction: Direction,
        peer: String,
        status: TransferStatus,
        file_count: usize,
        total_size: u64,
    },
}

impl TransferState {
    pub fn total_size(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }
}

impl AppState {
    pub fn new(
        alias: String,
        tcp_port: u16,
        udp_port: u16,
        http_port: u16,
        download_dir: PathBuf,
    ) -> Self {
        let (events, _) = broadcast::channel(256);

        // A history DB that fails to open (read-only filesystem, disk full)
        // shouldn't take the whole app down with it — same "best effort"
        // treatment discovery gets when its UDP socket can't bind.
        let history_path = download_dir.join(".swiftshare-history.db");
        let history = History::open(&history_path).unwrap_or_else(|e| {
            tracing::warn!("Could not open history db at {}: {:#}. Using in-memory history for this session.", history_path.display(), e);
            History::open_in_memory().expect("in-memory sqlite must always open")
        });

        Self {
            alias,
            tcp_port,
            udp_port,
            http_port,
            download_dir,
            history,
            peers: RwLock::new(HashMap::new()),
            transfers: RwLock::new(HashMap::new()),
            pending: RwLock::new(HashMap::new()),
            cancel_tokens: RwLock::new(HashMap::new()),
            events,
            discovery_socket: RwLock::new(None),
        }
    }

    pub fn fingerprint(&self) -> String {
        use sha2::{Digest, Sha256};
        let data = format!("{}:{}:{}", self.alias, self.tcp_port, self.udp_port);
        hex::encode(Sha256::digest(data))
    }

    pub fn emit(&self, event: Event) {
        // Errors only mean nobody has the UI open.
        let _ = self.events.send(event);
    }

    // --- peers ---

    pub async fn add_peer(&self, peer: PeerInfo) {
        self.peers.write().await.insert(peer.fingerprint.clone(), peer);
    }

    pub async fn get_peers(&self) -> Vec<PeerInfo> {
        let now = now_secs();
        let mut peers: Vec<PeerInfo> = self
            .peers
            .read()
            .await
            .values()
            // saturating: a peer with a skewed clock must not underflow-panic here
            .filter(|p| now.saturating_sub(p.last_seen) < 30)
            .cloned()
            .collect();
        peers.sort_by(|a, b| a.alias.cmp(&b.alias));
        peers
    }

    // --- transfers ---

    pub async fn add_transfer(&self, transfer: TransferState) {
        self.transfers
            .write()
            .await
            .insert(transfer.session_id.clone(), transfer);
    }

    /// Registers a fresh token for this session and returns it. Call once per
    /// session, before the send/receive loop starts.
    pub async fn register_cancellable(&self, session_id: &str) -> CancellationToken {
        let token = CancellationToken::new();
        self.cancel_tokens
            .write()
            .await
            .insert(session_id.to_string(), token.clone());
        token
    }

    /// Returns false if the session is unknown or already finished — the UI
    /// treats that as "too late to cancel" rather than an error.
    pub async fn cancel(&self, session_id: &str) -> bool {
        let Some(token) = self.cancel_tokens.read().await.get(session_id).cloned() else {
            return false;
        };
        token.cancel();
        // A pending (not yet accepted) transfer has no active loop to catch
        // the token — reject it directly so Cancel works before approval too.
        self.decide(session_id, false).await;
        true
    }

    pub async fn set_status(&self, session_id: &str, status: TransferStatus) {
        let mut transfers = self.transfers.write().await;
        let Some(t) = transfers.get_mut(session_id) else {
            return;
        };
        let finished = !matches!(status, TransferStatus::Pending | TransferStatus::Active);
        t.status = status.clone();
        if finished {
            t.finished_at = Some(now_secs());
        }
        let (direction, peer, file_count, total_size) =
            (t.direction, t.peer.clone(), t.files.len(), t.total_size());
        if finished {
            self.history.record(t);
        }
        drop(transfers);

        if finished {
            self.cancel_tokens.write().await.remove(session_id);
            self.emit(Event::SessionDone {
                session_id: session_id.to_string(),
                direction,
                peer,
                status,
                file_count,
                total_size,
            });
        }
    }

    pub async fn set_progress(&self, session_id: &str, file_id: &str, bytes: u64) {
        let mut transfers = self.transfers.write().await;
        if let Some(t) = transfers.get_mut(session_id) {
            if let Some(f) = t.files.iter_mut().find(|f| f.file_id == file_id) {
                f.bytes = bytes;
            }
        }
    }

    pub async fn finish_file(&self, session_id: &str, file_id: &str, error: Option<String>) {
        let mut transfers = self.transfers.write().await;
        let Some(t) = transfers.get_mut(session_id) else {
            return;
        };
        let Some(f) = t.files.iter_mut().find(|f| f.file_id == file_id) else {
            return;
        };
        f.done = true;
        f.error = error.clone();
        if error.is_none() {
            f.bytes = f.size;
        }
        let name = f.name.clone();
        drop(transfers);

        self.emit(Event::FileDone {
            session_id: session_id.to_string(),
            file_id: file_id.to_string(),
            name,
            error,
        });
    }

    /// Active first, then most recent. Finished entries expire after HISTORY_TTL_SECS.
    pub async fn list_transfers(&self) -> Vec<TransferState> {
        let now = now_secs();
        let mut transfers = self.transfers.write().await;
        transfers.retain(|_, t| {
            t.finished_at
                .is_none_or(|at| now.saturating_sub(at) < HISTORY_TTL_SECS)
        });

        let mut list: Vec<TransferState> = transfers.values().cloned().collect();
        list.sort_by_key(|t| (t.finished_at.is_some(), std::cmp::Reverse(t.started_at)));
        list
    }

    // --- accept / reject ---

    /// Blocks until the user decides, the session is dropped, or we time out.
    /// A timeout is a rejection: never write files nobody approved.
    pub async fn await_decision(&self, session_id: &str) -> bool {
        let (tx, rx) = oneshot::channel();
        self.pending.write().await.insert(session_id.to_string(), tx);

        let accepted = matches!(tokio::time::timeout(DECISION_TIMEOUT, rx).await, Ok(Ok(true)));

        self.pending.write().await.remove(session_id);
        self.emit(Event::Decided {
            session_id: session_id.to_string(),
            accepted,
        });
        accepted
    }

    /// Returns false if the session already timed out or was answered.
    pub async fn decide(&self, session_id: &str, accept: bool) -> bool {
        let Some(tx) = self.pending.write().await.remove(session_id) else {
            return false;
        };
        tx.send(accept).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh temp dir per call so AppState's on-disk history db never
    /// collides with another test (or leaves a shared `/tmp` file behind).
    fn state() -> AppState {
        let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        AppState::new("TestPC".into(), 45678, 45679, 8080, dir.path().to_path_buf())
    }

    fn peer(alias: &str, last_seen: u64) -> PeerInfo {
        PeerInfo {
            alias: alias.into(),
            fingerprint: alias.into(),
            ip: "192.168.1.10".into(),
            tcp_port: 45678,
            udp_port: 45679,
            last_seen,
        }
    }

    #[tokio::test]
    async fn test_add_and_get_peers() {
        let state = state();
        state.add_peer(peer("OtherPC", now_secs())).await;
        let peers = state.get_peers().await;
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].alias, "OtherPC");
    }

    #[tokio::test]
    async fn test_stale_peers_dropped() {
        let state = state();
        state.add_peer(peer("OldPC", 0)).await;
        assert_eq!(state.get_peers().await.len(), 0);
    }

    /// A peer whose clock is ahead used to underflow-panic in debug builds.
    #[tokio::test]
    async fn test_future_timestamp_does_not_panic() {
        let state = state();
        state.add_peer(peer("FuturePC", now_secs() + 5_000)).await;
        assert_eq!(state.get_peers().await.len(), 1);
    }

    #[tokio::test]
    async fn test_decision_accept() {
        let state = Arc::new(state());
        let s = Arc::clone(&state);
        let waiter = tokio::spawn(async move { s.await_decision("sess1").await });

        // Let the waiter register before deciding.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(state.decide("sess1", true).await);
        assert!(waiter.await.unwrap());
    }

    #[tokio::test]
    async fn test_decision_reject() {
        let state = Arc::new(state());
        let s = Arc::clone(&state);
        let waiter = tokio::spawn(async move { s.await_decision("sess2").await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(state.decide("sess2", false).await);
        assert!(!waiter.await.unwrap());
    }

    #[tokio::test]
    async fn test_decide_unknown_session() {
        let state = state();
        assert!(!state.decide("nope", true).await);
    }

    #[tokio::test]
    async fn test_cancel_trips_the_token() {
        let state = state();
        let token = state.register_cancellable("sess-cancel").await;
        assert!(!token.is_cancelled());

        assert!(state.cancel("sess-cancel").await);
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_cancel_unknown_session_is_false() {
        let state = state();
        assert!(!state.cancel("ghost").await);
    }

    #[tokio::test]
    async fn test_cancel_before_approval_rejects_it() {
        let state = Arc::new(state());
        state.register_cancellable("sess-pending").await;

        let s = Arc::clone(&state);
        let waiter = tokio::spawn(async move { s.await_decision("sess-pending").await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(state.cancel("sess-pending").await);
        assert!(!waiter.await.unwrap(), "cancelling before approval must reject, not hang");
    }

    #[tokio::test]
    async fn test_finished_transfer_drops_its_token() {
        let state = state();
        state
            .add_transfer(TransferState {
                session_id: "sess-done".into(),
                peer: "Peer".into(),
                direction: Direction::Send,
                files: vec![],
                status: TransferStatus::Active,
                started_at: now_secs(),
                finished_at: None,
            })
            .await;
        state.register_cancellable("sess-done").await;

        state.set_status("sess-done", TransferStatus::Completed).await;

        // No token left to cancel — the loop it guarded is long gone.
        assert!(!state.cancel("sess-done").await);
    }
}
