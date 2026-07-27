// TCP file transfer: receiving server and sending session.

use anyhow::Context;
use bytes::BytesMut;
use futures_util::{FutureExt, SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;

use crate::codec::TransferCodec;
use crate::protocol::{FileMetadata, FileToken, TransferCommand, TransferFrame};
use crate::state::{
    now_secs, AppState, Direction, Event, FileProgress, TransferState, TransferStatus,
    DECISION_TIMEOUT,
};

/// Bytes coalesced before hitting the socket. The old 64KB was a syscall every
/// 64KB plus a JSON header frame per chunk; this is where the throughput went.
const CHUNK_SIZE: usize = 512 * 1024;
const WRITE_BUF: usize = 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Cap on UI/WebSocket progress chatter — a 10GB file would otherwise emit
/// twenty thousand events.
const PROGRESS_INTERVAL: Duration = Duration::from_millis(100);
/// Sender waits a little longer than the receiver's own decision timeout.
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(DECISION_TIMEOUT.as_secs() + 15);
/// Upper bound on draining unread bytes before closing a cancelled
/// connection. See the comment at its use site for why this exists at all.
const CANCEL_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

// ---------------------------------------------------------------- paths

/// Turn a peer-supplied name/relative path into a path guaranteed to stay
/// inside `base`. Everything here is attacker-controlled: a peer that sends
/// `../../.ssh/authorized_keys` must not get a write outside the download dir.
pub fn safe_join(base: &Path, raw: &str) -> Option<PathBuf> {
    let mut out = base.to_path_buf();
    let mut depth = 0usize;

    for part in raw.replace('\\', "/").split('/') {
        let part = part.trim();
        if part.is_empty() || part == "." {
            continue;
        }
        // Reject before any rewriting: stripping trailing dots first would
        // silently turn ".." into an empty, skippable component.
        if part.chars().all(|c| c == '.') || part.contains('\0') || part.contains(':') {
            return None;
        }
        // Windows ignores trailing dots and spaces, which would let
        // "evil.exe." resolve to a different name than the one approved.
        let part = part.trim_end_matches(['.', ' ']);
        if part.is_empty() {
            return None;
        }
        if Path::new(part).components().count() != 1
            || !matches!(Path::new(part).components().next(), Some(Component::Normal(_)))
        {
            return None;
        }
        out.push(part);
        depth += 1;
    }

    // ponytail: rejects traversal by construction; a pre-existing symlink inside
    // the download dir could still redirect a write. Canonicalize per-component
    // if untrusted peers ever get write access to that directory.
    (depth > 0).then_some(out)
}

/// `report.pdf` -> `report (1).pdf` when the name is taken.
async fn unique_path(path: PathBuf) -> PathBuf {
    if !path.exists() {
        return path;
    }
    let stem = path.file_stem().unwrap_or_default().to_string_lossy().to_string();
    let ext = path.extension().map(|e| format!(".{}", e.to_string_lossy()));
    let parent = path.parent().unwrap_or(Path::new(".")).to_path_buf();

    for n in 1..10_000 {
        let candidate = parent.join(format!("{} ({}){}", stem, n, ext.as_deref().unwrap_or("")));
        if !candidate.exists() {
            return candidate;
        }
    }
    path
}

/// Peer aliases land in the UI and in logs; keep them short and printable.
fn clean_alias(raw: &str) -> String {
    let cleaned: String = raw.chars().filter(|c| !c.is_control()).take(64).collect();
    let cleaned = cleaned.trim().to_string();
    if cleaned.is_empty() {
        "Desconocido".to_string()
    } else {
        cleaned
    }
}

// ---------------------------------------------------------------- receiving

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
                    let state = Arc::clone(&self.state);
                    tokio::spawn(async move {
                        if let Err(e) = receive(stream, state).await {
                            tracing::error!("Transfer from {} failed: {:#}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("Accept error: {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

enum ReceiveOutcome {
    Done,
    CancelledLocally,
    CancelledByPeer(String),
}

struct ActiveReceive {
    file_id: String,
    name: String,
    writer: BufWriter<tokio::fs::File>,
    /// Written to `.part` first so an aborted transfer never leaves a file
    /// that looks complete.
    partial_path: PathBuf,
    final_path: PathBuf,
    received: u64,
    total: u64,
    hasher: Sha256,
    last_emit: Instant,
}

async fn receive(stream: TcpStream, state: Arc<AppState>) -> anyhow::Result<()> {
    stream.set_nodelay(true)?;
    let mut framed = Framed::new(stream, TransferCodec);

    let first = framed
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("Connection closed before handshake"))??;

    let (session_id, peer_alias, files) = match first {
        TransferFrame::Message(TransferCommand::PrepareTransfer {
            session_id,
            peer_alias,
            files,
        }) => (session_id, clean_alias(&peer_alias), files),
        _ => return Err(anyhow::anyhow!("Expected PrepareTransfer as first message")),
    };

    if files.is_empty() {
        return Err(anyhow::anyhow!("PrepareTransfer with no files"));
    }

    // Resolve every destination up front: if any path is hostile, refuse the
    // whole batch rather than half-writing it.
    let mut destinations: HashMap<String, PathBuf> = HashMap::new();
    for f in &files {
        let raw = f.relative_path.as_deref().unwrap_or(&f.name);
        match safe_join(&state.download_dir, raw) {
            Some(path) => {
                destinations.insert(f.id.clone(), path);
            }
            None => {
                let reason = format!("Ruta de archivo no permitida: {}", f.name);
                tracing::warn!("Rejecting transfer from {}: {}", peer_alias, reason);
                let _ = framed
                    .send(TransferFrame::Message(TransferCommand::CancelTransfer {
                        session_id,
                        reason: reason.clone(),
                    }))
                    .await;
                return Err(anyhow::anyhow!(reason));
            }
        }
    }

    let progress: Vec<FileProgress> = files
        .iter()
        .map(|f| FileProgress {
            file_id: f.id.clone(),
            name: f.relative_path.clone().unwrap_or_else(|| f.name.clone()),
            size: f.size,
            bytes: 0,
            done: false,
            error: None,
        })
        .collect();

    state
        .add_transfer(TransferState {
            session_id: session_id.clone(),
            peer: peer_alias.clone(),
            direction: Direction::Recv,
            files: progress.clone(),
            status: TransferStatus::Pending,
            started_at: now_secs(),
            finished_at: None,
        })
        .await;

    // Registered before the approval prompt so a cancel click works whether
    // the user is still deciding or the transfer is already under way.
    let cancel = state.register_cancellable(&session_id).await;

    state.emit(Event::Incoming {
        session_id: session_id.clone(),
        peer: peer_alias.clone(),
        total_size: progress.iter().map(|f| f.size).sum(),
        files: progress,
    });

    tracing::info!(
        "Incoming: {} file(s) from {} — waiting for approval",
        files.len(),
        peer_alias
    );

    let accepted = state.await_decision(&session_id).await;

    let accepted_files: HashMap<String, FileToken> = files
        .iter()
        .map(|f| {
            (
                f.id.clone(),
                FileToken {
                    token: uuid::Uuid::new_v4().to_string(),
                    accepted,
                },
            )
        })
        .collect();

    framed
        .send(TransferFrame::Message(TransferCommand::TransferResponse {
            session_id: session_id.clone(),
            accepted_files: accepted_files.clone(),
        }))
        .await?;

    if !accepted {
        tracing::info!("Transfer from {} rejected", peer_alias);
        state.set_status(&session_id, TransferStatus::Rejected).await;
        return Ok(());
    }

    state.set_status(&session_id, TransferStatus::Active).await;
    tokio::fs::create_dir_all(&state.download_dir).await?;

    match receive_files(
        &mut framed,
        &state,
        &session_id,
        &files,
        &accepted_files,
        &destinations,
        &cancel,
    )
    .await
    {
        Ok(ReceiveOutcome::Done) => {
            state.set_status(&session_id, TransferStatus::Completed).await;
            Ok(())
        }
        Ok(ReceiveOutcome::CancelledLocally) => {
            tracing::info!("Transfer from {} cancelled by us", peer_alias);
            let _ = framed
                .send(TransferFrame::Message(TransferCommand::CancelTransfer {
                    session_id: session_id.clone(),
                    reason: "Cancelado por el destinatario".into(),
                }))
                .await;
            // Dropping the socket now, with the sender's bytes still queued
            // up unread, tends to make the OS send a RST instead of a clean
            // FIN — and a RST can take the CancelTransfer frame we just sent
            // down with it, leaving the sender to fail on a bare "broken
            // pipe" instead of hearing why. Draining first (bounded, in case
            // the sender never notices and keeps pushing) lets the message
            // actually arrive before the connection goes away.
            let _ = tokio::time::timeout(CANCEL_DRAIN_TIMEOUT, async {
                while framed.next().await.is_some() {}
            })
            .await;
            state.set_status(&session_id, TransferStatus::Cancelled).await;
            Ok(())
        }
        Ok(ReceiveOutcome::CancelledByPeer(reason)) => {
            tracing::info!("Transfer from {} cancelled by sender: {}", peer_alias, reason);
            state.set_status(&session_id, TransferStatus::Cancelled).await;
            Ok(())
        }
        Err(e) => {
            state
                .set_status(&session_id, TransferStatus::Failed { error: e.to_string() })
                .await;
            Err(e)
        }
    }
}

async fn receive_files(
    framed: &mut Framed<TcpStream, TransferCodec>,
    state: &Arc<AppState>,
    session_id: &str,
    files: &[FileMetadata],
    tokens: &HashMap<String, FileToken>,
    destinations: &HashMap<String, PathBuf>,
    cancel: &CancellationToken,
) -> anyhow::Result<ReceiveOutcome> {
    let mut active: Option<ActiveReceive> = None;

    let result: anyhow::Result<ReceiveOutcome> = async {
        loop {
            // biased: once cancelled, stop even if a frame is also ready —
            // the user asked to stop, not "stop after the next frame".
            let frame = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(ReceiveOutcome::CancelledLocally),
                frame = framed.next() => frame,
            };
            let Some(frame) = frame else { break };

            match frame? {
                TransferFrame::Message(TransferCommand::StartFile {
                    file_id, token, ..
                }) => {
                    // Constant work, but tokens are per-connection uuids and the
                    // list is tiny; no timing surface worth hardening here.
                    let expected = tokens
                        .get(&file_id)
                        .ok_or_else(|| anyhow::anyhow!("Unknown file {}", file_id))?;
                    if expected.token != token {
                        return Err(anyhow::anyhow!("Invalid token for file {}", file_id));
                    }

                    let meta = files
                        .iter()
                        .find(|f| f.id == file_id)
                        .ok_or_else(|| anyhow::anyhow!("Unknown file {}", file_id))?;
                    let dest = destinations
                        .get(&file_id)
                        .ok_or_else(|| anyhow::anyhow!("No destination for {}", file_id))?;

                    if let Some(parent) = dest.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    let final_path = unique_path(dest.clone()).await;
                    let mut partial = final_path.clone().into_os_string();
                    partial.push(".part");
                    let partial_path = PathBuf::from(partial);

                    let file = tokio::fs::File::create(&partial_path).await?;
                    active = Some(ActiveReceive {
                        file_id,
                        name: meta.relative_path.clone().unwrap_or_else(|| meta.name.clone()),
                        writer: BufWriter::with_capacity(WRITE_BUF, file),
                        partial_path,
                        final_path,
                        received: 0,
                        total: meta.size,
                        hasher: Sha256::new(),
                        last_emit: Instant::now() - PROGRESS_INTERVAL,
                    });
                }

                TransferFrame::Data(data) => {
                    let Some(a) = active.as_mut() else {
                        return Err(anyhow::anyhow!("Data frame outside of a file"));
                    };
                    a.writer.write_all(&data).await?;
                    a.hasher.update(&data);
                    a.received += data.len() as u64;

                    if a.last_emit.elapsed() >= PROGRESS_INTERVAL {
                        a.last_emit = Instant::now();
                        state.set_progress(session_id, &a.file_id, a.received).await;
                        state.emit(Event::Progress {
                            session_id: session_id.to_string(),
                            direction: Direction::Recv,
                            file_id: a.file_id.clone(),
                            name: a.name.clone(),
                            bytes: a.received,
                            total: a.total,
                        });
                    }
                }

                TransferFrame::Message(TransferCommand::FileComplete {
                    file_id, sha256, ..
                }) => {
                    let Some(mut a) = active.take() else {
                        return Err(anyhow::anyhow!("FileComplete without an open file"));
                    };
                    if a.file_id != file_id {
                        return Err(anyhow::anyhow!("FileComplete for the wrong file"));
                    }
                    a.writer.flush().await?;
                    a.writer.shutdown().await?;

                    let computed = hex::encode(a.hasher.finalize());
                    if computed != sha256 {
                        // A corrupt file must not be left looking valid.
                        let _ = tokio::fs::remove_file(&a.partial_path).await;
                        let err = format!("Checksum no coincide para {}", a.name);
                        tracing::error!("{} (expected {}, got {})", err, sha256, computed);
                        state.finish_file(session_id, &a.file_id, Some(err)).await;
                        continue;
                    }

                    tokio::fs::rename(&a.partial_path, &a.final_path).await?;
                    tracing::info!("Received {} ({} bytes)", a.final_path.display(), a.received);
                    state.set_progress(session_id, &a.file_id, a.received).await;
                    state.finish_file(session_id, &a.file_id, None).await;
                }

                TransferFrame::Message(TransferCommand::SessionComplete { .. }) => break,

                // The sender stopped, not an error on our side.
                TransferFrame::Message(TransferCommand::CancelTransfer { reason, .. }) => {
                    return Ok(ReceiveOutcome::CancelledByPeer(reason));
                }

                _ => {}
            }
        }
        Ok(ReceiveOutcome::Done)
    }
    .await;

    // Whatever went wrong, don't leave a half-written .part behind.
    if let Some(a) = active {
        let _ = tokio::fs::remove_file(&a.partial_path).await;
        let interrupted = match &result {
            Ok(ReceiveOutcome::CancelledLocally) => Some("Cancelado".to_string()),
            Ok(ReceiveOutcome::CancelledByPeer(_)) => Some("Cancelado por el emisor".to_string()),
            Ok(ReceiveOutcome::Done) => Some("Transferencia interrumpida".to_string()),
            Err(_) => None,
        };
        if let Some(msg) = interrupted {
            state.finish_file(session_id, &a.file_id, Some(msg)).await;
        }
    }

    result
}

// ---------------------------------------------------------------- sending

/// A live outbound transfer. Bytes are pushed through as they arrive from the
/// browser — nothing is staged on disk and the file is never read twice.
pub struct SendSession {
    framed: Framed<TcpStream, TransferCodec>,
    session_id: String,
    tokens: HashMap<String, FileToken>,
    state: Arc<AppState>,
    buf: BytesMut,
    current: Option<CurrentSend>,
    cancel: CancellationToken,
    /// Set when `check_live` sees the receiver's own CancelTransfer, so
    /// `is_cancelled()` reports true even though our local token never fired.
    cancelled_by_peer: bool,
}

struct CurrentSend {
    file_id: String,
    name: String,
    sent: u64,
    total: u64,
    hasher: Sha256,
    last_emit: Instant,
}

impl SendSession {
    /// Connects, announces the batch and blocks until the peer accepts.
    pub async fn open(
        state: Arc<AppState>,
        target: std::net::SocketAddr,
        peer_name: String,
        files: Vec<FileMetadata>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(!files.is_empty(), "No hay archivos que enviar");

        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(target))
            .await
            .with_context(|| format!("Tiempo agotado conectando a {}", target))?
            .with_context(|| format!("No se pudo conectar a {}", target))?;
        stream.set_nodelay(true)?;

        let session_id = uuid::Uuid::new_v4().to_string();
        let cancel = state.register_cancellable(&session_id).await;
        let mut framed = Framed::new(stream, TransferCodec);

        framed
            .send(TransferFrame::Message(TransferCommand::PrepareTransfer {
                session_id: session_id.clone(),
                peer_alias: state.alias.clone(),
                files: files.clone(),
            }))
            .await?;

        state
            .add_transfer(TransferState {
                session_id: session_id.clone(),
                peer: peer_name,
                direction: Direction::Send,
                files: files
                    .iter()
                    .map(|f| FileProgress {
                        file_id: f.id.clone(),
                        name: f.relative_path.clone().unwrap_or_else(|| f.name.clone()),
                        size: f.size,
                        bytes: 0,
                        done: false,
                        error: None,
                    })
                    .collect(),
                status: TransferStatus::Pending,
                started_at: now_secs(),
                finished_at: None,
            })
            .await;

        // Cancelling while still waiting for approval must not leave the
        // caller blocked for the full RESPONSE_TIMEOUT.
        let response = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                state.set_status(&session_id, TransferStatus::Cancelled).await;
                return Err(anyhow::anyhow!("Cancelado por el usuario"));
            }
            response = tokio::time::timeout(RESPONSE_TIMEOUT, framed.next()) => {
                response
                    .map_err(|_| anyhow::anyhow!("El destinatario no respondió a tiempo"))?
                    .ok_or_else(|| anyhow::anyhow!("El destinatario cerró la conexión"))??
            }
        };

        let tokens = match response {
            TransferFrame::Message(TransferCommand::TransferResponse { accepted_files, .. }) => {
                accepted_files
            }
            TransferFrame::Message(TransferCommand::CancelTransfer { reason, .. }) => {
                return Err(anyhow::anyhow!("{}", reason));
            }
            _ => return Err(anyhow::anyhow!("Respuesta inesperada del destinatario")),
        };

        if !tokens.values().any(|t| t.accepted) {
            state
                .set_status(&session_id, TransferStatus::Rejected)
                .await;
            return Err(anyhow::anyhow!("El destinatario rechazó la transferencia"));
        }

        state.set_status(&session_id, TransferStatus::Active).await;

        Ok(Self {
            framed,
            session_id,
            tokens,
            state,
            buf: BytesMut::with_capacity(CHUNK_SIZE),
            current: None,
            cancel,
            cancelled_by_peer: false,
        })
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// True whether it was our own cancel button or the receiver's that
    /// stopped this session — either way the session ends as `Cancelled`,
    /// not `Failed`.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled() || self.cancelled_by_peer
    }

    /// Checked before every write. Catches both our own cancel button and the
    /// receiver's — `Framed` supports sending independently of reading, so a
    /// receiver that gave up mid-transfer can still push us a `CancelTransfer`
    /// without us ever running a background read loop for it.
    fn check_live(&mut self) -> anyhow::Result<()> {
        if self.cancel.is_cancelled() {
            anyhow::bail!("Cancelado por el usuario");
        }
        match self.framed.next().now_or_never() {
            None => Ok(()), // nothing waiting on the socket, keep going
            Some(None) => Err(anyhow::anyhow!("El destinatario cerró la conexión")),
            Some(Some(Ok(TransferFrame::Message(TransferCommand::CancelTransfer {
                reason,
                ..
            })))) => {
                self.cancelled_by_peer = true;
                Err(anyhow::anyhow!("Cancelado por el destinatario: {}", reason))
            }
            Some(Some(Ok(_))) => Ok(()), // unexpected during upload; ignore
            Some(Some(Err(e))) => Err(e.into()),
        }
    }

    pub async fn start_file(&mut self, meta: &FileMetadata) -> anyhow::Result<()> {
        let token = self
            .tokens
            .get(&meta.id)
            .filter(|t| t.accepted)
            .ok_or_else(|| anyhow::anyhow!("Archivo rechazado: {}", meta.name))?;

        self.framed
            .send(TransferFrame::Message(TransferCommand::StartFile {
                session_id: self.session_id.clone(),
                file_id: meta.id.clone(),
                token: token.token.clone(),
            }))
            .await?;

        self.current = Some(CurrentSend {
            file_id: meta.id.clone(),
            name: meta.relative_path.clone().unwrap_or_else(|| meta.name.clone()),
            sent: 0,
            total: meta.size,
            hasher: Sha256::new(),
            last_emit: Instant::now() - PROGRESS_INTERVAL,
        });
        Ok(())
    }

    /// Hashes on the way past, so the file is never read a second time.
    pub async fn write(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.check_live()?;

        let cur = self
            .current
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("write() sin start_file()"))?;
        cur.hasher.update(data);
        cur.sent += data.len() as u64;
        self.buf.extend_from_slice(data);

        while self.buf.len() >= CHUNK_SIZE {
            let chunk = self.buf.split_to(CHUNK_SIZE).freeze();
            self.framed.send(TransferFrame::Data(chunk)).await?;
        }

        let cur = self.current.as_mut().unwrap();
        if cur.last_emit.elapsed() >= PROGRESS_INTERVAL {
            cur.last_emit = Instant::now();
            let (file_id, name, sent, total) =
                (cur.file_id.clone(), cur.name.clone(), cur.sent, cur.total);
            self.state.set_progress(&self.session_id, &file_id, sent).await;
            self.state.emit(Event::Progress {
                session_id: self.session_id.clone(),
                direction: Direction::Send,
                file_id,
                name,
                bytes: sent,
                total,
            });
        }
        Ok(())
    }

    pub async fn finish_file(&mut self) -> anyhow::Result<u64> {
        let cur = self
            .current
            .take()
            .ok_or_else(|| anyhow::anyhow!("finish_file() sin start_file()"))?;

        if !self.buf.is_empty() {
            let chunk = self.buf.split().freeze();
            self.framed.send(TransferFrame::Data(chunk)).await?;
        }

        self.framed
            .send(TransferFrame::Message(TransferCommand::FileComplete {
                session_id: self.session_id.clone(),
                file_id: cur.file_id.clone(),
                sha256: hex::encode(cur.hasher.finalize()),
            }))
            .await?;

        self.state.finish_file(&self.session_id, &cur.file_id, None).await;
        Ok(cur.sent)
    }

    pub async fn finish(mut self) -> anyhow::Result<()> {
        self.framed
            .send(TransferFrame::Message(TransferCommand::SessionComplete {
                session_id: self.session_id.clone(),
            }))
            .await?;
        self.framed.flush().await?;
        self.state
            .set_status(&self.session_id, TransferStatus::Completed)
            .await;
        Ok(())
    }

    pub async fn fail(mut self, error: String) {
        let _ = self
            .framed
            .send(TransferFrame::Message(TransferCommand::CancelTransfer {
                session_id: self.session_id.clone(),
                reason: error.clone(),
            }))
            .await;
        self.state
            .set_status(&self.session_id, TransferStatus::Failed { error })
            .await;
    }

    /// Like `fail`, but for a stop the user asked for: `Cancelled` status
    /// instead of `Failed`, and skipped entirely if the receiver was the one
    /// who cancelled (no point echoing their own CancelTransfer back at them).
    pub async fn cancel_and_notify(mut self) {
        if self.cancel.is_cancelled() {
            let _ = self
                .framed
                .send(TransferFrame::Message(TransferCommand::CancelTransfer {
                    session_id: self.session_id.clone(),
                    reason: "Cancelado por el remitente".into(),
                }))
                .await;
        }
        self.state
            .set_status(&self.session_id, TransferStatus::Cancelled)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_join_normal() {
        let base = Path::new("/downloads");
        assert_eq!(
            safe_join(base, "report.pdf").unwrap(),
            PathBuf::from("/downloads/report.pdf")
        );
        assert_eq!(
            safe_join(base, "docs/2024/report.pdf").unwrap(),
            PathBuf::from("/downloads/docs/2024/report.pdf")
        );
    }

    #[test]
    fn test_safe_join_rejects_traversal() {
        let base = Path::new("/downloads");

        // Anything containing `..`, a drive prefix or a NUL is refused outright.
        for hostile in [
            "../../.ssh/authorized_keys",
            "..",
            "...",
            "docs/../../etc/passwd",
            "..\\..\\windows\\system32\\evil.dll",
            "C:\\windows\\evil.dll",
            "a\0b",
        ] {
            assert!(safe_join(base, hostile).is_none(), "accepted: {:?}", hostile);
        }

        // Nothing usable left.
        assert!(safe_join(base, "").is_none());
        assert!(safe_join(base, "   ").is_none());
        assert!(safe_join(base, "/").is_none());

        // An absolute path is re-rooted inside the download dir, not refused.
        assert_eq!(
            safe_join(base, "/etc/passwd").unwrap(),
            PathBuf::from("/downloads/etc/passwd")
        );

        // Windows drops trailing dots, so they must not survive into the name.
        assert_eq!(
            safe_join(base, "evil.exe.").unwrap(),
            PathBuf::from("/downloads/evil.exe")
        );
    }

    #[tokio::test]
    async fn test_unique_path_avoids_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.txt");
        assert_eq!(unique_path(path.clone()).await, path);

        tokio::fs::write(&path, b"x").await.unwrap();
        assert_eq!(
            unique_path(path.clone()).await,
            dir.path().join("a (1).txt")
        );
    }

    #[test]
    fn test_clean_alias() {
        assert_eq!(clean_alias("  Mac de Ana \n"), "Mac de Ana");
        assert_eq!(clean_alias(""), "Desconocido");
        assert_eq!(clean_alias(&"x".repeat(200)).len(), 64);
    }

    // ---- end to end over a real socket ----

    fn test_state(alias: &str, dir: PathBuf) -> Arc<AppState> {
        Arc::new(AppState::new(alias.to_string(), 0, 0, 0, dir))
    }

    /// Answers the first incoming request, standing in for a user clicking.
    fn auto_decide(state: Arc<AppState>, accept: bool) {
        tokio::spawn(async move {
            let mut events = state.events.subscribe();
            while let Ok(event) = events.recv().await {
                if let Event::Incoming { session_id, .. } = event {
                    state.decide(&session_id, accept).await;
                    return;
                }
            }
        });
    }

    fn meta(name: &str, size: u64, relative_path: Option<&str>) -> FileMetadata {
        FileMetadata {
            id: uuid::Uuid::new_v4().to_string(),
            name: name.to_string(),
            size,
            mime_type: "application/octet-stream".to_string(),
            relative_path: relative_path.map(str::to_string),
        }
    }

    async fn start_receiver(dir: PathBuf) -> (Arc<AppState>, std::net::SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let receiver = test_state("Receiver", dir);
        let server = TransferServer { listener, state: receiver.clone() };
        tokio::spawn(server.run());
        (receiver, addr)
    }

    #[tokio::test]
    async fn test_accepted_transfer_lands_intact() {
        let dir = tempfile::tempdir().unwrap();
        let (receiver, addr) = start_receiver(dir.path().to_path_buf()).await;
        auto_decide(receiver, true);

        // Bigger than CHUNK_SIZE so coalescing and multi-frame reassembly run.
        let payload: Vec<u8> = (0..1_500_000u32).map(|i| (i % 251) as u8).collect();
        let files = vec![
            meta("hello.txt", 5, None),
            meta("big.bin", payload.len() as u64, Some("carpeta/anidada/big.bin")),
        ];

        let sender = test_state("Sender", dir.path().join("unused"));
        let mut session = SendSession::open(sender, addr, "Receiver".into(), files.clone())
            .await
            .expect("receiver should accept");

        session.start_file(&files[0]).await.unwrap();
        session.write(b"hello").await.unwrap();
        session.finish_file().await.unwrap();

        session.start_file(&files[1]).await.unwrap();
        for part in payload.chunks(40_000) {
            session.write(part).await.unwrap();
        }
        session.finish_file().await.unwrap();
        session.finish().await.unwrap();

        // Wait for the receiver to flush and rename.
        for _ in 0..100 {
            if dir.path().join("carpeta/anidada/big.bin").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert_eq!(
            tokio::fs::read_to_string(dir.path().join("hello.txt")).await.unwrap(),
            "hello"
        );
        assert_eq!(
            tokio::fs::read(dir.path().join("carpeta/anidada/big.bin")).await.unwrap(),
            payload,
            "received bytes differ from what was sent"
        );

        let mut entries = tokio::fs::read_dir(dir.path()).await.unwrap();
        while let Some(e) = entries.next_entry().await.unwrap() {
            assert!(
                !e.file_name().to_string_lossy().ends_with(".part"),
                "left a partial file behind"
            );
        }
    }

    #[tokio::test]
    async fn test_sender_cancel_stops_transfer_and_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let (receiver, addr) = start_receiver(dir.path().to_path_buf()).await;
        auto_decide(receiver.clone(), true);

        // Bigger than CHUNK_SIZE so at least one Data frame actually reaches
        // the receiver before we cancel.
        let payload = vec![7u8; 700_000];
        let files = vec![meta("big.bin", payload.len() as u64, None)];
        let sender_state = test_state("Sender", dir.path().join("unused"));
        let mut session = SendSession::open(sender_state.clone(), addr, "Receiver".into(), files.clone())
            .await
            .unwrap();
        let session_id = session.session_id().to_string();

        session.start_file(&files[0]).await.unwrap();
        session.write(&payload).await.unwrap();

        assert!(sender_state.cancel(&session_id).await, "cancel() should find the live token");

        let result = session.write(b"more after cancel").await;
        assert!(result.is_err(), "write() must fail once cancelled");
        session.cancel_and_notify().await;

        // The receiver should learn about it and end up Cancelled, not stuck
        // Active or reporting a plain Failed.
        let mut status = None;
        for _ in 0..50 {
            let transfers = receiver.list_transfers().await;
            if let Some(t) = transfers.iter().find(|t| t.session_id == session_id) {
                if !matches!(t.status, TransferStatus::Active) {
                    status = Some(t.status.clone());
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            matches!(status, Some(TransferStatus::Cancelled)),
            "expected Cancelled, got {:?}",
            status
        );

        let mut entries = tokio::fs::read_dir(dir.path()).await.unwrap();
        while let Some(e) = entries.next_entry().await.unwrap() {
            let name = e.file_name().to_string_lossy().to_string();
            assert!(!name.ends_with(".part"), "left a partial file behind");
            assert_ne!(name, "big.bin", "a cancelled transfer must not produce the final file");
        }
    }

    #[tokio::test]
    async fn test_receiver_cancel_is_detected_by_sender() {
        let dir = tempfile::tempdir().unwrap();
        let (receiver, addr) = start_receiver(dir.path().to_path_buf()).await;
        auto_decide(receiver.clone(), true);

        // Cancel as soon as the transfer is genuinely under way, not just
        // pending approval.
        let receiver_for_cancel = receiver.clone();
        tokio::spawn(async move {
            let mut events = receiver_for_cancel.events.subscribe();
            while let Ok(event) = events.recv().await {
                if let Event::Progress { session_id, .. } = event {
                    receiver_for_cancel.cancel(&session_id).await;
                    return;
                }
            }
        });

        let payload = vec![9u8; 3_000_000];
        let files = vec![meta("big.bin", payload.len() as u64, None)];
        let sender_state = test_state("Sender", dir.path().join("unused"));
        let mut session = SendSession::open(sender_state, addr, "Receiver".into(), files.clone())
            .await
            .unwrap();
        session.start_file(&files[0]).await.unwrap();

        // check_live() is polled on every write(); keep feeding data until
        // either it reports the peer's cancellation or we time out, so a
        // regression here fails fast instead of hanging the suite.
        let detected = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                for chunk in payload.chunks(50_000) {
                    if session.write(chunk).await.is_err() {
                        return true;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or(false);

        assert!(detected, "sender never noticed the receiver had cancelled");
        // The sender's own cancel token never fired here — only the peer's
        // CancelTransfer did. is_cancelled() must still report true, or the
        // caller reports this as a plain Failed instead of Cancelled.
        assert!(
            session.is_cancelled(),
            "peer-initiated cancellation must count as cancelled, not just as a write error"
        );
    }

    #[tokio::test]
    async fn test_rejected_transfer_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (receiver, addr) = start_receiver(dir.path().to_path_buf()).await;
        auto_decide(receiver, false);

        let sender = test_state("Sender", dir.path().join("unused"));
        let result =
            SendSession::open(sender, addr, "Receiver".into(), vec![meta("secret.txt", 5, None)])
                .await;

        assert!(result.is_err(), "a rejected transfer must not open");
        assert!(!dir.path().join("secret.txt").exists());
    }

    #[tokio::test]
    async fn test_traversal_path_is_refused_before_prompting() {
        let dir = tempfile::tempdir().unwrap();
        let inbox = dir.path().join("inbox");
        let (receiver, addr) = start_receiver(inbox.clone()).await;
        auto_decide(receiver, true);

        let sender = test_state("Sender", dir.path().join("unused"));
        let result = SendSession::open(
            sender,
            addr,
            "Attacker".into(),
            vec![meta("pwned", 4, Some("../../pwned"))],
        )
        .await;

        assert!(result.is_err(), "receiver must refuse escaping paths");
        assert!(!dir.path().join("pwned").exists());
        assert!(!inbox.join("pwned").exists());
    }

    #[tokio::test]
    async fn test_unanswered_request_blocks_the_sender() {
        // Nobody decides: open() must keep waiting rather than write anything.
        let dir = tempfile::tempdir().unwrap();
        let (_receiver, addr) = start_receiver(dir.path().to_path_buf()).await;
        let sender = test_state("Sender", dir.path().join("unused"));

        let pending = tokio::time::timeout(
            Duration::from_millis(400),
            SendSession::open(sender, addr, "Receiver".into(), vec![meta("waiting.txt", 1, None)]),
        )
        .await;

        assert!(pending.is_err(), "open() should still be awaiting approval");
        assert!(!dir.path().join("waiting.txt").exists());
    }
}
