// Web UI server

use axum::{
    body::Body,
    extract::ws::{Message, WebSocket},
    extract::{DefaultBodyLimit, Multipart, Path, Query, State, WebSocketUpgrade},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;

use crate::history::HistoryEntry;
use crate::protocol::FileMetadata;
use crate::state::{AppState, PeerInfo, TransferState};
use crate::transfer::SendSession;

fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/app.js", get(static_js))
        .route("/styles.css", get(static_css))
        .route("/fonts/{name}", get(static_font))
        .route("/img/logo-mark.png", get(static_logo_mark))
        .route("/img/favicon.png", get(static_favicon))
        .route("/api/state", get(get_state))
        .route("/api/peers", get(list_peers))
        .route("/api/peers/connect", post(manual_connect))
        .route("/api/send", post(send_files))
        .route("/api/decision", post(decide))
        .route("/api/transfers", get(list_transfers))
        .route("/api/transfers/cancel", post(cancel_transfer))
        .route("/api/history", get(list_history))
        .route("/api/history/clear", post(clear_history))
        .route("/api/received", get(list_received))
        .route("/api/received/open", post(open_download_dir))
        .route("/api/ws", get(ws_handler))
        .layer(DefaultBodyLimit::disable()) // uploads stream through; nothing is buffered
        .with_state(state)
}

/// Binds the UI's listener and returns the address actually bound — pass
/// `port: 0` to let the OS pick one, then read it back here before starting
/// anything that needs to know it (e.g. a Tauri window navigating to it).
///
/// Loopback only: the UI drives local send/receive/accept decisions, and has
/// no auth of its own — anyone who could reach it over the network could
/// accept transfers and read received files in your name.
pub async fn bind(port: u16) -> anyhow::Result<tokio::net::TcpListener> {
    let addr = format!("127.0.0.1:{}", port);
    Ok(tokio::net::TcpListener::bind(&addr).await?)
}

pub async fn serve(listener: tokio::net::TcpListener, state: Arc<AppState>) -> anyhow::Result<()> {
    tracing::info!("Web UI listening on http://{}", listener.local_addr()?);
    axum::serve(listener, build_router(state)).await?;
    Ok(())
}

pub async fn start_web_ui(state: Arc<AppState>, port: u16) -> anyhow::Result<()> {
    let listener = bind(port).await?;
    serve(listener, state).await
}

fn asset(content_type: &'static str, body: &'static str) -> axum::response::Response {
    axum::response::Response::builder()
        .header("content-type", content_type)
        .body(axum::body::Body::from(body))
        .unwrap()
}

async fn index() -> impl IntoResponse {
    asset("text/html; charset=utf-8", include_str!("../web/index.html"))
}

async fn static_js() -> impl IntoResponse {
    asset("application/javascript; charset=utf-8", include_str!("../web/app.js"))
}

async fn static_css() -> impl IntoResponse {
    asset("text/css; charset=utf-8", include_str!("../web/styles.css"))
}

const FONTS: &[(&str, &[u8])] = &[
    ("chakra-petch-500.woff2", include_bytes!("../web/fonts/chakra-petch-500.woff2")),
    ("chakra-petch-600.woff2", include_bytes!("../web/fonts/chakra-petch-600.woff2")),
    ("chakra-petch-700.woff2", include_bytes!("../web/fonts/chakra-petch-700.woff2")),
    ("sora-400.woff2", include_bytes!("../web/fonts/sora-400.woff2")),
    ("sora-600.woff2", include_bytes!("../web/fonts/sora-600.woff2")),
    ("plex-mono-400.woff2", include_bytes!("../web/fonts/plex-mono-400.woff2")),
    ("plex-mono-500.woff2", include_bytes!("../web/fonts/plex-mono-500.woff2")),
];

async fn static_logo_mark() -> impl IntoResponse {
    ([("content-type", "image/png")], include_bytes!("../web/img/logo-mark.png").as_slice())
}

async fn static_favicon() -> impl IntoResponse {
    ([("content-type", "image/png")], include_bytes!("../web/img/favicon.png").as_slice())
}

async fn static_font(Path(name): Path<String>) -> Response {
    match FONTS.iter().find(|(n, _)| *n == name) {
        Some((_, bytes)) => Response::builder()
            .header("content-type", "font/woff2")
            .header("cache-control", "public, max-age=31536000, immutable")
            .body(Body::from(*bytes))
            .unwrap(),
        None => Response::builder().status(404).body(Body::empty()).unwrap(),
    }
}

async fn get_state(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "alias": state.alias,
        "tcp_port": state.tcp_port,
        "udp_port": state.udp_port,
        "http_port": state.http_port,
        "download_dir": state.download_dir.to_string_lossy(),
    }))
}

async fn list_peers(State(state): State<Arc<AppState>>) -> Json<Vec<PeerInfo>> {
    Json(state.get_peers().await)
}

async fn list_transfers(State(state): State<Arc<AppState>>) -> Json<Vec<TransferState>> {
    Json(state.list_transfers().await)
}

/// Persisted history survives restarts, unlike `list_transfers` (in-memory,
/// pruned after HISTORY_TTL_SECS) — this is "what did I send last week".
async fn list_history(State(state): State<Arc<AppState>>) -> Json<Vec<HistoryEntry>> {
    let entries = tokio::task::spawn_blocking(move || state.history.recent(200))
        .await
        .unwrap_or_default();
    Json(entries)
}

async fn clear_history(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let _ = tokio::task::spawn_blocking(move || state.history.clear()).await;
    Json(serde_json::json!({ "status": "ok" }))
}

// ---------------------------------------------------------------- accept/reject

#[derive(serde::Deserialize)]
struct DecisionRequest {
    session_id: String,
    accept: bool,
}

async fn decide(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DecisionRequest>,
) -> Json<serde_json::Value> {
    if state.decide(&req.session_id, req.accept).await {
        Json(serde_json::json!({ "status": "ok" }))
    } else {
        Json(serde_json::json!({
            "status": "error",
            "error": "La solicitud ya expiró o fue respondida"
        }))
    }
}

#[derive(serde::Deserialize)]
struct CancelRequest {
    session_id: String,
}

/// Works for either direction: the token that stops the loop lives in this
/// process's AppState regardless of whether we're the sender or receiver.
async fn cancel_transfer(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CancelRequest>,
) -> Json<serde_json::Value> {
    if state.cancel(&req.session_id).await {
        Json(serde_json::json!({ "status": "ok" }))
    } else {
        Json(serde_json::json!({
            "status": "error",
            "error": "La transferencia ya terminó"
        }))
    }
}

// ---------------------------------------------------------------- sending

#[derive(serde::Deserialize)]
struct SendQuery {
    target_ip: String,
    target_tcp_port: Option<u16>,
    target_alias: Option<String>,
}

/// One manifest entry per file, sent as the first multipart field so the peer
/// can show names and sizes in its approval prompt before any bytes move.
#[derive(serde::Deserialize)]
struct ManifestEntry {
    name: String,
    size: u64,
    relative_path: Option<String>,
}

fn err(msg: impl std::fmt::Display) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "error", "error": msg.to_string() }))
}

/// Streams the upload straight through to the peer: no temp file, no second
/// read for hashing. The response only comes back once the peer has the bytes.
async fn send_files(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SendQuery>,
    mut multipart: Multipart,
) -> Json<serde_json::Value> {
    let target = format!(
        "{}:{}",
        params.target_ip.trim(),
        params.target_tcp_port.unwrap_or(45678)
    );
    let Ok(addr) = target.parse::<std::net::SocketAddr>() else {
        return err(format!("Dirección inválida: {}", target));
    };

    // First field: the manifest.
    let manifest: Vec<ManifestEntry> = match multipart.next_field().await {
        Ok(Some(field)) if field.name() == Some("manifest") => match field.text().await {
            Ok(text) => match serde_json::from_str(&text) {
                Ok(m) => m,
                Err(e) => return err(format!("Manifiesto inválido: {}", e)),
            },
            Err(e) => return err(format!("No se pudo leer el manifiesto: {}", e)),
        },
        Ok(_) => return err("Falta el manifiesto de archivos"),
        Err(e) => return err(format!("Error leyendo la petición: {}", e)),
    };

    if manifest.is_empty() {
        return err("No hay archivos que enviar");
    }

    let metas: Vec<FileMetadata> = manifest
        .iter()
        .map(|entry| FileMetadata {
            id: uuid::Uuid::new_v4().to_string(),
            name: entry.name.clone(),
            size: entry.size,
            mime_type: mime_guess::from_path(&entry.name)
                .first_or_octet_stream()
                .to_string(),
            relative_path: entry.relative_path.clone(),
        })
        .collect();

    let peer_name = params.target_alias.unwrap_or_else(|| params.target_ip.clone());
    let mut session = match SendSession::open(state, addr, peer_name, metas.clone()).await {
        Ok(s) => s,
        Err(e) => return err(format!("{:#}", e)),
    };
    let session_id = session.session_id().to_string();

    for meta in &metas {
        if let Err(e) = send_one_file(&mut session, &mut multipart, meta).await {
            // A cancel (ours or the receiver's) surfaces as a plain error from
            // send_one_file; is_cancelled() is what tells the two apart so we
            // report "Cancelled" instead of "Failed".
            if session.is_cancelled() {
                session.cancel_and_notify().await;
                return err("Transferencia cancelada");
            }
            let msg = format!("{:#}", e);
            session.fail(msg.clone()).await;
            return err(msg);
        }
    }

    match session.finish().await {
        Ok(()) => Json(serde_json::json!({
            "status": "ok",
            "session_id": session_id,
            "files": metas.len(),
        })),
        Err(e) => err(format!("{:#}", e)),
    }
}

/// One file: pull its multipart field, stream it through, close it out.
async fn send_one_file(
    session: &mut SendSession,
    multipart: &mut Multipart,
    meta: &FileMetadata,
) -> anyhow::Result<()> {
    let mut field = multipart
        .next_field()
        .await?
        .ok_or_else(|| anyhow::anyhow!("El navegador envió menos archivos de los anunciados"))?;

    session.start_file(meta).await?;
    while let Some(chunk) = field.chunk().await? {
        session.write(&chunk).await?;
    }
    session.finish_file().await?;
    Ok(())
}

// ---------------------------------------------------------------- received files

async fn list_received(State(state): State<Arc<AppState>>) -> Json<Vec<serde_json::Value>> {
    let mut files = Vec::new();

    if let Ok(mut entries) = tokio::fs::read_dir(&state.download_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with('.') || name.ends_with(".part") {
                continue;
            }
            let Ok(meta) = entry.metadata().await else { continue };
            files.push(serde_json::json!({
                "name": name,
                "size": meta.len(),
                "is_dir": meta.is_dir(),
                "modified": meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs()),
            }));
        }
    }

    files.sort_by_key(|f| std::cmp::Reverse(f["modified"].as_u64().unwrap_or(0)));
    Json(files)
}

async fn open_download_dir(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    match open::that_detached(&state.download_dir) {
        Ok(()) => Json(serde_json::json!({ "status": "ok" })),
        Err(e) => err(format!("No se pudo abrir la carpeta: {}", e)),
    }
}

// ---------------------------------------------------------------- discovery probe

#[derive(serde::Deserialize)]
struct ManualConnectRequest {
    ip: String,
}

/// Unicast probe for networks where broadcast is filtered. The peer answers
/// on the discovery port, which is why this must reuse discovery's socket.
async fn manual_connect(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ManualConnectRequest>,
) -> Json<serde_json::Value> {
    let ip = payload.ip.trim();
    let Ok(target) = format!("{}:{}", ip, state.udp_port).parse::<std::net::SocketAddr>() else {
        return err(format!("IP inválida: {}", ip));
    };

    let socket = state.discovery_socket.read().await.clone();
    let Some(socket) = socket else {
        return err("El descubrimiento UDP no está disponible en este equipo");
    };

    let msg = crate::discovery::DiscoveryMessage {
        alias: state.alias.clone(),
        fingerprint: state.fingerprint(),
        tcp_port: state.tcp_port,
        udp_port: state.udp_port,
        http_port: state.http_port,
        announce: true,
    };

    match socket.send_to(&serde_json::to_vec(&msg).unwrap_or_default(), target).await {
        Ok(_) => {
            tracing::info!("Manual discovery probe sent to {}", target);
            Json(serde_json::json!({
                "status": "ok",
                "message": format!("Sonda enviada a {}", ip)
            }))
        }
        Err(e) => err(format!("No se pudo contactar a {}: {}", ip, e)),
    }
}

// ---------------------------------------------------------------- websocket

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(socket: WebSocket, state: Arc<AppState>) {
    let (mut tx, mut rx) = socket.split();
    let mut events = state.events.subscribe();

    // Drain incoming frames so pings/closes are handled and the task ends when
    // the browser goes away.
    let mut client = tokio::spawn(async move { while let Some(Ok(_)) = rx.next().await {} });

    loop {
        tokio::select! {
            event = events.recv() => match event {
                Ok(event) => {
                    let payload = match serde_json::to_string(&event) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    if tx.send(Message::Text(payload.into())).await.is_err() {
                        break;
                    }
                }
                // Lagged: a slow tab missed events. Progress is re-sent
                // continuously, so keep going instead of dropping the socket.
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(_) => break,
            },
            _ = &mut client => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    fn test_state() -> Arc<AppState> {
        let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        Arc::new(AppState::new(
            "TestPC".to_string(),
            45678,
            45679,
            8080,
            dir.path().to_path_buf(),
        ))
    }

    #[tokio::test]
    async fn test_index_returns_html() {
        let app = Router::new().route("/", get(index)).with_state(test_state());
        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn test_decision_on_unknown_session_reports_error() {
        let app = Router::new()
            .route("/api/decision", post(decide))
            .with_state(test_state());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/decision")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"session_id":"ghost","accept":true}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "error");
    }
}
