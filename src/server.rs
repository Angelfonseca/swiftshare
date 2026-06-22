// Web UI server

use axum::{
    extract::{DefaultBodyLimit, Multipart, Query, State, WebSocketUpgrade},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use axum::extract::ws::{Message, WebSocket};
use futures_util::{StreamExt, SinkExt};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio_util::codec::Framed;
use crate::codec::TransferCodec;
use crate::protocol::{TransferCommand, TransferFrame, FileMetadata};
use crate::state::{AppState, PeerInfo, TransferState};

pub async fn start_web_ui(state: Arc<AppState>, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/", get(index))
        .route("/app.js", get(static_js))
        .route("/styles.css", get(static_css))
        .route("/api/peers", get(list_peers))
        .route("/api/peers/connect", post(manual_connect))
        .route("/api/send", post(send_file))
        .route("/api/files/list", get(list_available_files))
        .route("/api/incoming", get(list_incoming))
        .route("/api/transfers", get(list_transfers))
        .route("/api/ws", get(ws_handler))
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024 * 1024)) // 10GB max
        .with_state(state);

    let addr = format!("127.0.0.1:{}", port);
    tracing::info!("Web UI listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn index() -> impl IntoResponse {
    axum::response::Response::builder()
        .header("content-type", "text/html; charset=utf-8")
        .body(axum::body::Body::from(include_str!("../web/index.html").to_string()))
        .unwrap()
}

async fn static_js() -> impl IntoResponse {
    axum::response::Response::builder()
        .header("content-type", "application/javascript")
        .body(axum::body::Body::from(include_str!("../web/app.js").to_string()))
        .unwrap()
}

async fn static_css() -> impl IntoResponse {
    axum::response::Response::builder()
        .header("content-type", "text/css")
        .body(axum::body::Body::from(include_str!("../web/styles.css").to_string()))
        .unwrap()
}

async fn list_peers(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<PeerInfo>> {
    let peers = state.get_peers().await;
    Json(peers)
}

async fn manual_connect(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ManualConnectRequest>,
) -> Json<serde_json::Value> {
    let ip = payload.ip.trim().to_string();
    let tcp_port = payload.tcp_port;

    // Validate IP
    if !ip.contains('.') || ip.split('.').count() != 4 {
        return Json(serde_json::json!({ "error": "Formato IP inválido. Usa: xxx.xxx.xxx.xxx" }));
    }

    // Try multiple discovery methods
    let mut found = false;

    // Method 1: Direct UDP to IP:45679
    let direct_addr = format!("{}:{}", ip, 45679);
    if let Ok(addr) = direct_addr.parse::<std::net::SocketAddr>() {
        if let Err(e) = send_discovery_to(addr).await {
            tracing::warn!("Direct discovery failed: {}", e);
        } else {
            found = true;
        }
    }

    // Method 2: Broadcast to subnet 192.168.x.255
    let parts: Vec<&str> = ip.split('.').collect();
    if parts.len() == 4 {
        let subnet = format!("{}.255:45679", parts[..3].join("."));
        if let Ok(addr) = subnet.parse::<std::net::SocketAddr>() {
            let _ = send_discovery_to(addr).await;
        }
    }

    // Method 3: Broadcast to 255.255.255.255
    if let Ok(addr) = "255.255.255.255:45679".parse::<std::net::SocketAddr>() {
        let _ = send_discovery_to(addr).await;
    }

    if found {
        Json(serde_json::json!({ "status": "success", "message": format!("Probing {}... Espera 3 segundos para que responda", ip) }))
    } else {
        Json(serde_json::json!({ "status": "probing", "message": format!("Enviando probe a {}... Espera 3 segundos", ip) }))
    }
}

async fn send_file(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SendQueryParams>,
    mut multipart: Multipart,
) -> Json<serde_json::Value> {
    let target_ip = params.target_ip.unwrap_or_default();
    let target_tcp_port = params.target_tcp_port.unwrap_or(45678);
    let mut saved_files = Vec::new();

    if target_ip.is_empty() {
        return Json(serde_json::json!({
            "status": "error",
            "error": "Selecciona un dispositivo antes de enviar"
        }));
    }

    loop {
        match multipart.next_field().await {
            Ok(Some(field)) => {
                if field.file_name().is_none() { continue; }

                let file_name = field.file_name().unwrap_or("unknown").to_string();
                let content_type = field.content_type().unwrap_or("application/octet-stream").to_string();

                let target_addr = format!("{}:{}", target_ip, target_tcp_port);
                match target_addr.parse::<std::net::SocketAddr>() {
                    Ok(addr) => {
                        match forward_stream_to_peer(field, &file_name, addr, state.clone()).await {
                            Ok(size) => {
                                saved_files.push(serde_json::json!({
                                    "name": file_name,
                                    "size": size,
                                    "type": content_type
                                }));
                            }
                            Err(e) => {
                                return Json(serde_json::json!({
                                    "status": "error",
                                    "error": format!("Error al enviar al peer: {}", e)
                                }));
                            }
                        }
                    }
                    Err(_) => {
                        return Json(serde_json::json!({
                            "status": "error",
                            "error": format!("Dirección inválida: {}", target_addr)
                        }));
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                return Json(serde_json::json!({
                    "status": "error",
                    "error": format!("Error al recibir archivo: {}", e)
                }));
            }
        }
    }

    Json(serde_json::json!({
        "status": "saved",
        "files": saved_files
    }))
}

#[derive(serde::Deserialize)]
struct SendQueryParams {
    target_ip: Option<String>,
    target_tcp_port: Option<u16>,
}

/// Save uploaded file to temp, forward to peer via TCP, then delete temp
async fn forward_stream_to_peer(
    mut field: axum::extract::multipart::Field<'_>,
    file_name: &str,
    target_addr: std::net::SocketAddr,
    state: Arc<AppState>,
) -> Result<u64, String> {
    use tokio::io::AsyncWriteExt;

    // Save to temp file first
    let temp_dir = std::env::temp_dir().join("swiftshare");
    tokio::fs::create_dir_all(&temp_dir).await
        .map_err(|e| format!("Error creating temp dir: {}", e))?;

    let temp_path = temp_dir.join(file_name);
    let partial_path = temp_dir.join(format!("{}.partial", file_name));

    let mut file = tokio::fs::File::create(&partial_path).await
        .map_err(|e| format!("Error creating temp file: {}", e))?;

    let mut total: u64 = 0;
    loop {
        match field.chunk().await {
            Ok(Some(chunk)) => {
                file.write_all(&chunk).await
                    .map_err(|e| format!("Error writing temp file: {}", e))?;
                total += chunk.len() as u64;
            }
            Ok(None) => break,
            Err(e) => {
                let _ = tokio::fs::remove_file(&partial_path).await;
                return Err(format!("Error reading upload: {}", e));
            }
        }
    }

    file.flush().await.map_err(|e| format!("Error flushing: {}", e))?;
    drop(file);
    tokio::fs::rename(&partial_path, &temp_path).await
        .map_err(|e| format!("Error renaming: {}", e))?;

    // Forward via FileSender (handles token exchange correctly)
    let sender = crate::transfer::FileSender::new(state);
    sender.send_file(&temp_path, target_addr).await
        .map_err(|e| format!("Error enviando al peer: {}", e))?;

    let _ = tokio::fs::remove_file(&temp_path).await;
    Ok(total)
}

async fn list_available_files(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<serde_json::Value>> {
    let mut files = Vec::new();
    let temp_dir = state.download_dir.join("swiftshare-inbox");

    if let Ok(mut entries) = tokio::fs::read_dir(&temp_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.file_type().await.map(|ft| ft.is_file()).unwrap_or(false) {
                if let Ok(metadata) = entry.metadata().await {
                    files.push(serde_json::json!({
                        "name": entry.file_name().to_string_lossy().to_string(),
                        "size": metadata.len(),
                        "path": entry.path().to_string_lossy().to_string(),
                    }));
                }
            }
        }
    }

    Json(files)
}

async fn list_incoming(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<TransferState>> {
    let transfers = state.get_active_transfers().await;
    Json(transfers)
}

async fn list_transfers(
    State(state): State<Arc<AppState>>,
) -> Json<Vec<TransferState>> {
    let transfers = state.get_active_transfers().await;
    Json(transfers)
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(socket: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut _receiver) = socket.split();
    let mut rx = state.progress_tx.subscribe();

    tokio::spawn(async move {
        while let Ok(event) = rx.recv().await {
            let msg = serde_json::to_string(&event).unwrap_or_default();
            if sender.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });
}

async fn send_discovery_to(addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_broadcast(true)?;
    socket.set_nonblocking(true)?;

    let std_socket: std::net::UdpSocket = socket.into();
    let tokio_socket = tokio::net::UdpSocket::from_std(std_socket)?;

    let msg = serde_json::json!({ "type": "discovery", "addr": addr.to_string() });
    let data = serde_json::to_vec(&msg)?;
    tokio_socket.send_to(&data, addr).await?;

    Ok(())
}

#[derive(serde::Deserialize)]
struct ManualConnectRequest {
    ip: String,
    tcp_port: u16,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn test_index_returns_html() {
        let state = crate::state::AppState::new(
            "TestPC".to_string(),
            45678,
            45679,
            8080,
            std::path::PathBuf::from("/tmp"),
        );
        let app = Router::new()
            .route("/", get(index))
            .with_state(Arc::new(state));

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), 200);
    }
}
