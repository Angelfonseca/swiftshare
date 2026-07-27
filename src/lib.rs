// swiftshare library: the CLI binary and the Tauri shell both build on this.
//
// `start()` wires up discovery, the TCP transfer server and the web UI as
// background tasks and hands back the shared state plus the address the UI
// actually bound to — the caller decides what to do with a running app
// (block on ctrl-c, point a native window at it, whatever).

pub mod cli;
pub mod codec;
pub mod discovery;
pub mod history;
pub mod protocol;
pub mod server;
pub mod state;
pub mod transfer;

use std::sync::Arc;

pub struct RunningApp {
    pub state: Arc<state::AppState>,
    pub http_addr: std::net::SocketAddr,
}

pub async fn start(cli: &cli::Cli) -> anyhow::Result<RunningApp> {
    let alias = cli.resolve_alias();
    let download_dir = cli.resolve_download_dir();
    tokio::fs::create_dir_all(&download_dir).await?;

    let state = Arc::new(state::AppState::new(
        alias.clone(),
        cli.tcp_port,
        cli.udp_port,
        cli.http_port,
        download_dir.clone(),
    ));

    let local_info = discovery::DiscoveryMessage {
        alias: alias.clone(),
        fingerprint: state.fingerprint(),
        tcp_port: cli.tcp_port,
        udp_port: cli.udp_port,
        http_port: cli.http_port,
        announce: false,
    };

    // Discovery is best-effort: manual connect by IP still works without it.
    match discovery::DiscoveryService::new(state.clone(), local_info).await {
        Ok(discovery) => {
            let discovery = Arc::new(discovery);
            *state.discovery_socket.write().await = Some(discovery.socket());

            tokio::spawn(Arc::clone(&discovery).listen());
            tokio::spawn(Arc::clone(&discovery).periodic_announce());
            tokio::spawn(discovery.prune_stale_peers());
            tracing::info!("UDP discovery active on port {}", cli.udp_port);
        }
        Err(e) => {
            tracing::warn!("UDP discovery unavailable ({}). Use manual connect by IP.", e);
        }
    }

    let tcp_server = transfer::TransferServer::new(cli.tcp_port, state.clone()).await?;
    tokio::spawn(async move { tcp_server.run().await });

    let listener = server::bind(cli.http_port).await?;
    let http_addr = listener.local_addr()?;
    let web_state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = server::serve(listener, web_state).await {
            tracing::error!("Web UI error: {:#}", e);
        }
    });

    tracing::info!("swiftshare listo — http://{} | alias: {}", http_addr, alias);
    tracing::info!("Los archivos recibidos van a {}", download_dir.display());

    Ok(RunningApp { state, http_addr })
}
