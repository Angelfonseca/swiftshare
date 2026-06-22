// Main entry point

mod cli;
mod codec;
mod discovery;
mod error;
mod protocol;
mod resume;
mod server;
mod state;
mod transfer;

use clap::Parser;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .init();

    let cli = cli::Cli::parse();

    let alias = cli.resolve_alias();
    let download_dir = cli.resolve_download_dir().clone();

    tracing::info!("swiftshare starting");
    tracing::info!("Alias: {}", alias);
    tracing::info!("TCP port: {}", cli.tcp_port);
    tracing::info!("UDP port: {}", cli.udp_port);
    tracing::info!("HTTP port: {}", cli.http_port);
    tracing::info!("Download dir: {:?}", download_dir);

    let state = Arc::new(state::AppState::new(
        alias.clone(),
        cli.tcp_port,
        cli.udp_port,
        cli.http_port,
        download_dir,
    ));

    let local_info = discovery::DiscoveryMessage {
        alias: alias.clone(),
        fingerprint: generate_fingerprint(&alias, cli.tcp_port, cli.udp_port),
        tcp_port: cli.tcp_port,
        udp_port: cli.udp_port,
        http_port: cli.http_port,
        announce: false,
    };

    let discovery = discovery::DiscoveryService::new(state.clone(), local_info).await?;
    let discovery = Arc::new(discovery);

    // Start discovery tasks
    let d = Arc::clone(&discovery);
    tokio::spawn(async move { d.listen().await });
    let d = Arc::clone(&discovery);
    tokio::spawn(async move { d.periodic_announce().await });
    let d = Arc::clone(&discovery);
    tokio::spawn(async move { d.prune_stale_peers().await });

    // Start TCP transfer server
    let tcp_server = transfer::TransferServer::new(cli.tcp_port, state.clone()).await?;

    // Start web UI
    let _web_ui = server::start_web_ui(
        state.clone(),
        cli.http_port,
    );

    // Run all services concurrently
    tokio::select! {
        result = tcp_server.run() => {
            tracing::info!("TCP server stopped");
        }
        result = _web_ui => {
            if let Err(e) = result {
                tracing::error!("Web UI error: {}", e);
            }
        }
    }

    Ok(())
}

fn generate_fingerprint(alias: &str, tcp_port: u16, udp_port: u16) -> String {
    use sha2::{Sha256, Digest};
    let data = format!("{}:{}:{}", alias, tcp_port, udp_port);
    let hash = Sha256::digest(data);
    hex::encode(hash)
}
