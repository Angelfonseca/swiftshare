// CLI entry point — thin wrapper around the swiftshare library.

use clap::Parser;
use swiftshare::cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = cli::Cli::parse();
    let open_browser = cli.open;

    let app = swiftshare::start(&cli).await?;

    if open_browser {
        let _ = open::that_detached(format!("http://{}", app.http_addr));
    }

    tokio::signal::ctrl_c().await?;
    tracing::info!("Cerrando swiftshare");
    Ok(())
}
