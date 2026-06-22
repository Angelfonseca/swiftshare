// CLI argument parsing

use clap::Parser;
use rand::Rng;

#[derive(Parser, Debug)]
#[command(name = "swiftshare")]
#[command(about = "Fast P2P file transfer tool for local networks")]
#[command(version)]
pub struct Cli {
    /// Alias for this device (random if not provided)
    #[arg(short, long, default_value = "")]
    pub alias: String,

    /// Download directory for received files
    #[arg(short, long, default_value = "")]
    pub download_dir: String,

    /// TCP port for file transfers
    #[arg(long, default_value_t = 45678)]
    pub tcp_port: u16,

    /// UDP port for discovery
    #[arg(long, default_value_t = 45679)]
    pub udp_port: u16,

    /// HTTP port for web UI
    #[arg(long, default_value_t = 8080)]
    pub http_port: u16,
}

impl Cli {
    pub fn resolve_alias(&self) -> String {
        if !self.alias.is_empty() {
            self.alias.clone()
        } else {
            generate_random_alias()
        }
    }

    pub fn resolve_download_dir(&self) -> std::path::PathBuf {
        if !self.download_dir.is_empty() {
            std::path::PathBuf::from(&self.download_dir)
        } else {
            dirs::download_dir().unwrap_or_else(|| std::path::PathBuf::from("."))
        }
    }
}

fn generate_random_alias() -> String {
    let adjectives = ["Swift", "Happy", "Brave", "Calm", "Dark", "Eager", "Clever", "Bold"];
    let nouns = ["Fox", "Bear", "Hawk", "Wolf", "Deer", "Lynx", "Eagle", "Otter"];

    let mut rng = rand::rng();
    let adj_idx: usize = rng.random_range(0..adjectives.len());
    let noun_idx: usize = rng.random_range(0..nouns.len());
    let num: u16 = rng.random_range(0..1000);

    format!("{} {} {}", adjectives[adj_idx], nouns[noun_idx], num)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_alias_not_empty() {
        let alias = generate_random_alias();
        assert!(!alias.is_empty());
        assert!(alias.len() > 3);
    }

    #[test]
    fn test_cli_parse() {
        let cli = Cli::try_parse_from(["swiftshare", "--alias", "MyPC"]).unwrap();
        assert_eq!(cli.alias, "MyPC");
        assert_eq!(cli.tcp_port, 45678);
        assert_eq!(cli.http_port, 8080);
    }

    #[test]
    fn test_resolve_download_dir_default() {
        let cli = Cli::try_parse_from(["swiftshare"]).unwrap();
        let dir = cli.resolve_download_dir();
        assert!(dir.is_absolute() || dir.to_string_lossy().starts_with("."));
    }
}
