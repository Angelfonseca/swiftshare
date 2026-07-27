// UDP discovery service for peer-to-peer device discovery

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;

use crate::state::AppState;

const MULTICAST_ADDR: &str = "224.0.0.167";
const ANNOUNCE_INTERVAL: u64 = 2;
const STALE_TIMEOUT: u64 = 10;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryMessage {
    pub alias: String,
    pub fingerprint: String,
    pub tcp_port: u16,
    pub udp_port: u16,
    pub http_port: u16,
    pub announce: bool,
}

pub struct DiscoveryService {
    socket: Arc<UdpSocket>,
    state: Arc<AppState>,
    local_info: DiscoveryMessage,
}

impl DiscoveryService {
    pub async fn new(state: Arc<AppState>, local_info: DiscoveryMessage) -> anyhow::Result<Self> {
        // Bind the port we actually announce, not a hardcoded one, or
        // --udp-port silently does nothing and two instances collide.
        let socket = Self::create_socket(local_info.udp_port).await?;
        Ok(Self {
            socket,
            state,
            local_info,
        })
    }

    async fn create_socket(port: u16) -> anyhow::Result<Arc<UdpSocket>> {
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )
        .context("Failed to create UDP socket")?;

        socket.set_broadcast(true).context("Failed to set broadcast")?;
        socket.set_reuse_address(true).context("Failed to set reuse address")?;
        // On macOS/BSD, SO_REUSEADDR alone doesn't let two processes share a
        // UDP port — needed so two swiftshare instances on the same machine
        // (testing, or a dev box) can both hear the same broadcast.
        #[cfg(unix)]
        socket.set_reuse_port(true).context("Failed to set reuse port")?;

        use std::net::SocketAddr;
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
        socket
            .bind(&addr.into())
            .with_context(|| format!("Failed to bind UDP port {}", port))?;

        socket.set_nonblocking(true).context("Failed to set nonblocking")?;

        let std_socket: std::net::UdpSocket = socket.into();
        let tokio_socket = UdpSocket::from_std(std_socket)
            .context("Failed to convert to tokio UdpSocket")?;

        Ok(Arc::new(tokio_socket))
    }

    pub async fn announce(&self) -> anyhow::Result<()> {
        let mut msg = self.local_info.clone();
        msg.announce = true;

        let data = serde_json::to_vec(&msg)?;
        let port = self.local_info.udp_port;

        // Networks differ in what they let through, so try all three.
        for target in [
            format!("255.255.255.255:{}", port),
            format!("{}:{}", MULTICAST_ADDR, port),
            format!("192.168.255.255:{}", port),
        ] {
            if let Ok(addr) = target.parse::<std::net::SocketAddr>() {
                let _ = self.socket.send_to(&data, addr).await;
            }
        }

        Ok(())
    }

    pub fn socket(&self) -> Arc<UdpSocket> {
        Arc::clone(&self.socket)
    }

    pub async fn listen(self: Arc<Self>) {
        let mut buf = [0u8; 4096];

        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((len, from_addr)) => {
                    let Ok(msg) = serde_json::from_slice::<DiscoveryMessage>(&buf[..len]) else {
                        continue;
                    };
                    if msg.fingerprint == self.local_info.fingerprint {
                        continue;
                    }

                    let known = self
                        .state
                        .peers
                        .read()
                        .await
                        .contains_key(&msg.fingerprint);
                    if !known {
                        tracing::info!("Discovered peer: {} at {}", msg.alias, from_addr);
                    }

                    self.state
                        .add_peer(crate::state::PeerInfo {
                            alias: msg.alias.clone(),
                            fingerprint: msg.fingerprint.clone(),
                            ip: from_addr.ip().to_string(),
                            tcp_port: msg.tcp_port,
                            udp_port: msg.udp_port,
                            last_seen: crate::state::now_secs(),
                        })
                        .await;

                    // Answer announcements directly. Broadcast is filtered on
                    // plenty of networks, so this unicast reply is what makes
                    // "connect by IP" work in both directions.
                    if msg.announce {
                        let mut reply = self.local_info.clone();
                        reply.announce = false;
                        if let Ok(data) = serde_json::to_vec(&reply) {
                            let _ = self.socket.send_to(&data, from_addr).await;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("UDP recv error: {}", e);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    pub async fn periodic_announce(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(ANNOUNCE_INTERVAL)).await;
            if let Err(e) = self.announce().await {
                tracing::warn!("Announce failed: {}", e);
            }
        }
    }

    pub async fn prune_stale_peers(self: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(STALE_TIMEOUT)).await;
            let now = crate::state::now_secs();
            self.state
                .peers
                .write()
                .await
                .retain(|_, p| now.saturating_sub(p.last_seen) < STALE_TIMEOUT);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovery_message_serialization() {
        let msg = DiscoveryMessage {
            alias: "TestPC".to_string(),
            fingerprint: "abc123".to_string(),
            tcp_port: 45678,
            udp_port: 45679,
            http_port: 8080,
            announce: true,
        };

        let json = serde_json::to_string(&msg).unwrap();
        let parsed: DiscoveryMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.alias, "TestPC");
        assert_eq!(parsed.tcp_port, 45678);
        assert_eq!(parsed.http_port, 8080);
        assert!(parsed.announce);
    }
}
