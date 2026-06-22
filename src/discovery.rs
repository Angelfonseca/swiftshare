// UDP discovery service for peer-to-peer device discovery

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::RwLock;

use crate::state::AppState;

const BROADCAST_PORT: u16 = 45679;
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
        let socket = Self::create_socket().await?;
        Ok(Self {
            socket,
            state,
            local_info,
        })
    }

    async fn create_socket() -> anyhow::Result<Arc<UdpSocket>> {
        let socket = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        )
        .context("Failed to create UDP socket")?;

        socket.set_broadcast(true).context("Failed to set broadcast")?;
        socket.set_reuse_address(true).context("Failed to set reuse address")?;

        use std::net::SocketAddr;
        let addr: SocketAddr = format!("0.0.0.0:{}", BROADCAST_PORT).parse().unwrap();
        socket
            .bind(&addr.into())
            .context("Failed to bind UDP socket")?;

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

        // Method 1: Broadcast to 255.255.255.255
        let broadcast_addr: std::net::SocketAddr = format!("255.255.255.255:{}", BROADCAST_PORT)
            .parse()
            .context("Failed to parse broadcast address")?;
        let _ = self.socket.send_to(&data, broadcast_addr).await;

        // Method 2: Multicast
        let multicast_addr: std::net::SocketAddr =
            format!("{}:{}", MULTICAST_ADDR, BROADCAST_PORT).parse().context(
                "Failed to parse multicast address",
            )?;
        let _ = self.socket.send_to(&data, multicast_addr).await;

        // Method 3: Broadcast to local subnet (192.168.x.x)
        let broadcast_subnet: std::net::SocketAddr = format!("192.168.255.255:{}", BROADCAST_PORT)
            .parse()
            .context("Failed to parse subnet broadcast address")?;
        let _ = self.socket.send_to(&data, broadcast_subnet).await;

        Ok(())
    }

    pub async fn discover_single(&self, ip: &str) -> anyhow::Result<()> {
        let msg = self.local_info.clone();
        let data = serde_json::to_vec(&msg)?;

        // Try direct UDP to specific IP
        let addr = format!("{}:{}", ip, BROADCAST_PORT);
        let socket_addr: std::net::SocketAddr = addr
            .parse()
            .context(format!("Invalid address: {}", addr))?;
        self.socket.send_to(&data, socket_addr).await?;

        tracing::info!("Discovery probe sent to {}", ip);
        Ok(())
    }

    pub async fn listen(self: Arc<Self>) {
        let mut buf = [0u8; 4096];

        loop {
            match self.socket.recv_from(&mut buf).await {
                Ok((len, from_addr)) => {
                    let data = &buf[..len];

                    if let Ok(msg) = serde_json::from_slice::<DiscoveryMessage>(data) {
                        if msg.fingerprint == self.local_info.fingerprint {
                            continue;
                        }

                        tracing::info!(
                            "Discovered peer: {} from {}",
                            msg.alias,
                            from_addr
                        );

                        let peer_info = crate::state::PeerInfo {
                            alias: msg.alias.clone(),
                            fingerprint: msg.fingerprint.clone(),
                            ip: from_addr.ip().to_string(),
                            tcp_port: msg.tcp_port,
                            udp_port: msg.udp_port,
                            last_seen: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                        };

                        self.state.add_peer(peer_info).await;
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
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let mut peers = self.state.peers.write().await;
            peers.retain(|_, p| now - p.last_seen < STALE_TIMEOUT);
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
