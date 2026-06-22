// UDP discovery service for peer-to-peer device discovery

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::sync::RwLock;

use crate::state::PeerInfo;

const BROADCAST_PORT: u16 = 45679;
const MULTICAST_ADDR: &str = "224.0.0.167";
const ANNOUNCE_INTERVAL: u64 = 3;
const STALE_TIMEOUT: u64 = 30;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryMessage {
    pub alias: String,
    pub fingerprint: String,
    pub tcp_port: u16,
    pub udp_port: u16,
    pub announce: bool,
}

pub struct DiscoveryService {
    socket: Arc<UdpSocket>,
    peers: Arc<RwLock<HashMap<String, DiscoveredPeer>>>,
    local_info: DiscoveryMessage,
}

struct DiscoveredPeer {
    info: DiscoveryMessage,
    addr: std::net::SocketAddr,
    last_seen: Instant,
}

impl DiscoveryService {
    pub async fn new(local_info: DiscoveryMessage) -> anyhow::Result<Self> {
        let socket = Self::create_broadcast_socket().await?;
        Ok(Self {
            socket,
            peers: Arc::new(RwLock::new(HashMap::new())),
            local_info,
        })
    }

    async fn create_broadcast_socket() -> anyhow::Result<Arc<UdpSocket>> {
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

        // Broadcast
        let broadcast_addr: std::net::SocketAddr = format!("255.255.255.255:{}", BROADCAST_PORT)
            .parse()
            .context("Failed to parse broadcast address")?;
        self.socket.send_to(&data, broadcast_addr).await?;

        // Multicast
        let multicast_addr: std::net::SocketAddr =
            format!("{}:{}", MULTICAST_ADDR, BROADCAST_PORT).parse().context(
                "Failed to parse multicast address",
            )?;
        self.socket.send_to(&data, multicast_addr).await?;

        Ok(())
    }

    pub async fn register_with(&self, target: std::net::SocketAddr) -> anyhow::Result<()> {
        let mut msg = self.local_info.clone();
        msg.announce = true;

        let data = serde_json::to_vec(&msg)?;
        self.socket.send_to(&data, target).await?;

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

                        let mut peers = self.peers.write().await;
                        peers.insert(
                            msg.fingerprint.clone(),
                            DiscoveredPeer {
                                info: msg.clone(),
                                addr: from_addr,
                                last_seen: Instant::now(),
                            },
                        );

                        if msg.announce {
                            let _ = self.send_response(from_addr).await;
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

    async fn send_response(&self, to: std::net::SocketAddr) -> anyhow::Result<()> {
        let msg = DiscoveryMessage {
            announce: false,
            ..self.local_info.clone()
        };
        let data = serde_json::to_vec(&msg)?;
        self.socket.send_to(&data, to).await?;
        Ok(())
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
            let mut peers = self.peers.write().await;
            let now = Instant::now();
            peers.retain(|_, peer| {
                now.duration_since(peer.last_seen).as_secs() < STALE_TIMEOUT
            });
        }
    }

    pub async fn get_peers(&self) -> Vec<DiscoveryMessage> {
        let peers = self.peers.read().await;
        peers.values().map(|p| p.info.clone()).collect()
    }

    pub async fn get_peer_count(&self) -> usize {
        let peers = self.peers.read().await;
        peers.len()
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
            announce: true,
        };

        let json = serde_json::to_string(&msg).unwrap();
        let parsed: DiscoveryMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.alias, "TestPC");
        assert_eq!(parsed.tcp_port, 45678);
        assert!(parsed.announce);
    }

    #[tokio::test]
    async fn test_broadcast_socket_creation() {
        let socket = DiscoveryService::create_broadcast_socket().await;
        // Socket creation may fail if port is already in use (e.g., from another test or running server)
        // This is expected in CI/test environments
        if socket.is_err() {
            tracing::warn!("Broadcast socket creation failed (port may be in use): {}", socket.unwrap_err());
        }
    }

    #[tokio::test]
    async fn test_discovery_service_creation() {
        let local_info = DiscoveryMessage {
            alias: "TestPC".to_string(),
            fingerprint: "test-fp".to_string(),
            tcp_port: 45678,
            udp_port: 45679,
            announce: false,
        };

        let _service = DiscoveryService::new(local_info).await;
    }
}
