// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    convert::TryFrom,
    future::pending,
    net::SocketAddr,
    rc::Rc,
    time::{Duration, Instant},
};

use clap::Parser;
use monad_wireauth::{
    messages::Packet, metrics::DEFAULT_METRICS, Config, PublicKey, StdContext, API, RETRY_ALWAYS,
};
use monoio::{net::udp::UdpSocket, time::sleep_until};
use secp256k1::rand::{rngs::StdRng, SeedableRng};
use tracing::{debug, info, warn};
use zerocopy::IntoBytes;

#[derive(Parser, Debug)]
#[command(version, about = "monoio-based authenticated protocol demo")]
struct Args {
    #[arg(short, long, help = "listener address")]
    listener: SocketAddr,

    #[arg(short, long, value_delimiter = ',', help = "peer addresses")]
    peers: Vec<SocketAddr>,
}

struct PeerNode {
    manager: API<StdContext>,
    socket: Rc<UdpSocket>,
    id: u64,
}

impl PeerNode {
    fn new(addr: SocketAddr, seed: u64, id: u64) -> std::io::Result<Self> {
        let mut rng = StdRng::seed_from_u64(seed);
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let _public_key = keypair.pubkey();

        info!(id = id, addr = %addr, "initializing node");

        let config = Config {
            session_timeout: Duration::from_secs(10),
            session_timeout_jitter: Duration::ZERO,
            keepalive_interval: Duration::from_secs(3),
            keepalive_jitter: Duration::ZERO,
            rekey_interval: Duration::from_secs(60),
            rekey_jitter: Duration::ZERO,
            ..Default::default()
        };

        let context = StdContext::new();
        let manager = API::new(DEFAULT_METRICS, config, keypair, context);
        let socket = UdpSocket::bind(addr)?;

        Ok(Self {
            manager,
            socket: Rc::new(socket),
            id,
        })
    }

    fn connect(&mut self, peer_id: u64, peer_public: PublicKey, peer_addr: SocketAddr) {
        info!(
            id = self.id,
            peer_id = peer_id,
            peer_addr = %peer_addr,
            "connecting to peer"
        );
        self.manager
            .connect(peer_public, peer_addr, RETRY_ALWAYS)
            .unwrap();
    }

    async fn send_all_packets(&mut self) -> std::io::Result<()> {
        while let Some((addr, packet)) = self.manager.next_packet() {
            let packet_vec = packet.to_vec();
            let (result, _) = self.socket.send_to(packet_vec, addr).await;
            result?;
        }
        Ok(())
    }

    fn encrypt_by_public_key(
        &mut self,
        peer_public: &PublicKey,
        plaintext: &mut [u8],
    ) -> monad_wireauth::Result<monad_wireauth::messages::DataPacketHeader> {
        self.manager.encrypt_by_public_key(peer_public, plaintext)
    }

    fn next_deadline(&mut self) -> Option<Instant> {
        self.manager.next_deadline()
    }
}

#[monoio::main(timer_enabled = true)]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    if args.peers.is_empty() {
        warn!("no peers specified");
    }

    let id = args.listener.port() as u64;
    let mut node = PeerNode::new(args.listener, id, id)?;

    let mut peer_keys: Vec<(u64, monad_secp::PubKey, SocketAddr)> = Vec::new();
    for &peer_addr in &args.peers {
        let peer_id = peer_addr.port() as u64;
        let seed = peer_id;
        let mut rng = StdRng::seed_from_u64(seed);
        let peer_keypair = monad_secp::KeyPair::generate(&mut rng);
        let peer_public = peer_keypair.pubkey();
        peer_keys.push((peer_id, peer_public, peer_addr));
    }

    for &(peer_id, peer_public, peer_addr) in &peer_keys {
        node.connect(peer_id, peer_public, peer_addr);
    }

    let mut tick_interval = monoio::time::interval(Duration::from_secs(1));

    loop {
        let socket = node.socket.clone();
        let buf = vec![0u8; 65536];

        monoio::select! {
            recv_result = socket.recv_from(buf) => {
                let (result, mut buf) = recv_result;
                if let Ok((len, src)) = result {
                    match Packet::try_from(&mut buf[..len]) {
                        Ok(Packet::Control(control)) => {
                            let _ = node.manager.dispatch_control(control, src);
                        }
                        Ok(Packet::Data(data_packet)) => {
                            if let Ok((plaintext, _peer_key)) = node.manager.decrypt(data_packet, src) {
                                if let Ok(msg) = std::str::from_utf8(plaintext.as_ref()) {
                                    info!(id = node.id, src = %src, message = %msg, "received message");
                                }
                            }
                        }
                        Err(_) => {}
                    }
                }
            },
            _ = async {
                match node.next_deadline() {
                    Some(deadline) => {
                        debug!(?deadline, "next deadline");
                        sleep_until(deadline.into()).await;
                        node.manager.tick();
                    },
                    None => pending().await,
                }
            } => {},
            _ = tick_interval.tick() => {
                for &(peer_id, peer_public, peer_addr) in &peer_keys {
                    let message = format!("hello from {} to {}", node.id, peer_id);
                    let mut plaintext = message.as_bytes().to_vec();

                    match node.encrypt_by_public_key(&peer_public, &mut plaintext) {
                        Ok(header) => {
                            let mut packet = Vec::new();
                            packet.extend_from_slice(header.as_bytes());
                            packet.extend_from_slice(&plaintext);

                            let (result, _) = node.socket.send_to(packet, peer_addr).await;
                            match result {
                                Ok(_) => {
                                    info!(from = node.id, to = peer_id, message = %message, "sent message");
                                }
                                Err(e) => {
                                    warn!(from = node.id, to = peer_id, error = %e, "failed to send message");
                                }
                            }
                        }
                        Err(e) => {
                            warn!(from = node.id, to = peer_id, error = %e, "failed to encrypt message");
                        }
                    }
                }
            },
        }

        node.send_all_packets().await?;
    }
}
