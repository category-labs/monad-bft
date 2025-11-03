use std::{convert::TryFrom, rc::Rc, time::Duration};

use bytes::Bytes;
use monad_wireauth_api::{Config, StdContext, API};
use monad_wireauth_protocol::messages::Packet;
use monad_wireauth_session::DEFAULT_RETRY_ATTEMPTS;
use monoio::net::udp::UdpSocket;
use secp256k1::rand::{rngs::StdRng, SeedableRng};
use zerocopy::IntoBytes;

struct PeerNode {
    manager: API<StdContext>,
    socket: Rc<UdpSocket>,
    public_key: monad_secp::PubKey,
}

impl PeerNode {
    fn new(port: u16, seed: u64) -> std::io::Result<Self> {
        let mut rng = StdRng::seed_from_u64(seed);
        let keypair = monad_secp::KeyPair::generate(&mut rng);
        let public_key = keypair.pubkey();

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
        let manager = API::new(config, keypair, context);

        let addr = format!("127.0.0.1:{}", port);
        let socket = UdpSocket::bind(addr)?;

        Ok(Self {
            manager,
            socket: Rc::new(socket),
            public_key,
        })
    }

    fn connect(&mut self, peer_public: monad_secp::PubKey, peer_addr: std::net::SocketAddr) {
        self.manager
            .connect(peer_public, peer_addr, DEFAULT_RETRY_ATTEMPTS)
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

    async fn recv_and_dispatch(&mut self) -> std::io::Result<Option<Bytes>> {
        let buf = vec![0u8; 65536];
        let (result, mut buf) = self.socket.recv_from(buf).await;
        let (len, src) = result?;

        let parsed_packet = match Packet::try_from(&mut buf[..len]) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };

        match parsed_packet {
            Packet::Control(control) => {
                let _ = self.manager.dispatch_control(control, src);
                Ok(None)
            }
            Packet::Data(data_packet) => match self.manager.decrypt(data_packet, src) {
                Ok((plaintext, _public_key)) => Ok(Some(plaintext.as_slice().to_vec().into())),
                Err(_) => Ok(None),
            },
        }
    }

    fn encrypt_by_public_key(
        &mut self,
        peer_public: &monad_secp::PubKey,
        plaintext: &mut [u8],
    ) -> monad_wireauth_api::Result<monad_wireauth_protocol::messages::DataPacketHeader> {
        self.manager.encrypt_by_public_key(peer_public, plaintext)
    }
}

async fn exchange_handshake(alice: &mut PeerNode, bob: &mut PeerNode) -> std::io::Result<()> {
    let mut iterations = 0;
    let max_iterations = 10;

    while iterations < max_iterations {
        alice.send_all_packets().await?;
        bob.send_all_packets().await?;

        let alice_socket = alice.socket.clone();
        let bob_socket = bob.socket.clone();

        let alice_task = async {
            let buf = vec![0u8; 65536];
            monoio::select! {
                recv_result = alice_socket.recv_from(buf) => {
                    let (result, mut buf) = recv_result;
                    if let Ok((len, src)) = result {
                        if let Ok(parsed_packet) = Packet::try_from(&mut buf[..len]) {
                            match parsed_packet {
                                Packet::Control(control) => {
                                    let _ = alice.manager.dispatch_control(control, src);
                                }
                                Packet::Data(data_packet) => {
                                    let _ = alice.manager.decrypt(data_packet, src);
                                }
                            }
                        }
                        true
                    } else {
                        false
                    }
                },
                _ = monoio::time::sleep(Duration::from_millis(10)) => {
                    false
                },
            }
        };

        let bob_task = async {
            let buf = vec![0u8; 65536];
            monoio::select! {
                recv_result = bob_socket.recv_from(buf) => {
                    let (result, mut buf) = recv_result;
                    if let Ok((len, src)) = result {
                        if let Ok(parsed_packet) = Packet::try_from(&mut buf[..len]) {
                            match parsed_packet {
                                Packet::Control(control) => {
                                    let _ = bob.manager.dispatch_control(control, src);
                                }
                                Packet::Data(data_packet) => {
                                    let _ = bob.manager.decrypt(data_packet, src);
                                }
                            }
                        }
                        true
                    } else {
                        false
                    }
                },
                _ = monoio::time::sleep(Duration::from_millis(10)) => {
                    false
                },
            }
        };

        let (alice_received, bob_received) = monoio::join!(alice_task, bob_task);

        if !alice_received && !bob_received {
            break;
        }

        iterations += 1;
    }

    Ok(())
}

#[monoio::test(timer_enabled = true)]
async fn test_e2e_handshake_and_data() {
    let mut alice = PeerNode::new(28001, 1).unwrap();
    let mut bob = PeerNode::new(28002, 2).unwrap();

    let bob_addr = bob.socket.local_addr().unwrap();
    alice.connect(bob.public_key.clone(), bob_addr);

    exchange_handshake(&mut alice, &mut bob).await.unwrap();

    let mut plaintext = b"hello from alice".to_vec();
    let header = alice
        .encrypt_by_public_key(&bob.public_key, &mut plaintext)
        .expect("alice encrypt failed");

    let mut packet = Vec::new();
    packet.extend_from_slice(header.as_bytes());
    packet.extend_from_slice(&plaintext);

    let (result, _) = alice.socket.send_to(packet, bob_addr).await;
    result.unwrap();

    let received = bob.recv_and_dispatch().await.unwrap();
    assert_eq!(received, Some(Bytes::from(&b"hello from alice"[..])));
}

#[monoio::test(timer_enabled = true)]
async fn test_e2e_bidirectional() {
    let mut alice = PeerNode::new(28003, 3).unwrap();
    let mut bob = PeerNode::new(28004, 4).unwrap();

    let bob_addr = bob.socket.local_addr().unwrap();
    let alice_addr = alice.socket.local_addr().unwrap();

    alice.connect(bob.public_key.clone(), bob_addr);

    exchange_handshake(&mut alice, &mut bob).await.unwrap();

    let mut msg_alice = b"message from alice".to_vec();
    let header_alice = alice
        .encrypt_by_public_key(&bob.public_key, &mut msg_alice)
        .unwrap();

    let mut packet_alice = Vec::new();
    packet_alice.extend_from_slice(header_alice.as_bytes());
    packet_alice.extend_from_slice(&msg_alice);

    let (result, _) = alice.socket.send_to(packet_alice, bob_addr).await;
    result.unwrap();

    let received_bob = bob.recv_and_dispatch().await.unwrap();
    assert_eq!(received_bob, Some(Bytes::from(&b"message from alice"[..])));

    let mut msg_bob = b"message from bob".to_vec();
    let header_bob = bob
        .encrypt_by_public_key(&alice.public_key, &mut msg_bob)
        .unwrap();

    let mut packet_bob = Vec::new();
    packet_bob.extend_from_slice(header_bob.as_bytes());
    packet_bob.extend_from_slice(&msg_bob);

    let (result, _) = bob.socket.send_to(packet_bob, alice_addr).await;
    result.unwrap();

    let received_alice = alice.recv_and_dispatch().await.unwrap();
    assert_eq!(received_alice, Some(Bytes::from(&b"message from bob"[..])));
}
