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

mod tests {
    use std::{
        convert::TryFrom,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Once,
        time::Duration,
    };

    use monad_wireauth::{messages::Packet, Config, TestContext, API};
    use proptest::prelude::*;
    use secp256k1::rand::rng;
    use tracing_subscriber::EnvFilter;
    use zerocopy::IntoBytes;

    static INIT: Once = Once::new();

    const TIME_ADVANCE_MILLIS: u64 = 1;
    const REKEY_INTERVAL_SECS: u64 = 10;
    const SESSION_TIMEOUT_SECS: u64 = 20;

    fn init_logging() {
        INIT.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::from_default_env())
                .try_init();
        });
    }

    fn test_config() -> Config {
        Config {
            rekey_interval: Duration::from_secs(REKEY_INTERVAL_SECS),
            session_timeout: Duration::from_secs(SESSION_TIMEOUT_SECS),
            ..Config::default()
        }
    }

    struct PeerState {
        public_key: monad_secp::PubKey,
        private_key_bytes: [u8; 32],
        manager: API<TestContext>,
        context: TestContext,
        addr: SocketAddr,
        sent_data: Vec<Vec<u8>>,
        received_data: Vec<Vec<u8>>,
    }

    impl PeerState {
        fn new(peer_id: u8) -> Self {
            let mut rng = rng();
            let keypair = monad_secp::KeyPair::generate(&mut rng);
            let public_key = keypair.pubkey();
            let private_key_bytes = keypair.secret_bytes();
            let context = TestContext::new();
            let config = test_config();
            let manager = API::new(config, keypair, context.clone());
            let addr = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(10, 0, 0, peer_id)),
                30000 + peer_id as u16,
            );

            Self {
                public_key,
                private_key_bytes,
                manager,
                context,
                addr,
                sent_data: Vec::new(),
                received_data: Vec::new(),
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq)]
    enum ConnectionState {
        Connected,
        Disconnected,
    }

    struct TwoPeerModel {
        peers: [PeerState; 2],
        initiated: bool,
        expect_success: bool,
        connection_state: ConnectionState,
        connected_for: Duration,
    }

    impl TwoPeerModel {
        fn new() -> Self {
            Self {
                peers: [PeerState::new(1), PeerState::new(2)],
                initiated: false,
                expect_success: false,
                connection_state: ConnectionState::Connected,
                connected_for: Duration::ZERO,
            }
        }

        fn should_deliver_message(&self) -> bool {
            match self.connection_state {
                ConnectionState::Connected => true,
                ConnectionState::Disconnected => false,
            }
        }

        fn process_all_messages(&mut self) {
            loop {
                let mut had_packets = false;
                let mut packets_to_process = Vec::new();

                for i in 0..2 {
                    while let Some((_dst, packet)) = self.peers[i].manager.next_packet() {
                        had_packets = true;
                        let other = i ^ 1;
                        packets_to_process.push((i, other, self.peers[i].addr, packet.to_vec()));
                    }
                }

                if !had_packets {
                    break;
                }

                for (_sender_idx, receiver_idx, src_addr, mut packet) in packets_to_process {
                    if !self.should_deliver_message() {
                        continue;
                    }

                    if let Ok(parsed_packet) = Packet::try_from(&mut packet[..]) {
                        match parsed_packet {
                            Packet::Control(control) => {
                                let _ = self.peers[receiver_idx]
                                    .manager
                                    .dispatch_control(control, src_addr);
                            }
                            Packet::Data(data_packet) => {
                                if let Ok(plaintext) = self.peers[receiver_idx]
                                    .manager
                                    .decrypt(data_packet, src_addr)
                                {
                                    self.peers[receiver_idx]
                                        .received_data
                                        .push(plaintext.0.as_slice().to_vec());
                                }
                            }
                        }
                    }
                }
            }
        }

        fn apply_action(&mut self, action: Action) {
            match action {
                Action::Disconnect => {
                    self.connection_state = ConnectionState::Disconnected;
                    self.connected_for = Duration::ZERO;
                    self.initiated = false;
                    self.expect_success = false;
                }

                Action::Connect => {
                    if self.connection_state == ConnectionState::Disconnected {
                        self.connection_state = ConnectionState::Connected;
                        self.connected_for = Duration::ZERO;
                    }
                }

                Action::Initiate { from } => {
                    let time_advance = Duration::from_millis(TIME_ADVANCE_MILLIS);
                    self.peers[0].context.advance_time(time_advance);
                    self.peers[1].context.advance_time(time_advance);

                    let initiator_idx = (from - 1) as usize;
                    let responder_idx = initiator_idx ^ 1;

                    let responder_pubkey = self.peers[responder_idx].public_key.clone();
                    let responder_addr = self.peers[responder_idx].addr;

                    let _ = self.peers[initiator_idx].manager.connect(
                        responder_pubkey,
                        responder_addr,
                        monad_wireauth::RETRY_ALWAYS,
                    );

                    self.process_all_messages();

                    if self.connection_state == ConnectionState::Connected {
                        self.expect_success = true;
                    }
                    self.initiated = true
                }

                Action::Tick { seconds } => {
                    let duration = Duration::from_secs(seconds as u64);

                    for i in 0..2 {
                        self.peers[i].context.advance_time(duration);
                        self.peers[i].manager.tick();
                    }

                    self.process_all_messages();

                    if self.connection_state == ConnectionState::Connected && self.initiated {
                        self.connected_for = self.connected_for.saturating_add(duration);
                        let rekey_interval = Duration::from_secs(REKEY_INTERVAL_SECS);
                        let session_timeout = Duration::from_secs(SESSION_TIMEOUT_SECS);

                        if self.connected_for >= rekey_interval
                            && self.connected_for >= session_timeout
                        {
                            self.expect_success = true;
                        }
                    }
                }

                Action::Send { from, data } => {
                    let data_bytes = vec![from; data as usize];
                    let expect_success = self.expect_success;
                    let should_deliver = self.should_deliver_message();

                    let sender_idx = (from - 1) as usize;
                    let receiver_idx = sender_idx ^ 1;

                    let receiver_pubkey = self.peers[receiver_idx].public_key.clone();
                    let sender_addr = self.peers[sender_idx].addr;

                    self.peers[sender_idx].sent_data.push(data_bytes.clone());
                    let mut plaintext = data_bytes.clone();
                    let send_result = self.peers[sender_idx]
                        .manager
                        .encrypt_by_public_key(&receiver_pubkey, &mut plaintext);

                    if expect_success {
                        let header = send_result.unwrap_or_else(|e| {
                            panic!("after initiation and while connected, send should succeed, error={e:?}")
                        });

                        let mut packet = header.as_bytes().to_vec();
                        packet.extend_from_slice(&plaintext);

                        if should_deliver {
                            let parsed_packet =
                                Packet::try_from(&mut packet[..]).expect("packet should parse");

                            let plaintext = match parsed_packet {
                                Packet::Control(_) => {
                                    panic!("expected data packet, got control packet")
                                }
                                Packet::Data(data_packet) => self.peers[receiver_idx]
                                    .manager
                                    .decrypt(data_packet, sender_addr)
                                    .expect("decrypt should succeed"),
                            };

                            let received = plaintext.0.as_slice().to_vec();
                            assert_eq!(
                                data_bytes, received,
                                "dispatch should return decrypted data matching sent data"
                            );
                            self.peers[receiver_idx].received_data.push(received);
                        }
                    }

                    self.process_all_messages();
                }

                Action::Reset { peer } => {
                    let peer_idx = (peer - 1) as usize;
                    let context = self.peers[peer_idx].context.clone();
                    let config = test_config();
                    let mut key_bytes = self.peers[peer_idx].private_key_bytes;
                    let keypair = monad_secp::KeyPair::from_bytes(&mut key_bytes).unwrap();

                    self.peers[peer_idx].manager = API::new(config, keypair, context.clone());
                    self.peers[peer_idx].context = context;

                    self.connected_for = Duration::ZERO;
                    self.initiated = false;
                    self.expect_success = false;
                }

                Action::Migrate { peer, new_addr } => {
                    let peer_idx = (peer - 1) as usize;
                    self.peers[peer_idx].addr = new_addr;

                    let context = self.peers[peer_idx].context.clone();
                    let config = test_config();
                    let mut key_bytes = self.peers[peer_idx].private_key_bytes;
                    let keypair = monad_secp::KeyPair::from_bytes(&mut key_bytes).unwrap();

                    self.peers[peer_idx].manager = API::new(config, keypair, context.clone());
                    self.peers[peer_idx].context = context;

                    self.connected_for = Duration::ZERO;
                    self.initiated = false;
                    self.expect_success = false;
                }
            }
        }
    }

    #[derive(Debug, Clone)]
    enum Action {
        Initiate { from: u8 },
        Tick { seconds: u8 },
        Send { from: u8, data: u8 },
        Reset { peer: u8 },
        Migrate { peer: u8, new_addr: SocketAddr },
        Disconnect,
        Connect,
    }

    fn basic_action_strategy() -> impl Strategy<Value = Action> {
        prop_oneof![
            (1..=2u8).prop_map(|from| Action::Initiate { from }),
            (1..=3u8).prop_map(|seconds| Action::Tick { seconds }),
            (1..=2u8, 1..=100u8).prop_map(|(from, data)| Action::Send { from, data }),
        ]
    }

    fn reset_action_strategy() -> impl Strategy<Value = Action> {
        prop_oneof![
            (1..=2u8).prop_map(|from| Action::Initiate { from }),
            (1..=3u8).prop_map(|seconds| Action::Tick { seconds }),
            (1..=2u8, 1..=100u8).prop_map(|(from, data)| Action::Send { from, data }),
            (1..=2u8).prop_map(|peer| Action::Reset { peer }),
        ]
    }

    fn migration_action_strategy() -> impl Strategy<Value = Action> {
        prop_oneof![
            (1..=2u8).prop_map(|from| Action::Initiate { from }),
            (1..=3u8).prop_map(|seconds| Action::Tick { seconds }),
            (1..=2u8, 1..=100u8).prop_map(|(from, data)| Action::Send { from, data }),
            (1..=2u8, 0..=255u8, 0..=255u8).prop_map(|(peer, octet3, octet4)| Action::Migrate {
                peer,
                new_addr: SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(10, 0, octet3, octet4)),
                    30000 + peer as u16,
                )
            }),
        ]
    }

    fn message_loss_action_strategy() -> impl Strategy<Value = Action> {
        prop_oneof![
            (1..=2u8).prop_map(|from| Action::Initiate { from }),
            (1..=3u8).prop_map(|seconds| Action::Tick { seconds }),
            (1..=2u8, 1..=100u8).prop_map(|(from, data)| Action::Send { from, data }),
            Just(Action::Disconnect),
            Just(Action::Connect),
        ]
    }

    fn all_actions_strategy() -> impl Strategy<Value = Action> {
        prop_oneof![
            (1..=2u8).prop_map(|from| Action::Initiate { from }),
            (1..=3u8).prop_map(|seconds| Action::Tick { seconds }),
            (1..=2u8, 1..=100u8).prop_map(|(from, data)| Action::Send { from, data }),
            (1..=2u8).prop_map(|peer| Action::Reset { peer }),
            (1..=2u8, 0..=255u8, 0..=255u8).prop_map(|(peer, octet3, octet4)| Action::Migrate {
                peer,
                new_addr: SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(10, 0, octet3, octet4)),
                    30000 + peer as u16,
                )
            }),
            Just(Action::Disconnect),
            Just(Action::Connect),
        ]
    }

    proptest! {
        #[test]
        fn test_two_peer_model(actions in prop::collection::vec(basic_action_strategy(), 1..50)) {
            init_logging();
            let mut model = TwoPeerModel::new();

            for action in actions {
                model.apply_action(action);
            }
        }

        #[test]
        fn test_two_peer_model_with_memory_reset(actions in prop::collection::vec(reset_action_strategy(), 1..50)) {
            init_logging();
            let mut model = TwoPeerModel::new();

            for action in actions {
                model.apply_action(action);
            }
        }

        #[test]
        fn test_two_peer_model_with_ip_migration(actions in prop::collection::vec(migration_action_strategy(), 1..50)) {
            init_logging();
            let mut model = TwoPeerModel::new();

            for action in actions {
                model.apply_action(action);
            }
        }

        #[test]
        fn test_two_peer_model_with_message_loss(actions in prop::collection::vec(message_loss_action_strategy(), 1..100)) {
            init_logging();
            let mut model = TwoPeerModel::new();

            for action in actions {
                model.apply_action(action);
            }
        }

        #[test]
        fn test_two_peer_model_combined_all_failure_scenarios(actions in prop::collection::vec(all_actions_strategy(), 1..200)) {
            init_logging();
            let mut model = TwoPeerModel::new();

            for action in actions {
                model.apply_action(action);
            }
        }
    }
}
