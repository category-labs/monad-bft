use std::{
    collections::BTreeMap,
    net::{SocketAddr, SocketAddrV4},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::DataplaneBuilder;
use monad_executor::Executor;
use monad_executor_glue::{Message, RouterCommand};
use monad_peer_discovery::{
    discovery::{PeerDiscovery, PeerDiscoveryBuilder},
    driver::PeerDiscoveryDriver,
    MonadNameRecord, NameRecord,
};
use monad_raptorcast::{
    config::{
        RaptorCastConfig, RaptorCastConfigPrimary, RaptorCastConfigSecondary,
        SecondaryRaptorCastModeConfig,
    },
    RaptorCast, RaptorCastEvent,
};
use monad_secp::{KeyPair, SecpSignature};
use monad_types::{Deserializable, Epoch, NodeId, Round, RouterTarget, Serializable, Stake};
use rand_chacha::{rand_core::SeedableRng, ChaCha8Rng};
use serde::{Deserialize, Serialize};
use tracing_subscriber::fmt::format::FmtSpan;

type SignatureType = SecpSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;

fn parse_duration(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s).map_err(|e| e.to_string())
}

fn parse_size(s: &str) -> Result<usize, String> {
    use std::str::FromStr;

    use byte_unit::Byte;
    Byte::from_str(s)
        .map(|b| b.as_u64() as usize)
        .map_err(|e| e.to_string())
}

#[derive(Parser)]
#[command(name = "node")]
#[command(about = "Monad RaptorCast Node", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Run {
        #[arg(long)]
        cluster: String,
        #[arg(long)]
        index: usize,
    },
    Producer {
        #[arg(long)]
        cluster: String,
        #[arg(long)]
        index: usize,
        #[arg(long, value_parser = parse_duration)]
        interval: Duration,
        #[arg(long, value_parser = parse_size)]
        size: usize,
    },
    Generate {
        #[arg(long)]
        output: String,
        #[arg(long)]
        count: usize,
        #[arg(long, default_value = "127.0.0.1")]
        ip: String,
        #[arg(long, default_value = "30000")]
        tcp_port: u16,
        #[arg(long, default_value = "40000")]
        udp_port: u16,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParticipantConfig {
    public_key: String,
    private_key: String,
    tcp_addr: SocketAddrV4,
    udp_addr: SocketAddrV4,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterConfig {
    participants: Vec<ParticipantConfig>,
}

#[derive(Debug, Clone, Copy, alloy_rlp::RlpEncodable, alloy_rlp::RlpDecodable)]
struct MockMessage {
    timestamp: u64,
    message_len: usize,
}

impl MockMessage {
    fn new_with_timestamp(message_len: usize) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Self {
            timestamp,
            message_len,
        }
    }
}

impl Message for MockMessage {
    type NodeIdPubKey = PubKeyType;
    type Event = MockEvent<Self::NodeIdPubKey>;

    fn event(self, from: NodeId<Self::NodeIdPubKey>) -> Self::Event {
        MockEvent {
            from,
            message: self,
        }
    }
}

impl Serializable<bytes::Bytes> for MockMessage {
    fn serialize(&self) -> bytes::Bytes {
        let mut message = bytes::BytesMut::zeroed(self.message_len);
        let timestamp_bytes = self.timestamp.to_le_bytes();
        if message.len() >= 8 {
            message[0..8].copy_from_slice(&timestamp_bytes);
        }
        message.into()
    }
}

impl Deserializable<bytes::Bytes> for MockMessage {
    type ReadError = std::io::Error;

    fn deserialize(message: &bytes::Bytes) -> Result<Self, Self::ReadError> {
        if message.len() < 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Message too short",
            ));
        }
        let timestamp = u64::from_le_bytes(message[..8].try_into().unwrap());
        Ok(Self {
            timestamp,
            message_len: message.len(),
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct MockEvent<P: monad_crypto::certificate_signature::PubKey> {
    from: NodeId<P>,
    message: MockMessage,
}

impl<ST> From<RaptorCastEvent<MockEvent<CertificateSignaturePubKey<ST>>, ST>>
    for MockEvent<CertificateSignaturePubKey<ST>>
where
    ST: CertificateSignatureRecoverable,
{
    fn from(value: RaptorCastEvent<MockEvent<CertificateSignaturePubKey<ST>>, ST>) -> Self {
        match value {
            RaptorCastEvent::Message(event) => event,
            RaptorCastEvent::PeerManagerResponse(_) => unimplemented!(),
        }
    }
}

fn create_keypair(seed: u8) -> KeyPair {
    let mut key_bytes = [0u8; 32];
    let seed_bytes = (seed as u32).to_le_bytes();
    key_bytes[0] = seed_bytes[0];
    key_bytes[1] = seed_bytes[1];
    key_bytes[2] = seed_bytes[2];
    key_bytes[3] = seed_bytes[3];
    key_bytes[31] = 1;
    KeyPair::from_bytes(&mut key_bytes).unwrap()
}

fn create_raptorcast_config(keypair: Arc<KeyPair>) -> RaptorCastConfig<SignatureType> {
    RaptorCastConfig {
        shared_key: keypair,
        mtu: 1450,
        udp_message_max_age_ms: 5000,
        primary_instance: RaptorCastConfigPrimary::default(),
        secondary_instance: RaptorCastConfigSecondary {
            raptor10_redundancy: 2,
            mode: SecondaryRaptorCastModeConfig::None,
        },
    }
}

fn create_peer_discovery_builder(
    self_id: NodeId<PubKeyType>,
    self_record: MonadNameRecord<SignatureType>,
    routing_info: BTreeMap<NodeId<PubKeyType>, MonadNameRecord<SignatureType>>,
    epoch_validators: BTreeMap<NodeId<PubKeyType>, monad_raptorcast::util::Validator>,
) -> PeerDiscoveryBuilder<SignatureType> {
    PeerDiscoveryBuilder {
        self_id,
        self_record,
        current_round: Round(0),
        current_epoch: Epoch(0),
        epoch_validators: [(Epoch(0), epoch_validators.keys().cloned().collect())]
            .into_iter()
            .collect(),
        pinned_full_nodes: std::collections::BTreeSet::new(),
        routing_info,
        ping_period: Duration::from_millis(1000),
        refresh_period: Duration::from_millis(5000),
        request_timeout: Duration::from_millis(500),
        unresponsive_prune_threshold: 5,
        last_participation_prune_threshold: Round(100),
        min_num_peers: 3,
        max_num_peers: 50,
        rng: ChaCha8Rng::from_seed([0u8; 32]),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    
    runtime.block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run { cluster, index } => run_node(cluster, index).await,
        Commands::Producer {
            cluster,
            index,
            interval,
            size,
        } => run_producer(cluster, index, interval, size).await,
        Commands::Generate {
            output,
            count,
            ip,
            tcp_port,
            udp_port,
        } => generate_config(output, count, ip, tcp_port, udp_port),
    }
}

async fn run_producer(
    cluster_path: String,
    index: usize,
    interval: Duration,
    size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let config_str = std::fs::read_to_string(cluster_path)?;
    let cluster_config: ClusterConfig = toml::from_str(&config_str)?;

    if index >= cluster_config.participants.len() {
        return Err(format!(
            "Index {} out of range for {} participants",
            index,
            cluster_config.participants.len()
        )
        .into());
    }

    let my_config = &cluster_config.participants[index];
    let private_key_bytes = hex::decode(&my_config.private_key)?;
    let mut privkey_array = [0u8; 32];
    privkey_array.copy_from_slice(&private_key_bytes);
    let keypair = KeyPair::from_bytes(&mut privkey_array)?;

    let my_node_id = NodeId::new(keypair.pubkey());

    let mut routing_info = BTreeMap::new();
    let mut epoch_validators = BTreeMap::new();

    for participant in &cluster_config.participants {
        let mut participant_privkey = [0u8; 32];
        participant_privkey.copy_from_slice(&hex::decode(&participant.private_key)?);
        let participant_keypair = KeyPair::from_bytes(&mut participant_privkey)?;
        let participant_pubkey = participant_keypair.pubkey();
        let node_id = NodeId::new(participant_pubkey);

        let name_record = NameRecord {
            address: participant.udp_addr,
            seq: 0,
        };
        let monad_name_record = MonadNameRecord::new(name_record, &participant_keypair);

        routing_info.insert(node_id, monad_name_record);
        epoch_validators.insert(
            node_id,
            monad_raptorcast::util::Validator { stake: Stake::ONE },
        );
    }

    let my_name_record = NameRecord {
        address: my_config.udp_addr,
        seq: 0,
    };
    let my_monad_name_record = MonadNameRecord::new(my_name_record, &keypair);

    let server_address = SocketAddr::V4(SocketAddrV4::new(
        std::net::Ipv4Addr::new(0, 0, 0, 0),
        my_config.tcp_addr.port(),
    ));

    let dataplane = DataplaneBuilder::new(&server_address, 1_000).build();
    assert!(dataplane.block_until_ready(Duration::from_secs(2)));

    let (dataplane_reader, dataplane_writer) = dataplane.split();

    let pd = PeerDiscoveryDriver::new(create_peer_discovery_builder(
        my_node_id,
        my_monad_name_record,
        routing_info,
        epoch_validators.clone(),
    ));

    let mut raptorcast = RaptorCast::<
        SignatureType,
        MockMessage,
        MockMessage,
        <MockMessage as Message>::Event,
        PeerDiscovery<SignatureType>,
    >::new(
        create_raptorcast_config(Arc::new(keypair)),
        dataplane_reader,
        dataplane_writer,
        Arc::new(std::sync::Mutex::new(pd)),
    );

    raptorcast.exec(vec![RouterCommand::AddEpochValidatorSet {
        epoch: Epoch(0),
        validator_set: epoch_validators
            .iter()
            .map(|(id, v)| (*id, v.stake))
            .collect(),
    }]);

    tracing::info!(
        node_id = ?my_node_id,
        tcp_addr = ?server_address,
        udp_addr = ?my_config.udp_addr,
        interval = ?interval,
        message_size = size,
        "started producer node"
    );

    let mut interval_timer = tokio::time::interval(interval);
    interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            maybe_event = raptorcast.next() => {
                if let Some(event) = maybe_event {
                    match MockEvent::from(event) {
                        MockEvent { from, message } => {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_nanos() as u64;
                            let latency_ns = now - message.timestamp;
                            let latency_ms = latency_ns as f64 / 1_000_000.0;
                            tracing::info!(
                                from = ?from,
                                latency_ms = latency_ms,
                                message_size = message.message_len,
                                "message received"
                            );
                        }
                    }
                }
            }
            _ = interval_timer.tick() => {
                let message = MockMessage::new_with_timestamp(size);
                raptorcast.exec(vec![RouterCommand::Publish {
                    target: RouterTarget::Raptorcast(Epoch(0)),
                    message,
                }]);
                tracing::info!(
                    message_size = size,
                    "sent broadcast message"
                );
            }
        }
    }
}

async fn run_node(cluster_path: String, index: usize) -> Result<(), Box<dyn std::error::Error>> {
    let config_str = std::fs::read_to_string(cluster_path)?;
    let cluster_config: ClusterConfig = toml::from_str(&config_str)?;

    if index >= cluster_config.participants.len() {
        return Err(format!(
            "Index {} out of range for {} participants",
            index,
            cluster_config.participants.len()
        )
        .into());
    }

    let my_config = &cluster_config.participants[index];
    let private_key_bytes = hex::decode(&my_config.private_key)?;
    let mut privkey_array = [0u8; 32];
    privkey_array.copy_from_slice(&private_key_bytes);
    let keypair = KeyPair::from_bytes(&mut privkey_array)?;

    let my_node_id = NodeId::new(keypair.pubkey());

    let mut routing_info = BTreeMap::new();
    let mut epoch_validators = BTreeMap::new();

    for participant in &cluster_config.participants {
        let mut participant_privkey = [0u8; 32];
        participant_privkey.copy_from_slice(&hex::decode(&participant.private_key)?);
        let participant_keypair = KeyPair::from_bytes(&mut participant_privkey)?;
        let participant_pubkey = participant_keypair.pubkey();
        let node_id = NodeId::new(participant_pubkey);

        let name_record = NameRecord {
            address: participant.udp_addr,
            seq: 0,
        };
        let monad_name_record = MonadNameRecord::new(name_record, &participant_keypair);

        routing_info.insert(node_id, monad_name_record);
        epoch_validators.insert(
            node_id,
            monad_raptorcast::util::Validator { stake: Stake::ONE },
        );
    }

    let my_name_record = NameRecord {
        address: my_config.udp_addr,
        seq: 0,
    };
    let my_monad_name_record = MonadNameRecord::new(my_name_record, &keypair);

    let server_address = SocketAddr::V4(SocketAddrV4::new(
        std::net::Ipv4Addr::new(0, 0, 0, 0),
        my_config.tcp_addr.port(),
    ));

    let dataplane = DataplaneBuilder::new(&server_address, 1_000).build();
    assert!(dataplane.block_until_ready(Duration::from_secs(2)));

    let (dataplane_reader, dataplane_writer) = dataplane.split();

    let pd = PeerDiscoveryDriver::new(create_peer_discovery_builder(
        my_node_id,
        my_monad_name_record,
        routing_info,
        epoch_validators.clone(),
    ));

    let mut raptorcast = RaptorCast::<
        SignatureType,
        MockMessage,
        MockMessage,
        <MockMessage as Message>::Event,
        PeerDiscovery<SignatureType>,
    >::new(
        create_raptorcast_config(Arc::new(keypair)),
        dataplane_reader,
        dataplane_writer,
        Arc::new(std::sync::Mutex::new(pd)),
    );

    raptorcast.exec(vec![RouterCommand::AddEpochValidatorSet {
        epoch: Epoch(0),
        validator_set: epoch_validators
            .iter()
            .map(|(id, v)| (*id, v.stake))
            .collect(),
    }]);

    tracing::info!(
        node_id = ?my_node_id,
        tcp_addr = ?server_address,
        udp_addr = ?my_config.udp_addr,
        "Started node with raptorcast and discovery"
    );

    loop {
        tokio::select! {
            maybe_event = raptorcast.next() => {
                if let Some(event) = maybe_event {
                    match MockEvent::from(event) {
                        MockEvent { from, message } => {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_nanos() as u64;
                            let latency_ns = now - message.timestamp;
                            let latency_ms = latency_ns as f64 / 1_000_000.0;
                            tracing::info!(
                                from = ?from,
                                latency_ms = latency_ms,
                                message_size = message.message_len,
                                "message received"
                            );
                        }
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                tracing::trace!("Heartbeat");
            }
        }
    }
}

fn generate_config(
    output_path: String,
    count: usize,
    base_ip: String,
    tcp_port: u16,
    udp_port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut participants = Vec::new();

    let base_ip_addr: std::net::Ipv4Addr = base_ip
        .parse()
        .map_err(|_| format!("Invalid IP address: {}", base_ip))?;
    let base_ip_u32 = u32::from(base_ip_addr);

    for i in 0..count {
        let mut privkey = [0u8; 32];
        let idx_bytes = (i as u32).to_le_bytes();
        privkey[0] = idx_bytes[0];
        privkey[1] = idx_bytes[1];
        privkey[2] = idx_bytes[2];
        privkey[3] = idx_bytes[3];
        privkey[31] = 1;

        let keypair = KeyPair::from_bytes(&mut privkey.clone()).unwrap();
        let pubkey = keypair.pubkey();
        let pubkey_bytes = pubkey.bytes();

        let node_ip = std::net::Ipv4Addr::from(base_ip_u32 + i as u32);

        let participant = ParticipantConfig {
            public_key: hex::encode(pubkey_bytes),
            private_key: hex::encode(privkey),
            tcp_addr: SocketAddrV4::new(node_ip, tcp_port),
            udp_addr: SocketAddrV4::new(node_ip, udp_port),
        };
        participants.push(participant);
    }

    let cluster_config = ClusterConfig { participants };
    let toml_str = toml::to_string_pretty(&cluster_config)?;
    std::fs::write(&output_path, toml_str)?;

    println!(
        "Generated cluster configuration with {} nodes at {}",
        count, output_path
    );
    println!(
        "IP range: {} - {}",
        base_ip_addr,
        std::net::Ipv4Addr::from(base_ip_u32 + count as u32 - 1)
    );
    println!("Ports: TCP={}, UDP={}", tcp_port, udp_port);
    Ok(())
}
