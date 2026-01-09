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

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:16,prof_leak:true\0";

use std::{
    collections::{BTreeMap, HashMap},
    env,
    net::{SocketAddr, SocketAddrV4},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use byte_unit::Byte;
use clap::{Parser, Subcommand};
use eyre::Result;
use futures_util::{FutureExt, StreamExt};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::DataplaneBuilder;
use monad_executor::{Executor, ExecutorMetricsChain};
use monad_executor_glue::{Message, RouterCommand};
use monad_node_config::{fullnode_raptorcast::FullNodeRaptorCastConfig, FullNodeConfig};
use monad_peer_discovery::{
    driver::PeerDiscoveryDriver,
    mock::{NopDiscovery, NopDiscoveryBuilder},
    MonadNameRecord, NameRecord,
};
use monad_raptorcast::{
    config::{RaptorCastConfig, RaptorCastConfigPrimary},
    raptorcast_secondary::SecondaryRaptorCastModeConfig,
    RaptorCast, RaptorCastEvent, AUTHENTICATED_RAPTORCAST_SOCKET, RAPTORCAST_SOCKET,
};
use monad_secp::{KeyPair, SecpSignature};
use monad_types::{Deserializable, Epoch, NodeId, RouterTarget, Serializable, Stake};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

type SignatureType = SecpSignature;
type PubKeyType = CertificateSignaturePubKey<SignatureType>;

fn parse_duration(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s).map_err(|e| e.to_string())
}

fn parse_size(s: &str) -> Result<usize, String> {
    let byte = Byte::parse_str(s, true).map_err(|e| e.to_string())?;
    Ok(byte.as_u64() as usize)
}

#[derive(Parser)]
#[command(name = "node")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(
        about = "run as consumer (receives messages only). if NODE_INDEX env variable is set, it overwrites --index"
    )]
    Consumer {
        #[arg(long)]
        cluster: String,
        #[arg(long)]
        cluster_size: Option<usize>,
        #[arg(
            long,
            help = "node index (0-based). can be overwritten by NODE_INDEX env variable"
        )]
        index: Option<usize>,
        #[arg(long, help = "opentelemetry otlp endpoint for metrics export")]
        otel_endpoint: Option<String>,
        #[arg(
            long,
            value_parser = parse_duration,
            default_value = "10s",
            help = "interval for recording metrics"
        )]
        metrics_interval: Duration,
    },
    #[command(
        about = "run as producer (sends and receives messages). if NODE_INDEX env variable is set, it overwrites --index"
    )]
    Producer {
        #[arg(long)]
        cluster: String,
        #[arg(long)]
        cluster_size: Option<usize>,
        #[arg(
            long,
            help = "node index (0-based). can be overwritten by NODE_INDEX env variable"
        )]
        index: Option<usize>,
        #[arg(long, value_parser = parse_duration)]
        interval: Duration,
        #[arg(long, value_parser = parse_size)]
        size: usize,
        #[arg(long, help = "opentelemetry otlp endpoint for metrics export")]
        otel_endpoint: Option<String>,
        #[arg(
            long,
            value_parser = parse_duration,
            default_value = "10s",
            help = "interval for recording metrics"
        )]
        metrics_interval: Duration,
    },
    Generate {
        #[arg(long)]
        output: String,
        #[arg(long)]
        count: usize,
        #[arg(long, default_value = "127.0.0.1")]
        ip: String,
        #[arg(long, default_value = "30000")]
        port: u16,
    },
}

mod socket_addr_v4_serde {
    use std::net::SocketAddrV4;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(addr: &SocketAddrV4, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        addr.to_string().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SocketAddrV4, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParticipantConfig {
    public_key: String,
    private_key: String,
    #[serde(with = "socket_addr_v4_serde")]
    tcp_addr: SocketAddrV4,
    #[serde(with = "socket_addr_v4_serde")]
    udp_addr: SocketAddrV4,
    #[serde(with = "socket_addr_v4_serde")]
    authenticated_udp_addr: SocketAddrV4,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterConfig {
    participants: Vec<ParticipantConfig>,
}

#[derive(Debug, Clone, alloy_rlp::RlpEncodable, alloy_rlp::RlpDecodable)]
struct MockMessage {
    timestamp: u64,
    data: bytes::Bytes,
}

impl MockMessage {
    fn new_with_timestamp(message_len: usize) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let mut data = bytes::BytesMut::with_capacity(message_len);
        data.resize(message_len, 0);

        let timestamp_bytes = timestamp.to_le_bytes();
        if data.len() >= 8 {
            data[0..8].copy_from_slice(&timestamp_bytes);
        }

        if data.len() > 8 {
            let mut rng = thread_rng();
            rng.fill(&mut data[8..]);
        }

        Self {
            timestamp,
            data: data.freeze(),
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
        self.data.clone()
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
            data: message.clone(),
        })
    }
}

#[derive(Clone, Debug)]
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
            RaptorCastEvent::SecondaryRaptorcastPeersUpdate(_, _) => unimplemented!(),
        }
    }
}

fn create_raptorcast_config(keypair: Arc<KeyPair>) -> RaptorCastConfig<SignatureType> {
    RaptorCastConfig {
        shared_key: keypair,
        mtu: monad_dataplane::udp::DEFAULT_MTU,
        udp_message_max_age_ms: 5000,
        sig_verification_rate_limit: 4_000,
        primary_instance: RaptorCastConfigPrimary::default(),
        secondary_instance: FullNodeRaptorCastConfig {
            enable_publisher: false,
            enable_client: false,
            full_nodes_prioritized: FullNodeConfig { identities: vec![] },
            raptor10_fullnode_redundancy_factor: 2.0,
            round_span: monad_types::Round(10),
            invite_lookahead: monad_types::Round(5),
            max_invite_wait: monad_types::Round(3),
            deadline_round_dist: monad_types::Round(3),
            init_empty_round_span: monad_types::Round(1),
            max_group_size: 10,
            max_num_group: 5,
            invite_future_dist_min: monad_types::Round(1),
            invite_future_dist_max: monad_types::Round(5),
            invite_accept_heartbeat_ms: 100,
        },
    }
}

fn setup_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt::fmt()
        .with_env_filter(env_filter)
        .init();
}

#[derive(Default)]
struct LatencyMetrics {
    total_messages_received: u64,
    total_messages_sent: u64,
    last_latency_us: u64,
    min_latency_us: u64,
    max_latency_us: u64,
    total_latency_sum_us: u64,
}

impl LatencyMetrics {
    fn new() -> Self {
        Self {
            total_messages_received: 0,
            total_messages_sent: 0,
            last_latency_us: 0,
            min_latency_us: u64::MAX,
            max_latency_us: 0,
            total_latency_sum_us: 0,
        }
    }

    fn record_received(&mut self, latency_ns: u64) {
        let latency_us = latency_ns / 1_000;
        self.total_messages_received += 1;
        self.last_latency_us = latency_us;
        self.min_latency_us = self.min_latency_us.min(latency_us);
        self.max_latency_us = self.max_latency_us.max(latency_us);
        self.total_latency_sum_us += latency_us;
    }

    fn record_sent(&mut self) {
        self.total_messages_sent += 1;
    }

    fn avg_latency_us(&self) -> u64 {
        if self.total_messages_received > 0 {
            self.total_latency_sum_us / self.total_messages_received
        } else {
            0
        }
    }

    fn metrics(&self) -> Vec<(&'static str, u64)> {
        vec![
            (GAUGE_LATENCY_LAST_US, self.last_latency_us),
            (
                GAUGE_LATENCY_MIN_US,
                if self.min_latency_us == u64::MAX {
                    0
                } else {
                    self.min_latency_us
                },
            ),
            (GAUGE_LATENCY_MAX_US, self.max_latency_us),
            (GAUGE_LATENCY_AVG_US, self.avg_latency_us()),
            (GAUGE_MESSAGES_RECEIVED, self.total_messages_received),
            (GAUGE_MESSAGES_SENT, self.total_messages_sent),
        ]
    }
}

const GAUGE_LATENCY_LAST_US: &str = "raptorcast.latency.last_us";
const GAUGE_LATENCY_MIN_US: &str = "raptorcast.latency.min_us";
const GAUGE_LATENCY_MAX_US: &str = "raptorcast.latency.max_us";
const GAUGE_LATENCY_AVG_US: &str = "raptorcast.latency.avg_us";
const GAUGE_MESSAGES_RECEIVED: &str = "raptorcast.messages.received";
const GAUGE_MESSAGES_SENT: &str = "raptorcast.messages.sent";
const GAUGE_UPTIME_US: &str = "raptorcast.uptime_us";

fn send_metrics(
    meter: &opentelemetry::metrics::Meter,
    gauge_cache: &mut HashMap<&'static str, opentelemetry::metrics::Gauge<u64>>,
    latency_metrics: &LatencyMetrics,
    raptorcast_metrics: ExecutorMetricsChain,
    process_start: &Instant,
) {
    for (k, v) in latency_metrics
        .metrics()
        .into_iter()
        .chain(raptorcast_metrics.into_inner())
        .chain(std::iter::once((
            GAUGE_UPTIME_US,
            process_start.elapsed().as_micros() as u64,
        )))
    {
        let gauge = gauge_cache
            .entry(k)
            .or_insert_with(|| meter.u64_gauge(k).build());
        gauge.record(v, &[]);
    }
}

fn build_otel_meter_provider(
    otel_endpoint: &str,
    service_name: String,
    interval: Duration,
) -> Result<opentelemetry_sdk::metrics::SdkMeterProvider> {
    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_timeout(interval * 2)
        .with_endpoint(otel_endpoint)
        .build()?;

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(interval / 2)
        .build();

    let attrs = vec![opentelemetry::KeyValue::new(
        opentelemetry_semantic_conventions::resource::SERVICE_NAME,
        service_name,
    )];

    let provider_builder = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(
            opentelemetry_sdk::Resource::builder_empty()
                .with_attributes(attrs)
                .build(),
        );

    Ok(provider_builder.build())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    async_main().await
}

async fn async_main() -> Result<()> {
    let cli = Cli::parse();

    setup_tracing();

    match cli.command {
        Commands::Consumer {
            cluster,
            cluster_size,
            index,
            otel_endpoint,
            metrics_interval,
        } => {
            run_consumer(
                cluster,
                cluster_size,
                index,
                otel_endpoint,
                metrics_interval,
            )
            .await
        }
        Commands::Producer {
            cluster,
            cluster_size,
            index,
            interval,
            size,
            otel_endpoint,
            metrics_interval,
        } => {
            run_producer(
                cluster,
                cluster_size,
                index,
                interval,
                size,
                otel_endpoint,
                metrics_interval,
            )
            .await
        }
        Commands::Generate {
            output,
            count,
            ip,
            port,
        } => generate_config(output, count, ip, port),
    }
}

const UDP_BW: u64 = 1_000;

fn get_node_index(index_arg: Option<usize>) -> Result<usize> {
    let node_index = if let Ok(env_index) = env::var("NODE_INDEX") {
        let parsed = env_index
            .parse::<usize>()
            .map_err(|_| eyre::eyre!("NODE_INDEX must be a valid number"))?;
        let zero_based = parsed.saturating_sub(1);
        tracing::info!(
            node_index = zero_based,
            raw_env_value = parsed,
            source = "NODE_INDEX env variable",
            "using node index from environment (converted from 1-based to 0-based)"
        );
        zero_based
    } else if let Some(index) = index_arg {
        tracing::info!(
            node_index = index,
            source = "--index argument",
            "using node index from command line"
        );
        index
    } else {
        eyre::bail!(
            "node index must be provided via --index argument or NODE_INDEX environment variable"
        );
    };

    Ok(node_index)
}

struct NodeSetup {
    raptorcast: RaptorCast<
        SignatureType,
        MockMessage,
        MockMessage,
        <MockMessage as Message>::Event,
        NopDiscovery<SignatureType>,
        monad_raptorcast::auth::WireAuthProtocol,
    >,
    node_id: NodeId<CertificateSignaturePubKey<SignatureType>>,
    tcp_addr: SocketAddrV4,
    udp_addr: SocketAddr,
}

fn setup_node(
    cluster_path: String,
    cluster_size: Option<usize>,
    node_index: usize,
) -> Result<NodeSetup> {
    let index = node_index;

    let config_str = std::fs::read_to_string(cluster_path)?;
    let cluster_config: ClusterConfig = toml::from_str(&config_str)?;

    if index >= cluster_config.participants.len() {
        eyre::bail!(
            "index {} out of range for {} participants",
            index,
            cluster_config.participants.len()
        );
    }

    let my_config = &cluster_config.participants[index];
    let private_key_bytes = hex::decode(&my_config.private_key)?;
    let mut privkey_array = [0u8; 32];
    privkey_array.copy_from_slice(&private_key_bytes);
    let keypair = KeyPair::from_bytes(&mut privkey_array)?;

    let my_node_id = NodeId::new(keypair.pubkey());

    let mut routing_info = BTreeMap::new();
    let mut epoch_validators = BTreeMap::new();

    let participants_to_use = if let Some(size) = cluster_size {
        let limited_size = size.min(cluster_config.participants.len());
        tracing::info!(
            cluster_size = size,
            total_participants = cluster_config.participants.len(),
            using_participants = limited_size,
            "limiting validator set size"
        );
        &cluster_config.participants[..limited_size]
    } else {
        &cluster_config.participants[..]
    };

    for participant in participants_to_use {
        let mut participant_privkey = [0u8; 32];
        participant_privkey.copy_from_slice(&hex::decode(&participant.private_key)?);
        let participant_keypair = KeyPair::from_bytes(&mut participant_privkey)?;
        let participant_pubkey = participant_keypair.pubkey();
        let node_id = NodeId::new(participant_pubkey);

        let name_record = NameRecord::new_with_authentication(
            *participant.tcp_addr.ip(),
            participant.tcp_addr.port(),
            participant.udp_addr.port(),
            participant.authenticated_udp_addr.port(),
            0,
        );
        let monad_name_record =
            MonadNameRecord::<SignatureType>::new(name_record, &participant_keypair);

        routing_info.insert(node_id, monad_name_record);
        epoch_validators.insert(node_id, Stake::ONE);
    }

    let bind_ip = std::net::Ipv4Addr::new(0, 0, 0, 0);
    let tcp_addr = SocketAddr::V4(SocketAddrV4::new(bind_ip, my_config.tcp_addr.port()));
    let authenticated_udp_addr = SocketAddr::V4(SocketAddrV4::new(
        bind_ip,
        my_config.authenticated_udp_addr.port(),
    ));
    let non_authenticated_addr =
        SocketAddr::V4(SocketAddrV4::new(bind_ip, my_config.udp_addr.port()));

    let dataplane = DataplaneBuilder::new(&tcp_addr, UDP_BW)
        .with_udp_multishot(true)
        .extend_udp_sockets(vec![
            monad_dataplane::UdpSocketConfig {
                socket_addr: authenticated_udp_addr,
                label: AUTHENTICATED_RAPTORCAST_SOCKET.to_string(),
            },
            monad_dataplane::UdpSocketConfig {
                socket_addr: non_authenticated_addr,
                label: RAPTORCAST_SOCKET.to_string(),
            },
        ])
        .build();

    let (tcp_socket, mut udp_dataplane, dataplane_control) = dataplane.split();
    let (tcp_reader, tcp_writer) = tcp_socket.split();
    let authenticated_socket = udp_dataplane
        .take_socket(AUTHENTICATED_RAPTORCAST_SOCKET)
        .expect("authenticated socket not found");
    let non_authenticated_socket = udp_dataplane
        .take_socket(RAPTORCAST_SOCKET)
        .expect("non-authenticated socket not found");

    let mut known_addresses = std::collections::HashMap::new();
    for (node_id, record) in &routing_info {
        known_addresses.insert(*node_id, record.name_record.udp_socket());
    }

    let noop_builder = NopDiscoveryBuilder {
        known_addresses,
        name_records: routing_info.into_iter().collect(),
        pd: std::marker::PhantomData,
    };

    let pd = PeerDiscoveryDriver::new(noop_builder);

    let keypair_arc = Arc::new(keypair);
    let wireauth_config = monad_wireauth::Config::default();
    let auth_protocol = monad_raptorcast::auth::WireAuthProtocol::new(
        &monad_raptorcast::auth::metrics::UDP_METRICS,
        wireauth_config,
        keypair_arc.clone(),
    );

    let mut raptorcast = RaptorCast::<
        SignatureType,
        MockMessage,
        MockMessage,
        <MockMessage as Message>::Event,
        NopDiscovery<SignatureType>,
        monad_raptorcast::auth::WireAuthProtocol,
    >::new(
        create_raptorcast_config(keypair_arc),
        SecondaryRaptorCastModeConfig::None,
        tcp_reader,
        tcp_writer,
        Some(authenticated_socket),
        non_authenticated_socket,
        dataplane_control,
        Arc::new(std::sync::Mutex::new(pd)),
        Epoch(0),
        auth_protocol,
    );

    raptorcast.exec(vec![RouterCommand::AddEpochValidatorSet {
        epoch: Epoch(0),
        validator_set: epoch_validators
            .iter()
            .map(|(id, stake)| (*id, *stake))
            .collect(),
    }]);

    Ok(NodeSetup {
        raptorcast,
        node_id: my_node_id,
        tcp_addr: my_config.tcp_addr,
        udp_addr: authenticated_udp_addr,
    })
}

async fn run_producer(
    cluster_path: String,
    cluster_size: Option<usize>,
    index_arg: Option<usize>,
    interval: Duration,
    size: usize,
    otel_endpoint: Option<String>,
    metrics_interval: Duration,
) -> Result<()> {
    let node_index = get_node_index(index_arg)?;
    let NodeSetup {
        mut raptorcast,
        node_id,
        tcp_addr,
        udp_addr,
    } = setup_node(cluster_path, cluster_size, node_index)?;

    tracing::info!(
        node_id = ?node_id,
        tcp_addr = ?tcp_addr,
        udp_addr = ?udp_addr,
        interval = ?interval,
        message_size = size,
        otel_endpoint = ?otel_endpoint,
        "started producer node"
    );

    let (maybe_otel_meter_provider, mut maybe_metrics_ticker) = otel_endpoint
        .map(|endpoint| {
            let provider = build_otel_meter_provider(
                &endpoint,
                format!("raptorcast_producer_{}", node_index),
                metrics_interval,
            )
            .expect("failed to build otel provider");

            let mut timer = tokio::time::interval(metrics_interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            (provider, timer)
        })
        .unzip();

    let maybe_otel_meter = maybe_otel_meter_provider
        .as_ref()
        .map(|provider| provider.meter("raptorcast_latency"));

    let mut gauge_cache = HashMap::new();
    let process_start = Instant::now();
    let mut metrics = LatencyMetrics::new();

    let mut interval_timer = tokio::time::interval(interval);
    loop {
        tokio::select! {
            maybe_event = raptorcast.next() => {
                if let Some(MockEvent { from, message }) = maybe_event {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;
                    let latency_ns = now - message.timestamp;
                    let latency_ms = latency_ns as f64 / 1_000_000.0;

                    metrics.record_received(latency_ns);

                    tracing::info!(
                        from = ?from,
                        latency_ms = latency_ms,
                        message_size = message.data.len(),
                        "message received"
                    );
                }
            }
            _ = interval_timer.tick() => {
                let message = MockMessage::new_with_timestamp(size);
                raptorcast.exec(vec![RouterCommand::Publish {
                    target: RouterTarget::Raptorcast(Epoch(0)),
                    message,
                }]);

                metrics.record_sent();

                tracing::info!(
                    message_size = size,
                    "sent broadcast message"
                );
            }
            _ = match &mut maybe_metrics_ticker {
                Some(ticker) => ticker.tick().boxed(),
                None => futures_util::future::pending().boxed(),
            } => {
                let otel_meter = maybe_otel_meter.as_ref().expect("otel_endpoint must have been set");
                let raptorcast_metrics = raptorcast.metrics();
                send_metrics(otel_meter, &mut gauge_cache, &metrics, raptorcast_metrics, &process_start);
                tracing::debug!("exported metrics");
            }
        }
    }
}

async fn run_consumer(
    cluster_path: String,
    cluster_size: Option<usize>,
    index_arg: Option<usize>,
    otel_endpoint: Option<String>,
    metrics_interval: Duration,
) -> Result<()> {
    let node_index = get_node_index(index_arg)?;
    let NodeSetup {
        mut raptorcast,
        node_id,
        tcp_addr,
        udp_addr,
    } = setup_node(cluster_path, cluster_size, node_index)?;

    tracing::info!(
        node_id = ?node_id,
        tcp_addr = ?tcp_addr,
        udp_addr = ?udp_addr,
        otel_endpoint = ?otel_endpoint,
        "started consumer node"
    );

    let (maybe_otel_meter_provider, mut maybe_metrics_ticker) = otel_endpoint
        .map(|endpoint| {
            let provider = build_otel_meter_provider(
                &endpoint,
                format!("raptorcast_consumer_{}", node_index),
                metrics_interval,
            )
            .expect("failed to build otel provider");

            let mut timer = tokio::time::interval(metrics_interval);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            (provider, timer)
        })
        .unzip();

    let maybe_otel_meter = maybe_otel_meter_provider
        .as_ref()
        .map(|provider| provider.meter("raptorcast_latency"));

    let mut gauge_cache = HashMap::new();
    let process_start = Instant::now();
    let mut metrics = LatencyMetrics::new();

    loop {
        tokio::select! {
            maybe_event = raptorcast.next() => {
                if let Some(MockEvent { from, message }) = maybe_event {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;
                    let latency_ns = now - message.timestamp;
                    let latency_ms = latency_ns as f64 / 1_000_000.0;

                    metrics.record_received(latency_ns);

                    tracing::info!(
                        from = ?from,
                        latency_ms = latency_ms,
                        message_size = message.data.len(),
                        "message received"
                    );
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                tracing::trace!("heartbeat");
            }
            _ = match &mut maybe_metrics_ticker {
                Some(ticker) => ticker.tick().boxed(),
                None => futures_util::future::pending().boxed(),
            } => {
                let otel_meter = maybe_otel_meter.as_ref().expect("otel_endpoint must have been set");
                let raptorcast_metrics = raptorcast.metrics();
                send_metrics(otel_meter, &mut gauge_cache, &metrics, raptorcast_metrics, &process_start);
                tracing::debug!("exported metrics");
            }
        }
    }
}

fn generate_config(output_path: String, count: usize, base_ip: String, port: u16) -> Result<()> {
    let mut participants = Vec::new();

    let base_ip_addr: std::net::Ipv4Addr = base_ip
        .parse()
        .map_err(|_| eyre::eyre!("invalid ip address: {}", base_ip))?;
    let base_ip_u32 = u32::from(base_ip_addr);

    for i in 0..count {
        let ikm = (i as u32).to_le_bytes();
        let keypair = KeyPair::from_ikm(&ikm)?;
        let pubkey = keypair.pubkey();
        let pubkey_bytes = pubkey.bytes();
        let privkey = keypair.privkey_view().to_string();

        let node_ip = std::net::Ipv4Addr::from(base_ip_u32 + i as u32);

        let participant = ParticipantConfig {
            public_key: hex::encode(pubkey_bytes),
            private_key: privkey,
            tcp_addr: SocketAddrV4::new(node_ip, port),
            udp_addr: SocketAddrV4::new(node_ip, port),
            authenticated_udp_addr: SocketAddrV4::new(node_ip, port + 1),
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
    println!("Port: {}", port);
    Ok(())
}
