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
    collections::HashMap,
    marker::PhantomData,
    net::{SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex},
    time::Duration,
};

use monad_chain_config::{revision::MockChainRevision, MockChainConfig};
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::{
    block::{MockExecutionProtocol, PassthruBlockPolicy},
    block_validator::MockValidator,
    validator_data::{ValidatorSetData, ValidatorSetDataWithEpoch},
};
use monad_control_panel::ipc::ControlPanelIpcReceiver;
use monad_crypto::certificate_signature::{
    CertificateSignature, CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_dataplane::{DataplaneBuilder, UdpSocketConfig};
use monad_executor_glue::{Command, MonadEvent, RouterCommand, ValSetCommand};
use monad_peer_discovery::{
    driver::PeerDiscoveryDriver,
    mock::{NopDiscovery, NopDiscoveryBuilder},
};
use monad_raptorcast::{
    auth::NoopAuthProtocol, config::RaptorCastConfig, networking::Dataplane,
    raptorcast_secondary::SecondaryRaptorCastModeConfig, RaptorCast,
    AUTHENTICATED_RAPTORCAST_SOCKET, RAPTORCAST_SOCKET,
};
use monad_state::{Forkpoint, MonadMessage, MonadState, MonadStateBuilder, VerifiedMonadMessage};
use monad_state_backend::InMemoryState;
use monad_types::{Epoch, ExecutionProtocol, NodeId, Round, SeqNum};
use monad_updaters::{
    config_file::MockConfigFile, config_loader::MockConfigLoader, ledger::MockLedger,
    local_router::LocalPeerRouter, loopback::LoopbackExecutor, parent::ParentExecutor,
    statesync::MockStateSyncExecutor, timer::TokioTimer, tokio_timestamp::TokioTimestamp,
    txpool::MockTxPoolExecutor, val_set::MockValSetUpdaterNop, BoxUpdater, Updater,
};
use monad_validator::{
    signature_collection::SignatureCollection, simple_round_robin::SimpleRoundRobin,
    validator_set::ValidatorSetFactory,
};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer};

pub enum RouterConfig<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    Local(
        /// Must be passed ahead-of-time because they can't be instantiated individually
        LocalPeerRouter<ST, MonadMessage<ST, SCT, EPT>, VerifiedMonadMessage<ST, SCT, EPT>>,
    ),
    RaptorCast(RaptorCastConfig<ST>),
}

pub enum LedgerConfig {
    Mock,
}

pub enum ValSetConfig<SCT>
where
    SCT: SignatureCollection,
{
    Mock {
        genesis_validator_data: ValidatorSetData<SCT>,
        epoch_length: SeqNum,
    },
}

pub struct ExecutorConfig<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    pub local_addr: SocketAddr,
    pub known_addresses: HashMap<NodeId<SCT::NodeIdPubKey>, SocketAddrV4>,
    pub router_config: RouterConfig<ST, SCT, EPT>,
    pub ledger_config: LedgerConfig,
    pub val_set_config: ValSetConfig<SCT>,
    pub nodeid: NodeId<SCT::NodeIdPubKey>,
}

pub fn make_monad_executor<ST, SCT>(
    index: usize,
    state_backend: InMemoryState<ST, SCT>,
    config: ExecutorConfig<ST, SCT, MockExecutionProtocol>,
) -> ParentExecutor<
    BoxUpdater<
        'static,
        RouterCommand<ST, VerifiedMonadMessage<ST, SCT, MockExecutionProtocol>>,
        MonadEvent<ST, SCT, MockExecutionProtocol>,
    >,
    TokioTimer<MonadEvent<ST, SCT, MockExecutionProtocol>>,
    MockLedger<ST, SCT, MockExecutionProtocol>,
    MockConfigFile<ST, SCT, MockExecutionProtocol>,
    BoxUpdater<'static, ValSetCommand, MonadEvent<ST, SCT, MockExecutionProtocol>>,
    TokioTimestamp<ST, SCT, MockExecutionProtocol>,
    MockTxPoolExecutor<
        ST,
        SCT,
        MockExecutionProtocol,
        PassthruBlockPolicy,
        InMemoryState<ST, SCT>,
        MockChainConfig,
        MockChainRevision,
    >,
    ControlPanelIpcReceiver<ST, SCT, MockExecutionProtocol>,
    LoopbackExecutor<MonadEvent<ST, SCT, MockExecutionProtocol>>,
    MockStateSyncExecutor<ST, SCT, MockExecutionProtocol>,
    MockConfigLoader<ST, SCT, MockExecutionProtocol>,
>
where
    ST: CertificateSignatureRecoverable + Unpin,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>> + Unpin,
    <ST as CertificateSignature>::KeyPairType: Unpin,
    <SCT as SignatureCollection>::SignatureType: Unpin,
{
    let (filter, reload_handle) =
        tracing_subscriber::reload::Layer::new(EnvFilter::from_default_env());

    let subscriber = tracing_subscriber::Registry::default()
        .with(tracing_subscriber::fmt::Layer::default().with_filter(filter));

    let _ = tracing::subscriber::set_global_default(subscriber);

    let peer_discovery_builder = NopDiscoveryBuilder {
        known_addresses: config.known_addresses,
        ..Default::default()
    };

    ParentExecutor {
        metrics: Default::default(),
        router: match config.router_config {
            RouterConfig::Local(router) => Updater::boxed(router),

            RouterConfig::RaptorCast(cfg) => {
                let pdd = PeerDiscoveryDriver::new(peer_discovery_builder);
                let shared_peer_discovery_driver = Arc::new(Mutex::new(pdd));

                let auth_socket_addr =
                    SocketAddr::new(config.local_addr.ip(), config.local_addr.port() + 1);
                let dataplane_builder = DataplaneBuilder::new(&config.local_addr, 1_000)
                    .with_udp_buffer_size(62_500_000)
                    .extend_udp_sockets(vec![
                        UdpSocketConfig {
                            socket_addr: auth_socket_addr,
                            label: AUTHENTICATED_RAPTORCAST_SOCKET.to_string(),
                        },
                        UdpSocketConfig {
                            socket_addr: config.local_addr,
                            label: RAPTORCAST_SOCKET.to_string(),
                        },
                    ]);

                let dp = dataplane_builder.build();
                assert!(dp.block_until_ready(Duration::from_secs(1)));

                let (tcp_socket, mut udp_dataplane, control) = dp.split();
                let authenticated_socket = udp_dataplane
                    .take_socket(AUTHENTICATED_RAPTORCAST_SOCKET)
                    .expect("authenticated raptorcast socket");
                let non_authenticated_socket = udp_dataplane
                    .take_socket(RAPTORCAST_SOCKET)
                    .expect("raptorcast socket");
                let (tcp_reader, tcp_writer) = tcp_socket.split();

                let auth_protocol = NoopAuthProtocol::new();
                Updater::boxed(RaptorCast::<
                    ST,
                    MonadMessage<ST, SCT, MockExecutionProtocol>,
                    VerifiedMonadMessage<ST, SCT, MockExecutionProtocol>,
                    MonadEvent<ST, SCT, MockExecutionProtocol>,
                    NopDiscovery<ST>,
                    NoopAuthProtocol<CertificateSignaturePubKey<ST>>,
                    Dataplane,
                >::new(
                    cfg,
                    SecondaryRaptorCastModeConfig::None,
                    tcp_reader,
                    tcp_writer,
                    Some(authenticated_socket),
                    non_authenticated_socket,
                    control,
                    shared_peer_discovery_driver,
                    Epoch(0),
                    auth_protocol,
                ))
            }
        },

        timer: TokioTimer::default(),
        ledger: match config.ledger_config {
            LedgerConfig::Mock => MockLedger::new(state_backend.clone()),
        },
        config_file: MockConfigFile::default(),
        val_set: match config.val_set_config {
            ValSetConfig::Mock {
                genesis_validator_data,
                epoch_length,
            } => Updater::boxed(MockValSetUpdaterNop::new(
                genesis_validator_data,
                epoch_length,
            )),
        },
        timestamp: TokioTimestamp::new(Duration::from_millis(5), 100, 10001),
        txpool: MockTxPoolExecutor::default(),
        control_panel: ControlPanelIpcReceiver::new(
            format!("./monad_controlpanel_{}.sock", index).into(),
            Box::new(reload_handle),
            1000,
        )
        .expect("uds bind failed"),
        loopback: LoopbackExecutor::default(),
        state_sync: MockStateSyncExecutor::new(state_backend),
        config_loader: MockConfigLoader::default(),
    }
}

type MonadStateType<ST, SCT> = MonadState<
    ST,
    SCT,
    MockExecutionProtocol,
    PassthruBlockPolicy,
    InMemoryState<ST, SCT>,
    ValidatorSetFactory<CertificateSignaturePubKey<ST>>,
    SimpleRoundRobin<CertificateSignaturePubKey<ST>>,
    MockValidator,
    MockChainConfig,
    MockChainRevision,
>;

pub struct StateConfig<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub key: ST::KeyPairType,

    pub cert_key: <SCT::SignatureType as CertificateSignature>::KeyPairType,

    pub epoch_length: SeqNum,
    pub epoch_start_delay: Round,

    pub validators: ValidatorSetData<SCT>,
    pub consensus_config: ConsensusConfig<MockChainConfig, MockChainRevision>,
}

pub fn make_monad_state<ST, SCT>(
    state_backend: InMemoryState<ST, SCT>,
    config: StateConfig<ST, SCT>,
) -> (
    MonadStateType<ST, SCT>,
    Vec<
        Command<
            MonadEvent<ST, SCT, MockExecutionProtocol>,
            VerifiedMonadMessage<ST, SCT, MockExecutionProtocol>,
            ST,
            SCT,
            MockExecutionProtocol,
            PassthruBlockPolicy,
            InMemoryState<ST, SCT>,
            MockChainConfig,
            MockChainRevision,
        >,
    >,
)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    let forkpoint = Forkpoint::genesis();
    let locked_epoch_validators: Vec<_> = forkpoint
        .validator_sets
        .iter()
        .map(|locked_epoch| ValidatorSetDataWithEpoch {
            epoch: locked_epoch.epoch,
            validators: config.validators.clone(),
        })
        .collect();
    MonadStateBuilder {
        validator_set_factory: ValidatorSetFactory::default(),
        leader_election: SimpleRoundRobin::default(),
        block_validator: MockValidator {},
        block_policy: PassthruBlockPolicy {},
        state_backend,
        key: config.key,
        certkey: config.cert_key,
        beneficiary: Default::default(),
        forkpoint,
        locked_epoch_validators,
        block_sync_override_peers: Default::default(),
        consensus_config: config.consensus_config,
        whitelisted_statesync_nodes: Default::default(),
        statesync_expand_to_group: true,

        _phantom: PhantomData,
    }
    .build()
}
