use std::time::Duration;

use iced::{
    widget::{Column, Slider, Text},
    Element,
};
use iced_lazy::Component;
use monad_consensus_state::ConsensusConfig;
use monad_consensus_types::block_validator::MockValidator;
use monad_crypto::secp256k1::KeyPair;
use monad_eth_types::EthAddress;
use monad_executor::State;
use monad_mock_swarm::swarm_relation::SwarmRelation;
use monad_state::MonadConfig;
use monad_testutil::validators::create_keys_w_validators;
use monad_transformer::{GenericTransformer, LatencyTransformer, XorLatencyTransformer, ID};
use monad_types::{NodeId, Round, SeqNum, Stake};

use crate::{graph::SimulationConfig, VizSwarm};

type MonadStateConfig = <<VizSwarm as SwarmRelation>::State as State>::Config;
type LoggerConfig = <VizSwarm as SwarmRelation>::LoggerConfig;
type RouterSchedulerConfig = <VizSwarm as SwarmRelation>::RouterSchedulerConfig;
type MempoolConfig = <VizSwarm as SwarmRelation>::MempoolConfig;
type Pipeline = <VizSwarm as SwarmRelation>::Pipeline;

#[derive(Debug, Clone)]
pub struct SimConfig {
    pub num_nodes: u32,
    pub delta: Duration,
    pub max_tick: Duration,
    pub pipeline: Pipeline,
}

impl SimConfig {
    fn with_num_nodes(mut self, num_nodes: u32) -> Self {
        self.num_nodes = num_nodes;
        self
    }

    fn with_max_tick(mut self, max_tick: f32) -> Self {
        self.max_tick = Duration::from_secs_f32(max_tick);
        self
    }

    fn with_delta(mut self, delta: Duration) -> Self {
        self.delta = delta;
        self
    }
}

impl SimulationConfig<VizSwarm> for SimConfig {
    fn nodes(
        &self,
    ) -> Vec<(
        ID,
        MonadStateConfig,
        LoggerConfig,
        RouterSchedulerConfig,
        MempoolConfig,
        Pipeline,
        u64,
    )> {
        let (keys, cert_keys, _validators, validator_mapping) = create_keys_w_validators::<
            <VizSwarm as SwarmRelation>::SignatureCollectionType,
        >(self.num_nodes);

        let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();

        let state_configs = keys
            .into_iter()
            .zip(cert_keys)
            .map(|(key, certkey)| MonadConfig {
                transaction_validator: MockValidator,
                key,
                certkey,
                val_set_update_interval: SeqNum(2000),
                epoch_start_delay: Round(50),
                beneficiary: EthAddress::default(),
                validators: validator_mapping
                    .map
                    .iter()
                    .map(|(node_id, sctpubkey)| (node_id.0, Stake(1), *sctpubkey))
                    .collect::<Vec<_>>(),

                consensus_config: ConsensusConfig {
                    proposal_txn_limit: 5000,
                    proposal_gas_limit: 8_000_000,
                    state_root_delay: SeqNum(0),
                    propose_with_missing_blocks: false,
                    delta: self.delta,
                },
            })
            .collect::<Vec<_>>();

        pubkeys
            .iter()
            .copied()
            .zip(state_configs)
            .map(|(a, b)| {
                (
                    ID::new(NodeId(a)),
                    b,
                    LoggerConfig {},
                    RouterSchedulerConfig {
                        all_peers: pubkeys.iter().map(|pubkey| NodeId(*pubkey)).collect(),
                    },
                    MempoolConfig::default(),
                    self.pipeline.clone(),
                    1,
                )
            })
            .collect()
    }

    fn max_tick(&self) -> Duration {
        self.max_tick
    }
}

pub struct ConfigEditor<'c, Message> {
    config: &'c SimConfig,
    on_change: Box<dyn Fn(SimConfig) -> Message>,
}

impl<'c, Message> ConfigEditor<'c, Message> {
    pub fn new(config: &'c SimConfig, on_change: impl Fn(SimConfig) -> Message + 'static) -> Self {
        Self {
            config,
            on_change: Box::new(on_change),
        }
    }
}

impl<'c, Message, Renderer> Component<Message, Renderer> for ConfigEditor<'c, Message>
where
    Renderer: iced_native::text::Renderer + 'static,
    Renderer::Theme: iced::widget::slider::StyleSheet + iced::widget::text::StyleSheet,
{
    type State = ();
    type Event = SimConfig;

    fn update(&mut self, _state: &mut (), event: Self::Event) -> Option<Message> {
        Some((self.on_change)(event))
    }

    fn view(&self, _state: &()) -> Element<'_, Self::Event, Renderer> {
        let mut col = Column::new()
            .push(Text::new(format!("Num Nodes: {}", self.config.num_nodes)))
            .push(Slider::new(2..=20, self.config.num_nodes, |num_nodes| {
                self.config.clone().with_num_nodes(num_nodes)
            }))
            .push(Text::new(format!(
                "Simulation Length: {:.1}s",
                self.config.max_tick.as_secs_f32()
            )))
            .push(
                Slider::new(0.1..=20.0, self.config.max_tick.as_secs_f32(), |max_tick| {
                    self.config.clone().with_max_tick(max_tick)
                })
                .step(0.1),
            )
            .push(Text::new(format!(
                "Delta: {}ms",
                self.config.delta.as_millis()
            )))
            .push(Slider::new(
                1..=1_000,
                self.config.delta.as_millis() as u32,
                |delta_ms| {
                    self.config
                        .clone()
                        .with_delta(Duration::from_millis(delta_ms.into()))
                },
            ));

        for idx in 0..self.config.pipeline.len() {
            let layer = &self.config.pipeline[idx];
            let element = match layer {
                GenericTransformer::Latency(LatencyTransformer(latency)) => Column::new()
                    .push(Text::new(format!(
                        "({}) Latency Transformer: {}ms",
                        idx,
                        latency.as_millis()
                    )))
                    .push(Slider::new(
                        0..=1_000,
                        latency.as_millis() as u32,
                        move |l_ms: u32| {
                            let mut config = self.config.clone();
                            config.pipeline[idx] = GenericTransformer::Latency(LatencyTransformer(
                                Duration::from_millis(l_ms.into()),
                            ));
                            config
                        },
                    )),
                GenericTransformer::XorLatency(XorLatencyTransformer(latency)) => Column::new()
                    .push(Text::new(format!(
                        "({}) XorLatency Transformer: {}ms",
                        idx,
                        latency.as_millis()
                    )))
                    .push(Slider::new(
                        0..=1_000,
                        latency.as_millis() as u32,
                        move |l_ms| {
                            let mut config = self.config.clone();
                            config.pipeline[idx] = GenericTransformer::XorLatency(
                                XorLatencyTransformer(Duration::from_millis(l_ms.into())),
                            );
                            config
                        },
                    )),
                _ => todo!(),
            };
            col = col.push(element);
        }

        col.max_width(300).into()
    }
}

impl<'c, Message, Renderer> From<ConfigEditor<'c, Message>> for Element<'c, Message, Renderer>
where
    Message: 'c,
    Renderer: iced_native::text::Renderer + 'static,
    Renderer::Theme: iced::widget::slider::StyleSheet + iced::widget::text::StyleSheet,
{
    fn from(component: ConfigEditor<'c, Message>) -> Self {
        iced_lazy::component(component)
    }
}
