use std::time::Duration;

use iced::{
    widget::{Column, Slider, Text},
    Element,
};
use iced_lazy::Component;
use monad_consensus_types::{
    quorum_certificate::genesis_vote_info, transaction_validator::MockValidator,
    validation::Sha256Hash,
};
use monad_crypto::secp256k1::{KeyPair, PubKey};
use monad_executor::{
    mock_swarm::{LatencyTransformer, Layer, LayerTransformer, XorLatencyTransformer},
    State,
};
use monad_state::MonadConfig;
use monad_testutil::{signing::get_genesis_config, validators::create_keys_w_validators};
use monad_wal::{mock::MockWALoggerConfig, PersistenceLogger};

use crate::{graph::SimulationConfig, PersistenceLoggerType, SignatureCollectionType, MM, MS};

#[derive(Debug, Clone)]
pub struct SimConfig {
    pub num_nodes: u32,
    pub delta: Duration,
    pub max_tick: Duration,
    pub transformer: LayerTransformer<MM>,
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

impl SimulationConfig<MS, LayerTransformer<MM>, PersistenceLoggerType> for SimConfig {
    fn nodes(
        &self,
    ) -> Vec<(
        PubKey,
        <MS as State>::Config,
        <PersistenceLoggerType as PersistenceLogger>::Config,
    )> {
        let (keys, blskeys, validators, validators_prop) = create_keys_w_validators(self.num_nodes);

        let pubkeys = keys.iter().map(KeyPair::pubkey).collect::<Vec<_>>();
        let (genesis_block, genesis_sigs) = get_genesis_config::<Sha256Hash, SignatureCollectionType>(
            keys.iter(),
            &validators_prop,
        );

        let state_configs = keys
            .into_iter()
            .zip(std::iter::repeat(pubkeys.clone()))
            .map(|(key, pubkeys)| MonadConfig {
                transaction_validator: MockValidator,
                key,
                validators: pubkeys
                    .iter()
                    .cloned()
                    .zip(blskeys.iter().map(|k| k.pubkey()))
                    .collect(),

                delta: self.delta,
                genesis_block: genesis_block.clone(),
                genesis_vote_info: genesis_vote_info(genesis_block.get_id()),
                genesis_signatures: genesis_sigs.clone(),
            })
            .collect::<Vec<_>>();

        pubkeys
            .into_iter()
            .zip(state_configs)
            .zip(std::iter::repeat(MockWALoggerConfig {}))
            .map(|((a, b), c)| (a, b, c))
            .collect()
    }

    fn max_tick(&self) -> Duration {
        self.max_tick
    }

    fn transformer(&self) -> &LayerTransformer<MM> {
        &self.transformer
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

        for (idx, layer) in self.config.transformer.iter().enumerate() {
            let element = match layer {
                Layer::Latency(LatencyTransformer(latency)) => Column::new()
                    .push(Text::new(format!(
                        "({}) Latency Layer: {}ms",
                        idx,
                        latency.as_millis()
                    )))
                    .push(Slider::new(
                        0..=1_000,
                        latency.as_millis() as u32,
                        move |l_ms| {
                            let mut config = self.config.clone();
                            config.transformer[idx] = Layer::Latency(LatencyTransformer(
                                Duration::from_millis(l_ms.into()),
                            ));
                            config
                        },
                    )),
                Layer::XorLatency(XorLatencyTransformer(latency)) => Column::new()
                    .push(Text::new(format!(
                        "({}) XorLatency Layer: {}ms",
                        idx,
                        latency.as_millis()
                    )))
                    .push(Slider::new(
                        0..=1_000,
                        latency.as_millis() as u32,
                        move |l_ms| {
                            let mut config = self.config.clone();
                            config.transformer[idx] = Layer::XorLatency(XorLatencyTransformer(
                                Duration::from_millis(l_ms.into()),
                            ));
                            config
                        },
                    )),
                Layer::Blacklist(_) => todo!(),
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
