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
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use monad_crypto::NopPubKey;
use monad_transformer::{LinkMessage, Pipeline};
use monad_types::NodeId;

pub const DEFAULT_LINK_LATENCY_MS: u64 = 25;
pub const TOPOLOGY_MIN_COORD: i32 = -500;
pub const TOPOLOGY_MAX_COORD: i32 = 1500;
const TOPOLOGY_CENTER: f64 = 500.0;
const TOPOLOGY_RADIUS: f64 = 330.0;
const POSITION_LATENCY_BASE_MS: u64 = 5;
const POSITION_LATENCY_SCALE: f64 = 0.08;
const POSITION_LATENCY_MIN_MS: u64 = 1;
const POSITION_LATENCY_MAX_MS: u64 = 250;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkNodeState {
    pub id: NodeId<NopPubKey>,
    pub x: i32,
    pub y: i32,
    pub online: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkRule {
    pub latency: Duration,
    pub dropped: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveLinkRule {
    pub from: NodeId<NopPubKey>,
    pub to: NodeId<NopPubKey>,
    pub latency: Duration,
    pub dropped: bool,
    pub offline: bool,
    pub overridden: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkConfig {
    default_latency: Duration,
    peers: BTreeSet<NodeId<NopPubKey>>,
    nodes: BTreeMap<NodeId<NopPubKey>, NetworkNodeState>,
    links: BTreeMap<(NodeId<NopPubKey>, NodeId<NopPubKey>), LinkRule>,
}

impl NetworkConfig {
    pub fn new(default_latency: Duration, peers: BTreeSet<NodeId<NopPubKey>>) -> Self {
        assert!(default_latency > Duration::ZERO);
        let nodes = default_topology_nodes(&peers);
        Self {
            default_latency,
            peers,
            nodes,
            links: Default::default(),
        }
    }

    pub fn default_latency(&self) -> Duration {
        self.default_latency
    }

    #[cfg(test)]
    pub fn peers(&self) -> &BTreeSet<NodeId<NopPubKey>> {
        &self.peers
    }

    pub fn nodes(&self) -> Vec<NetworkNodeState> {
        self.nodes.values().cloned().collect()
    }

    pub fn set_default_latency(&mut self, latency: Duration) -> Result<(), String> {
        validate_latency(latency)?;
        self.default_latency = latency;
        Ok(())
    }

    pub fn set_link_latency(
        &mut self,
        from: NodeId<NopPubKey>,
        to: NodeId<NopPubKey>,
        latency: Duration,
    ) -> Result<(), String> {
        validate_latency(latency)?;
        self.validate_link(from, to)?;
        let dropped = self.configured_rule(from, to).dropped;
        self.links.insert((from, to), LinkRule { latency, dropped });
        Ok(())
    }

    pub fn set_link_dropped(
        &mut self,
        from: NodeId<NopPubKey>,
        to: NodeId<NopPubKey>,
        dropped: bool,
    ) -> Result<(), String> {
        self.validate_link(from, to)?;
        let latency = self.configured_rule(from, to).latency;
        self.links.insert((from, to), LinkRule { latency, dropped });
        Ok(())
    }

    pub fn clear_link_rule(
        &mut self,
        from: NodeId<NopPubKey>,
        to: NodeId<NopPubKey>,
    ) -> Result<(), String> {
        self.validate_link(from, to)?;
        self.links.remove(&(from, to));
        Ok(())
    }

    pub fn set_node_position(
        &mut self,
        node: NodeId<NopPubKey>,
        x: i32,
        y: i32,
    ) -> Result<(), String> {
        self.validate_node(node, "node")?;
        validate_position(x, y)?;

        let Some(state) = self.nodes.get_mut(&node) else {
            return Err("unknown node id".to_owned());
        };
        state.x = x;
        state.y = y;

        let peers: Vec<_> = self
            .peers
            .iter()
            .copied()
            .filter(|peer| *peer != node)
            .collect();
        for peer in peers {
            let latency = self.position_latency(node, peer)?;
            self.set_link_latency(node, peer, latency)?;
            self.set_link_latency(peer, node, latency)?;
        }
        Ok(())
    }

    pub fn set_node_online(
        &mut self,
        node: NodeId<NopPubKey>,
        online: bool,
    ) -> Result<(), String> {
        self.validate_node(node, "node")?;
        let Some(state) = self.nodes.get_mut(&node) else {
            return Err("unknown node id".to_owned());
        };
        state.online = online;
        Ok(())
    }

    pub fn apply(&mut self, command: &NetworkCommandAction) -> Result<(), String> {
        match command {
            NetworkCommandAction::SetDefaultLatency { latency } => {
                self.set_default_latency(*latency)
            }
            NetworkCommandAction::SetLinkLatency { from, to, latency } => {
                self.set_link_latency(*from, *to, *latency)
            }
            NetworkCommandAction::SetLinkDropped { from, to, dropped } => {
                self.set_link_dropped(*from, *to, *dropped)
            }
            NetworkCommandAction::ClearLinkRule { from, to } => {
                self.clear_link_rule(*from, *to)
            }
            NetworkCommandAction::SetNodePosition { node, x, y } => {
                self.set_node_position(*node, *x, *y)
            }
            NetworkCommandAction::SetNodeOnline { node, online } => {
                self.set_node_online(*node, *online)
            }
        }
    }

    pub fn effective_rule(&self, from: NodeId<NopPubKey>, to: NodeId<NopPubKey>) -> LinkRule {
        let rule = self.configured_rule(from, to);
        let offline = self.link_offline(from, to);
        LinkRule {
            latency: rule.latency,
            dropped: rule.dropped || offline,
        }
    }

    fn configured_rule(&self, from: NodeId<NopPubKey>, to: NodeId<NopPubKey>) -> LinkRule {
        self.links.get(&(from, to)).cloned().unwrap_or(LinkRule {
            latency: self.default_latency,
            dropped: false,
        })
    }

    pub fn effective_link_rules(&self) -> Vec<EffectiveLinkRule> {
        self.peers
            .iter()
            .flat_map(|from| {
                self.peers.iter().filter_map(move |to| {
                    if from == to {
                        return None;
                    }
                    let rule = self.effective_rule(*from, *to);
                    Some(EffectiveLinkRule {
                        from: *from,
                        to: *to,
                        latency: rule.latency,
                        dropped: rule.dropped,
                        offline: self.link_offline(*from, *to),
                        overridden: self.links.contains_key(&(*from, *to)),
                    })
                })
            })
            .collect()
    }

    fn min_latency(&self) -> Duration {
        self.effective_link_rules()
            .into_iter()
            .filter(|rule| !rule.dropped)
            .map(|rule| rule.latency)
            .chain(std::iter::once(self.default_latency))
            .min()
            .unwrap_or(self.default_latency)
    }

    fn position_latency(
        &self,
        from: NodeId<NopPubKey>,
        to: NodeId<NopPubKey>,
    ) -> Result<Duration, String> {
        let from = self
            .nodes
            .get(&from)
            .ok_or_else(|| "unknown source node id".to_owned())?;
        let to = self
            .nodes
            .get(&to)
            .ok_or_else(|| "unknown target node id".to_owned())?;
        Ok(latency_from_positions(from.x, from.y, to.x, to.y))
    }

    fn link_offline(&self, from: NodeId<NopPubKey>, to: NodeId<NopPubKey>) -> bool {
        !self.node_online(from) || !self.node_online(to)
    }

    fn node_online(&self, node: NodeId<NopPubKey>) -> bool {
        self.nodes
            .get(&node)
            .map(|node| node.online)
            .unwrap_or(false)
    }

    fn validate_link(
        &self,
        from: NodeId<NopPubKey>,
        to: NodeId<NopPubKey>,
    ) -> Result<(), String> {
        if from == to {
            return Err("cannot configure self link".to_owned());
        }
        self.validate_node(from, "source")?;
        self.validate_node(to, "target")?;
        Ok(())
    }

    fn validate_node(&self, node: NodeId<NopPubKey>, role: &str) -> Result<(), String> {
        if !self.peers.contains(&node) {
            return Err(format!("unknown {role} node id"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkCommand {
    pub tick: Duration,
    pub sequence: u64,
    pub action: NetworkCommandAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NetworkCommandAction {
    SetDefaultLatency {
        latency: Duration,
    },
    SetLinkLatency {
        from: NodeId<NopPubKey>,
        to: NodeId<NopPubKey>,
        latency: Duration,
    },
    SetLinkDropped {
        from: NodeId<NopPubKey>,
        to: NodeId<NopPubKey>,
        dropped: bool,
    },
    ClearLinkRule {
        from: NodeId<NopPubKey>,
        to: NodeId<NopPubKey>,
    },
    SetNodePosition {
        node: NodeId<NopPubKey>,
        x: i32,
        y: i32,
    },
    SetNodeOnline {
        node: NodeId<NopPubKey>,
        online: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkPipeline {
    config: NetworkConfig,
}

impl NetworkPipeline {
    pub fn new(config: NetworkConfig) -> Self {
        Self { config }
    }
}

impl<M> Pipeline<M> for NetworkPipeline {
    type NodeIdPubKey = NopPubKey;

    fn process(
        &mut self,
        message: LinkMessage<Self::NodeIdPubKey, M>,
    ) -> Vec<(Duration, LinkMessage<Self::NodeIdPubKey, M>)> {
        let rule = self
            .config
            .effective_rule(*message.from.get_peer_id(), *message.to.get_peer_id());
        if rule.dropped {
            Vec::new()
        } else {
            vec![(rule.latency, message)]
        }
    }

    fn len(&self) -> usize {
        1
    }

    fn is_empty(&self) -> bool {
        false
    }

    fn min_external_delay(&self) -> Duration {
        self.config.min_latency()
    }

    fn is_outbound_blocked(&self, _tick: Duration, node: &NodeId<Self::NodeIdPubKey>) -> bool {
        if !self.config.node_online(*node) {
            return true;
        }
        self.config
            .peers
            .iter()
            .filter(|peer| *peer != node)
            .all(|peer| self.config.effective_rule(*node, *peer).dropped)
    }
}

pub fn validate_latency(latency: Duration) -> Result<(), String> {
    if latency == Duration::ZERO {
        Err("latency must be greater than zero".to_owned())
    } else {
        Ok(())
    }
}

fn validate_position(x: i32, y: i32) -> Result<(), String> {
    if !(TOPOLOGY_MIN_COORD..=TOPOLOGY_MAX_COORD).contains(&x) {
        return Err(format!(
            "x must be between {TOPOLOGY_MIN_COORD} and {TOPOLOGY_MAX_COORD}"
        ));
    }
    if !(TOPOLOGY_MIN_COORD..=TOPOLOGY_MAX_COORD).contains(&y) {
        return Err(format!(
            "y must be between {TOPOLOGY_MIN_COORD} and {TOPOLOGY_MAX_COORD}"
        ));
    }
    Ok(())
}

fn latency_from_positions(from_x: i32, from_y: i32, to_x: i32, to_y: i32) -> Duration {
    let dx = f64::from(from_x - to_x);
    let dy = f64::from(from_y - to_y);
    let distance = (dx * dx + dy * dy).sqrt();
    let latency_ms = (POSITION_LATENCY_BASE_MS as f64 + distance * POSITION_LATENCY_SCALE)
        .round()
        .clamp(POSITION_LATENCY_MIN_MS as f64, POSITION_LATENCY_MAX_MS as f64)
        as u64;
    Duration::from_millis(latency_ms)
}

fn default_topology_nodes(
    peers: &BTreeSet<NodeId<NopPubKey>>,
) -> BTreeMap<NodeId<NopPubKey>, NetworkNodeState> {
    let total = peers.len().max(1) as f64;
    peers
        .iter()
        .enumerate()
        .map(|(index, peer)| {
            let angle = (index as f64 / total) * 2.0 * std::f64::consts::PI
                - std::f64::consts::FRAC_PI_2;
            let x = (TOPOLOGY_CENTER + TOPOLOGY_RADIUS * angle.cos())
                .round()
                .clamp(f64::from(TOPOLOGY_MIN_COORD), f64::from(TOPOLOGY_MAX_COORD))
                as i32;
            let y = (TOPOLOGY_CENTER + TOPOLOGY_RADIUS * angle.sin())
                .round()
                .clamp(f64::from(TOPOLOGY_MIN_COORD), f64::from(TOPOLOGY_MAX_COORD))
                as i32;
            (
                *peer,
                NetworkNodeState {
                    id: *peer,
                    x,
                    y,
                    online: true,
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, time::Duration};

    use monad_crypto::{certificate_signature::CertificateKeyPair, NopSignature};
    use monad_testutil::signing::create_keys;
    use monad_transformer::{LinkMessage, Pipeline, ID};
    use monad_types::NodeId;

    use super::{latency_from_positions, NetworkConfig, NetworkPipeline};

    fn test_peers() -> (NodeId<monad_crypto::NopPubKey>, NodeId<monad_crypto::NopPubKey>) {
        let keys = create_keys::<NopSignature>(2);
        (NodeId::new(keys[0].pubkey()), NodeId::new(keys[1].pubkey()))
    }

    fn test_peer_vec(count: usize) -> Vec<NodeId<monad_crypto::NopPubKey>> {
        create_keys::<NopSignature>(count.try_into().unwrap())
            .into_iter()
            .map(|key| NodeId::new(key.pubkey()))
            .collect()
    }

    fn test_message(
        from: NodeId<monad_crypto::NopPubKey>,
        to: NodeId<monad_crypto::NopPubKey>,
    ) -> LinkMessage<monad_crypto::NopPubKey, ()> {
        LinkMessage {
            from: ID::new(from),
            to: ID::new(to),
            message: (),
            from_tick: Duration::ZERO,
            nonce: 0,
        }
    }

    #[test]
    fn matrix_pipeline_applies_default_override_and_drop() {
        let (from, to) = test_peers();
        let mut config =
            NetworkConfig::new(Duration::from_millis(25), BTreeSet::from([from, to]));

        let mut pipeline = NetworkPipeline::new(config.clone());
        let output = pipeline.process(test_message(from, to));
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].0, Duration::from_millis(25));

        config
            .set_link_latency(from, to, Duration::from_millis(7))
            .unwrap();
        let mut pipeline = NetworkPipeline::new(config.clone());
        let output = pipeline.process(test_message(from, to));
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].0, Duration::from_millis(7));

        config.set_link_dropped(from, to, true).unwrap();
        let mut pipeline = NetworkPipeline::new(config);
        assert!(pipeline.process(test_message(from, to)).is_empty());
    }

    #[test]
    fn clear_link_rule_restores_default_latency() {
        let (from, to) = test_peers();
        let mut config =
            NetworkConfig::new(Duration::from_millis(25), BTreeSet::from([from, to]));
        config
            .set_link_latency(from, to, Duration::from_millis(7))
            .unwrap();
        config.clear_link_rule(from, to).unwrap();

        let mut pipeline = NetworkPipeline::new(config);
        let output = pipeline.process(test_message(from, to));
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].0, Duration::from_millis(25));
    }

    #[test]
    fn changed_config_does_not_rewrite_already_processed_messages() {
        let (from, to) = test_peers();
        let mut config =
            NetworkConfig::new(Duration::from_millis(25), BTreeSet::from([from, to]));
        let mut pipeline = NetworkPipeline::new(config.clone());
        let old_output = pipeline.process(test_message(from, to));

        config
            .set_link_latency(from, to, Duration::from_millis(7))
            .unwrap();
        let mut new_pipeline = NetworkPipeline::new(config);
        let new_output = new_pipeline.process(test_message(from, to));

        assert_eq!(old_output[0].0, Duration::from_millis(25));
        assert_eq!(new_output[0].0, Duration::from_millis(7));
    }

    #[test]
    fn offline_node_drops_inbound_and_outbound_until_restarted() {
        let (from, to) = test_peers();
        let mut config =
            NetworkConfig::new(Duration::from_millis(25), BTreeSet::from([from, to]));
        config
            .set_link_latency(from, to, Duration::from_millis(7))
            .unwrap();

        config.set_node_online(from, false).unwrap();
        assert!(config.effective_rule(from, to).dropped);
        assert!(config.effective_rule(to, from).dropped);

        config.set_node_online(from, true).unwrap();
        let restored = config.effective_rule(from, to);
        assert!(!restored.dropped);
        assert_eq!(restored.latency, Duration::from_millis(7));
        assert!(!config.effective_rule(to, from).dropped);
    }

    #[test]
    fn node_position_updates_connected_symmetric_latencies_only() {
        let peers = test_peer_vec(3);
        let from = peers[0];
        let peer_a = peers[1];
        let peer_b = peers[2];
        let mut config = NetworkConfig::new(
            Duration::from_millis(25),
            BTreeSet::from([from, peer_a, peer_b]),
        );
        let untouched = config.effective_rule(peer_a, peer_b).latency;

        config.set_node_position(from, 100, 100).unwrap();

        let nodes = config.nodes();
        let moved = nodes.iter().find(|node| node.id == from).unwrap();
        let peer_a_state = nodes.iter().find(|node| node.id == peer_a).unwrap();
        let peer_b_state = nodes.iter().find(|node| node.id == peer_b).unwrap();
        let expected_a = latency_from_positions(moved.x, moved.y, peer_a_state.x, peer_a_state.y);
        let expected_b = latency_from_positions(moved.x, moved.y, peer_b_state.x, peer_b_state.y);

        assert_eq!(config.effective_rule(from, peer_a).latency, expected_a);
        assert_eq!(config.effective_rule(peer_a, from).latency, expected_a);
        assert_eq!(config.effective_rule(from, peer_b).latency, expected_b);
        assert_eq!(config.effective_rule(peer_b, from).latency, expected_b);
        assert_eq!(config.effective_rule(peer_a, peer_b).latency, untouched);
    }
}
