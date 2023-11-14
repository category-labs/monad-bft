use std::{collections::BTreeMap, time::Duration};

use monad_crypto::secp256k1::PubKey;
use monad_executor_glue::{Command, LedgerCommand, Message, RouterCommand, RouterTarget};
use monad_types::NodeId;

use crate::State;

pub struct PendingMsg<S>
where
    S: State,
{
    pub send_id: NodeId,
    pub send_tick: Duration,
    pub event: S::Event,
    pub message: <S as State>::OutboundMessage,
}

pub struct NodesInfo<S>
where
    S: State,
{
    state: S,
    blockchain: Vec<S::Block>,
    pending_messages: Vec<PendingMsg<S>>,
}

impl<S> NodesInfo<S>
where
    S: State,
{
    pub fn state(&self) -> &S {
        &self.state
    }
    pub fn mut_state(&mut self) -> &mut S {
        &mut self.state
    }

    pub fn blockchain(&self) -> &Vec<S::Block> {
        &self.blockchain
    }

    pub fn mut_blockchain(&mut self) -> &mut Vec<S::Block> {
        &mut self.blockchain
    }

    pub fn pending_messages(&self) -> &Vec<PendingMsg<S>> {
        &self.pending_messages
    }

    pub fn mut_pending_messages(&mut self) -> &mut Vec<PendingMsg<S>> {
        &mut self.pending_messages
    }
}

pub struct ReplayNodes<S>
where
    S: State,
{
    pub replay_nodes_info: BTreeMap<NodeId, NodesInfo<S>>,
}

impl<S> ReplayNodes<S>
where
    S: State,
{
    pub fn new(peers: Vec<(PubKey, S::Config)>) -> Self {
        assert!(!peers.is_empty());
        let mut replay_nodes_info = BTreeMap::new();
        for (pubkey, state_config) in peers {
            let (state, _) = S::init(state_config);
            let blockchain = Vec::new();
            let pending_messages = Vec::new();
            let nodes_info = NodesInfo {
                state,
                blockchain,
                pending_messages,
            };
            replay_nodes_info.insert(NodeId(pubkey), nodes_info);
        }
        Self { replay_nodes_info }
    }

    pub fn step(&mut self, node_id: &NodeId, event: S::Event, tick: Duration) {
        let state = self.replay_nodes_info.get_mut(node_id).unwrap().mut_state();
        let commands = state.update(event);
        self.mutate_state(node_id, tick, commands);
    }

    fn mutate_state(
        &mut self,
        node_id: &NodeId,
        tick: Duration,
        cmds: Vec<
            Command<
                <S as State>::Event,
                <S as State>::OutboundMessage,
                <S as State>::Block,
                <S as State>::Checkpoint,
                <S as State>::SignatureCollection,
            >,
        >,
    ) {
        for command in cmds {
            match command {
                Command::LedgerCommand(LedgerCommand::LedgerCommit(b)) => {
                    let block = self
                        .replay_nodes_info
                        .get_mut(node_id)
                        .unwrap()
                        .mut_blockchain();
                    block.extend(b);
                }
                Command::RouterCommand(cmd) => match cmd {
                    RouterCommand::Publish { target, message } => {
                        let tos = match target {
                            RouterTarget::PointToPoint(to) => vec![to],
                            RouterTarget::Broadcast => {
                                let peer_ids: Vec<_> =
                                    self.replay_nodes_info.keys().copied().collect();
                                peer_ids.clone()
                            }
                        };
                        for to in tos.iter().filter(|to| **to != *node_id) {
                            let msg_queue = self
                                .replay_nodes_info
                                .get_mut(to)
                                .unwrap()
                                .mut_pending_messages();
                            msg_queue.push(PendingMsg {
                                send_id: *node_id,
                                send_tick: tick,
                                event: message.clone().into().event(*node_id),
                                message: message.clone(),
                            });
                        }
                    }
                },
                _ => {}
            }
        }
    }
}
