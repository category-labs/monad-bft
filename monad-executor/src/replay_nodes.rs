use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use monad_crypto::secp256k1::PubKey;

use crate::{
    Command, Identifiable, LedgerCommand, Message, PeerId, RouterCommand, RouterTarget, State,
};

pub struct PendingMsg<S>
where
    S: State,
{
    pub send_id: PeerId,
    pub send_tick: Duration,
    pub event: S::Event,
    pub message: <S as State>::Message,
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
    pub replay_nodes_info: BTreeMap<PeerId, NodesInfo<S>>,
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
            replay_nodes_info.insert(PeerId(pubkey), nodes_info);
        }
        Self { replay_nodes_info }
    }

    pub fn step(&mut self, node_id: &PeerId, event: S::Event, tick: Duration) {
        let state = self.replay_nodes_info.get_mut(node_id).unwrap().mut_state();
        let commands = state.update(event);
        self.mutate_state(node_id, tick, commands);
    }

    fn mutate_state(
        &mut self,
        node_id: &PeerId,
        tick: Duration,
        cmds: Vec<
            Command<
                <S as State>::Message,
                <S as State>::OutboundMessage,
                <S as State>::Block,
                <S as State>::Checkpoint,
            >,
        >,
    ) {
        let mut to_publish = Vec::new();
        let mut to_unpublish = HashSet::new();

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
                        to_publish.push((target, message));
                    }
                    RouterCommand::Unpublish { target, id } => {
                        to_unpublish.insert((target, id));
                    }
                },
                _ => {}
            }
        }
        for (target, out_message) in to_publish {
            let id = out_message.as_ref().id();
            if to_unpublish.contains(&(target, id)) {
                continue;
            }
            let message = out_message.into();

            let tos = match target {
                RouterTarget::PointToPoint(to) => vec![to],
                RouterTarget::Broadcast => {
                    let peer_ids: Vec<_> = self.replay_nodes_info.keys().copied().collect();
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
                    event: message.clone().event(*node_id),
                    message: message.clone(),
                });
            }
        }
    }
}
