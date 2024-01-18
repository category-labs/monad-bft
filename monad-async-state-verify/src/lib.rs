use std::marker::PhantomData;

use monad_crypto::{certificate_signature::PubKey, hasher::Hash};
use monad_types::{NodeId, Round, SeqNum};

pub enum AsyncStateVerifyCommand<P: PubKey> {
    /// Broadcast local execution result to other validators
    BroadcastStateRoot {
        self_id: NodeId<P>,
        seq_num: SeqNum,
        round: Round,
        state_root: Hash,
    },
    /// Send state root update to consensus state (we can remove this by having
    /// consensus taking a reference to async state verify)
    StateRootUpdate {
        seq_num: SeqNum,
        round: Round,
        state_root: Hash,
    },
}

pub trait AsyncStateVerifyProcess<P: PubKey> {
    fn handle_local_state_root(
        &mut self,
        seq_num: SeqNum,
        round: Round,
        state_root: Hash,
    ) -> Vec<AsyncStateVerifyCommand<P>>;

    fn handle_peer_state_root(
        &mut self,
        peer: NodeId<P>,
        seq_num: SeqNum,
        round: Round,
        state_root: Hash,
    ) -> Vec<AsyncStateVerifyCommand<P>>;
}

impl<P, T> AsyncStateVerifyProcess<P> for Box<T>
where
    P: PubKey,
    T: AsyncStateVerifyProcess<P> + ?Sized,
{
    fn handle_local_state_root(
        &mut self,
        seq_num: SeqNum,
        round: Round,
        state_root: Hash,
    ) -> Vec<AsyncStateVerifyCommand<P>> {
        (**self).handle_local_state_root(seq_num, round, state_root)
    }

    fn handle_peer_state_root(
        &mut self,
        peer: NodeId<P>,
        seq_num: SeqNum,
        round: Round,
        state_root: Hash,
    ) -> Vec<AsyncStateVerifyCommand<P>> {
        (**self).handle_peer_state_root(peer, seq_num, round, state_root)
    }
}

/// A mock version that doesn't verify/compare anything. It uses the state root
/// calculated by its own execution, and forward that to consensus state
#[derive(Clone)]
pub struct LocalAsyncStateVerify<P: PubKey> {
    _phantom: PhantomData<P>,
}

impl<P: PubKey> Default for LocalAsyncStateVerify<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P: PubKey> LocalAsyncStateVerify<P> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<P: PubKey> AsyncStateVerifyProcess<P> for LocalAsyncStateVerify<P> {
    fn handle_local_state_root(
        &mut self,
        seq_num: SeqNum,
        round: Round,
        state_root: Hash,
    ) -> Vec<AsyncStateVerifyCommand<P>> {
        vec![AsyncStateVerifyCommand::StateRootUpdate {
            seq_num,
            round,
            state_root,
        }]
    }

    fn handle_peer_state_root(
        &mut self,
        _peer: NodeId<P>,
        _seq_num: SeqNum,
        _round: Round,
        _state_root: Hash,
    ) -> Vec<AsyncStateVerifyCommand<P>> {
        // ignore peer state root values
        vec![]
    }
}
