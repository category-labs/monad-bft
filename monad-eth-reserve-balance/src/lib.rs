use monad_eth_types::{Balance, EthAccount, EthAddress, Nonce};
use monad_types::SeqNum;
use state_backend::StateBackend;
use tracing::{debug, trace};
pub mod state_backend;

// TODO: rename this result
pub enum ReserveBalanceCacheResult {
    Val(Balance, Nonce), // balance, nonce
    None,                // Account doesn't exist
    NeedSync,            // The requested block is ahead of latest triedb block seq num
}

// TODO: rename to AccountCachePolicy
pub trait ReserveBalanceCacheTrait<SBT> {
    fn new(state_backend: SBT, execution_delay: u64) -> Self;

    fn get_account(
        &mut self,
        backend_block_seq_num: SeqNum,
        address: &EthAddress,
    ) -> ReserveBalanceCacheResult;
}

#[derive(Debug)]
pub struct PassthruReserveBalanceCache<SBT> {
    state_backend: SBT,
}

impl<SBT: StateBackend + Clone> PassthruReserveBalanceCache<SBT> {
    /// for debug reasons: in mock-swarm, MonadStateBuilder accepts a function
    /// pointer which returns ReserveBalanceCache. The executors, in particular
    /// ledger is created separately. Need this method so MockEthLedger can be
    /// constructed to write to the shared state backend
    pub fn get_state_backend(&self) -> SBT {
        self.state_backend.clone()
    }
}

/// For twins testing only
impl<SBT: Clone> Clone for PassthruReserveBalanceCache<SBT> {
    fn clone(&self) -> Self {
        Self {
            state_backend: self.state_backend.clone(),
        }
    }
}

impl<SBT: StateBackend> ReserveBalanceCacheTrait<SBT> for PassthruReserveBalanceCache<SBT> {
    fn new(state_backend: SBT, _execution_delay: u64) -> Self {
        Self { state_backend }
    }

    fn get_account(
        &mut self,
        backend_block_seq_num: SeqNum,
        address: &EthAddress,
    ) -> ReserveBalanceCacheResult {
        debug!(
            block = backend_block_seq_num.0,
            "Passthru cache get_account"
        );

        if !self.state_backend.is_available(backend_block_seq_num.0) {
            trace!(
                block = backend_block_seq_num.0,
                "passthru cache get_account 1 \
                    backend needs sync "
            );
            return ReserveBalanceCacheResult::NeedSync;
        }

        if let Some(account) = self
            .state_backend
            .get_account(backend_block_seq_num.0, address)
        {
            let EthAccount {
                nonce,
                balance,
                code_hash: _,
            } = account;
            ReserveBalanceCacheResult::Val(balance, nonce)
        } else {
            ReserveBalanceCacheResult::None
        }
    }
}
