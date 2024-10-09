use std::{collections::HashMap, sync::Arc};

use alloy_rpc_client::ReqwestClient;
use reth_primitives::{revm_primitives::HashSet, Address};
use ruint::Uint;
use tokio::sync::{RwLock, RwLockReadGuard};

use self::manager::{ChainStateManager, ChainStateManagerHandle};

mod blockstream;
mod manager;

#[derive(Debug, Default)]
pub struct ChainState {
    new_accounts: HashSet<Address>,
    accounts: HashMap<Address, ChainAccountState>,
}

impl ChainState {
    pub async fn new_with_manager(
        client: ReqwestClient,
    ) -> (ChainStateManagerHandle, ChainStateView) {
        let (manager, chain_state) = ChainStateManager::new(client).await;

        (manager.start(), chain_state)
    }

    pub fn get_account(&self, address: &Address) -> Option<&ChainAccountState> {
        self.accounts.get(address)
    }
}

#[derive(Debug)]
pub struct ChainAccountState {
    balance: Uint<256, 4>,
    nonce: u64,
}

impl ChainAccountState {
    pub fn new(balance: Uint<256, 4>, nonce: u64) -> Self {
        Self { balance, nonce }
    }

    pub fn get_balance(&self) -> Uint<256, 4> {
        self.balance
    }

    pub fn get_nonce(&self) -> u64 {
        self.nonce
    }
}

pub type SharedChainState = Arc<RwLock<ChainState>>;

#[derive(Clone, Debug)]
pub struct ChainStateView {
    state: Arc<RwLock<ChainState>>,
}

impl ChainStateView {
    pub fn new(state: Arc<RwLock<ChainState>>) -> Self {
        Self { state }
    }

    pub async fn add_new_account(&self, address: Address) {
        let mut chain_state = self.state.write().await;

        if chain_state.accounts.contains_key(&address) {
            panic!(
                "attempted to add new account with address {address:?} when entry already existed"
            );
        }

        chain_state.new_accounts.insert(address);
    }

    pub async fn with_ro(&self, mut f: impl FnMut(RwLockReadGuard<'_, ChainState>)) {
        let chain_state = self.state.read().await;

        f(chain_state)
    }
}
