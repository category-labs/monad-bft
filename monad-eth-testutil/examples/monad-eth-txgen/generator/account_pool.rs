use std::collections::HashMap;

use rand::{
    rngs::{StdRng, ThreadRng},
    Rng, SeedableRng,
};
use reth_primitives::Address;

use super::{account::EthAccount, config::EthTxAddressPoolConfig};
use crate::{account::PrivateKey, state::ChainStateView};

pub struct AccountPool {
    accounts: HashMap<Address, EthAccount>,
    from: Vec<Address>,
    to: Vec<Address>,
}

impl AccountPool {
    pub async fn new_with_config(
        from_config: EthTxAddressPoolConfig,
        to_config: EthTxAddressPoolConfig,
        chain_state: &ChainStateView,
    ) -> Self {
        let mut accounts = HashMap::default();

        let mut from = Vec::default();
        let mut to = Vec::default();

        Self::register_accounts(chain_state, &mut accounts, &mut from, from_config).await;
        Self::register_accounts(chain_state, &mut accounts, &mut to, to_config).await;

        Self { accounts, from, to }
    }

    async fn register_accounts(
        chain_state: &ChainStateView,
        accounts: &mut HashMap<Address, EthAccount>,
        addresses: &mut Vec<Address>,
        config: EthTxAddressPoolConfig,
    ) {
        let list = match config {
            EthTxAddressPoolConfig::Single { private_key } => {
                let (address, account) = PrivateKey::new(private_key);

                vec![(address, account)]
            }
            EthTxAddressPoolConfig::RandomSeeded { seed, count } => {
                let mut rng = StdRng::seed_from_u64(seed);

                (0..count)
                    .map(|_| PrivateKey::new_with_random(&mut rng))
                    .collect()
            }
        };

        for (address, account) in list {
            addresses.push(address);

            if accounts.insert(address, EthAccount::new(account)).is_none() {
                chain_state.add_new_account(address).await;
            }
        }
    }

    pub fn iter_random(
        &mut self,
        limit: usize,
        mut f: impl FnMut(Address, &mut EthAccount, Address) -> bool,
    ) {
        let mut from_generator = Self::generator(&self.from);
        let mut to_generator = Self::generator(&self.to);

        for _ in 0..limit {
            let Some(from) = from_generator() else {
                return;
            };

            let Some(to) = to_generator() else {
                return;
            };

            let from_account = self.accounts.get_mut(&from).expect("from account exists");

            if !f(from, from_account, to) {
                break;
            }
        }
    }

    fn generator<'a>(list: &'a [Address]) -> Box<dyn FnMut() -> Option<Address> + 'a> {
        let mut rng = ThreadRng::default();
        let mut idxs = (0..list.len()).collect::<Vec<_>>();

        Box::new(move || {
            if list.len() == 1 {
                return Some(list[0]);
            }

            if idxs.is_empty() {
                return None;
            }

            let idx_idx = rng.gen_range(0..idxs.len());
            let last_idx_idx = idxs.len() - 1;

            idxs.swap(idx_idx, last_idx_idx);

            let idx = idxs.pop().expect("last element exists");

            Some(list[idx])
        })
    }
}
