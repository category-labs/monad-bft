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

use std::collections::{btree_map::Entry as BTreeMapEntry, BTreeMap, VecDeque};

use alloy_primitives::Address;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NonceUsage {
    Known(u64),
    Possible(VecDeque<u64>),
}

impl NonceUsage {
    pub fn merge_with_previous(&mut self, previous_nonce_usage: &Self) {
        match self {
            NonceUsage::Known(_) => {}
            NonceUsage::Possible(possible_nonces) => match previous_nonce_usage {
                NonceUsage::Known(previous_nonce) => {
                    let mut nonce = *previous_nonce;

                    for possible_nonce in possible_nonces {
                        if nonce + 1 == *possible_nonce {
                            nonce += 1;
                        }
                    }

                    *self = Self::Known(nonce);
                }
                NonceUsage::Possible(previous_possible_nonces) => {
                    for previous_possible_nonce in previous_possible_nonces.iter().rev() {
                        possible_nonces.push_front(*previous_possible_nonce);
                    }
                }
            },
        }
    }

    pub fn apply_to_account_nonce(&self, account_nonce: u64) -> u64 {
        match self {
            NonceUsage::Known(nonce) => *nonce + 1,
            NonceUsage::Possible(possible_nonces) => {
                Self::apply_possible_nonces_to_account_nonce(account_nonce, possible_nonces)
            }
        }
    }

    pub fn apply_possible_nonces_to_account_nonce(
        mut account_nonce: u64,
        possible_nonces: &VecDeque<u64>,
    ) -> u64 {
        for possible_nonce in possible_nonces {
            if *possible_nonce == account_nonce {
                account_nonce += 1
            }
        }

        account_nonce
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NonceUsageMap {
    pub map: BTreeMap<Address, NonceUsage>,
}

impl NonceUsageMap {
    pub fn get(&self, address: &Address) -> Option<&NonceUsage> {
        self.map.get(address)
    }

    pub fn add_known(&mut self, signer: Address, nonce: u64) -> Option<NonceUsage> {
        self.map.insert(signer, NonceUsage::Known(nonce))
    }

    pub fn entry(&mut self, address: Address) -> BTreeMapEntry<'_, Address, NonceUsage> {
        self.map.entry(address)
    }

    pub fn into_map(self) -> BTreeMap<Address, NonceUsage> {
        self.map
    }

    pub fn merge_with_previous_block<'a>(
        &mut self,
        previous_block_nonce_usages: impl Iterator<Item = (&'a Address, &'a NonceUsage)>,
    ) {
        for (address, previous_nonce_usage) in previous_block_nonce_usages {
            match self.map.entry(*address) {
                BTreeMapEntry::Vacant(v) => {
                    v.insert(previous_nonce_usage.to_owned());
                }
                BTreeMapEntry::Occupied(o) => {
                    o.into_mut().merge_with_previous(previous_nonce_usage);
                }
            }
        }
    }
}
