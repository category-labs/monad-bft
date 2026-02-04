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
    path::Path,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering::SeqCst},
        Arc,
    },
};

use alloy_consensus::Header;
use alloy_primitives::Address;
use alloy_rlp::Decodable;
use futures::{channel::oneshot, executor::block_on, future::join_all, FutureExt};
use key::Version;
use monad_bls::{BlsPubKey, BlsSignatureCollection};
use monad_eth_types::{AccountCodeOrHash, EthAccount, EthHeader, EIP7702_DELEGATION_PREFIX};
use monad_secp::{PubKey, SecpSignature};
use monad_state_backend::{StateBackend, StateBackendError};
use monad_triedb::TriedbHandle;
use monad_types::{BlockId, Epoch, Hash, SeqNum, Stake};
use tracing::{debug, trace, warn};

use crate::{
    decode::rlp_decode_account,
    key::{create_triedb_key, KeyInput},
};

pub mod decode;
pub mod key;
pub mod mock_triedb;
pub mod triedb_env;

const MAX_TRIEDB_ASYNC_POLLS: usize = 640_000;
const RODB_NODE_LRU_MAX_MEM: u64 = 50 << 20; // 50 MB

pub struct TriedbReader {
    handle: TriedbHandle,
    state_backend_total_lookups: Arc<AtomicU64>,
}

impl TriedbReader {
    /// for debug only
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn handle(&self) -> &TriedbHandle {
        &self.handle
    }

    pub fn try_new(triedb_path: &Path) -> Option<Self> {
        TriedbHandle::try_new(triedb_path, RODB_NODE_LRU_MAX_MEM).map(|handle| Self {
            handle,
            state_backend_total_lookups: Default::default(),
        })
    }

    pub fn get_latest_voted_block(&self) -> Option<SeqNum> {
        let latest_voted = self.handle.latest_voted_block()?;
        Some(SeqNum(latest_voted))
    }

    pub fn get_latest_voted_block_id(&self) -> Option<BlockId> {
        let latest_voted = self.handle.latest_voted_block_id()?;
        Some(BlockId(Hash(latest_voted)))
    }

    pub fn get_latest_proposed_block(&self) -> Option<SeqNum> {
        let latest_proposed = self.handle.latest_proposed_block()?;
        Some(SeqNum(latest_proposed))
    }

    pub fn get_latest_proposed_block_id(&self) -> Option<BlockId> {
        let latest_proposed = self.handle.latest_proposed_block_id()?;
        Some(BlockId(Hash(latest_proposed)))
    }

    pub fn get_latest_finalized_block(&self) -> Option<SeqNum> {
        let latest_finalized = self.handle.latest_finalized_block()?;
        Some(SeqNum(latest_finalized))
    }

    pub fn get_earliest_finalized_block(&self) -> Option<SeqNum> {
        let earliest_finalized = self.handle.earliest_finalized_block()?;
        Some(SeqNum(earliest_finalized))
    }

    pub fn get_account_finalized(
        &self,
        seq_num: &SeqNum,
        eth_address: &[u8; 20],
    ) -> Option<EthAccount> {
        self.get_account(seq_num, Version::Finalized, eth_address)
    }

    pub fn get_finalized_eth_header(&self, seq_num: &SeqNum) -> Option<EthHeader> {
        if self
            .raw_read_latest_finalized_block()
            .is_some_and(|latest_finalized| seq_num <= &latest_finalized)
        {
            let (triedb_key, key_len_nibbles) =
                create_triedb_key(Version::Finalized, KeyInput::BlockHeader);
            let eth_header_bytes = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0)?;
            let mut rlp_buf = eth_header_bytes.as_slice();
            let block_header = Header::decode(&mut rlp_buf).expect("invalid rlp eth header");

            Some(EthHeader(block_header))
        } else {
            None
        }
    }

    pub fn get_proposed_eth_header(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
    ) -> Option<EthHeader> {
        let (triedb_key, key_len_nibbles) =
            create_triedb_key(Version::Proposal(*block_id), KeyInput::BlockHeader);
        let eth_header_bytes = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0)?;

        let mut rlp_buf = eth_header_bytes.as_slice();
        let block_header = Header::decode(&mut rlp_buf).expect("invalid rlp eth header");

        Some(EthHeader(block_header))
    }

    // for accessing Version::Proposed, bft_id MUST BE VERIFIED
    pub fn get_account(
        &self,
        seq_num: &SeqNum,
        version: Version,
        eth_address: &[u8; 20],
    ) -> Option<EthAccount> {
        let (triedb_key, key_len_nibbles) =
            create_triedb_key(version, KeyInput::Address(eth_address));

        let result = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0);

        let Some(account_rlp) = result else {
            debug!(?seq_num, ?eth_address, "account not found");
            return None;
        };

        rlp_decode_account(account_rlp)
    }

    pub fn get_accounts_async<'a>(
        &self,
        seq_num: &SeqNum,
        version: Version,
        eth_addresses: impl Iterator<Item = &'a Address>,
    ) -> Option<Vec<Option<EthAccount>>> {
        // Counter which is updated when TrieDB processes a single async read to completion
        let completed_counter = Arc::new(AtomicUsize::new(0));
        let mut num_accounts = 0;
        let eth_account_receivers = eth_addresses.map(|eth_address| {
            trace!(?eth_address, block_id = ?seq_num.0, "reading account code_or_hash");
            num_accounts += 1;
            let (triedb_key, key_len_nibbles) =
                create_triedb_key(version, KeyInput::Address(eth_address.as_ref()));
            let (sender, receiver) = oneshot::channel();
            self.handle.read_async(
                triedb_key.as_ref(),
                key_len_nibbles,
                seq_num.0,
                completed_counter.clone(),
                sender,
                Arc::new(()),
            );
            receiver.map(|receiver_result| {
                // Receiver should not fail
                let maybe_rlp_account = receiver_result.expect("receiver can't be canceled");
                // RLP decode the received account value
                let res = maybe_rlp_account.and_then(rlp_decode_account);
                match res {
                    Some(mut eth_account) => {
                        trace!(?eth_account, block_id = ?seq_num.0, "account code_hash");
                        match eth_account.code_or_hash {
                            AccountCodeOrHash::CodeHash(code_hash) => {
                                // Request code
                                let (triedb_key, key_len_nibbles) =
                                    create_triedb_key(version, KeyInput::CodeHash(&code_hash));
                                let res = self.handle.read(&triedb_key, key_len_nibbles, seq_num.0);
                                trace!(?res, block_id = ?seq_num.0, ?eth_account, "account code_data");
                                if let Some(data) = res {
                                    if data.len() == 23 {
                                        let delegation_code = &data[0..3];
                                        eth_account.is_delegated =
                                            delegation_code == EIP7702_DELEGATION_PREFIX;
                                        if eth_account.is_delegated {
                                            trace!(?eth_account, block_id = ?seq_num.0, "is_delegated == true");
                                        }
                                    }
                                }
                            },
                            AccountCodeOrHash::InlineCode(_) => {
                                eth_account.is_delegated = true;
                            }
                            AccountCodeOrHash::IsEmpty => {}
                        }
                        Some(eth_account)
                    }
                    None => None,
                }
            })
        });
        // Join all futures of receivers
        let eth_account_receivers = join_all(eth_account_receivers);
        let mut poll_count = 0;
        // Poll TrieDB until the completed_counter reaches num_accounts
        // TODO: if initiating the reads had an error, this call to triedb_poll will not terminate.
        //       wrap this loop in a timeout so consensus doesn't get stuck
        while completed_counter.load(SeqCst) < num_accounts && poll_count < MAX_TRIEDB_ASYNC_POLLS {
            // blocking = true => wait to do more work
            // max_completions = usize::MAX => process as many completions before returning
            poll_count += self.handle.triedb_poll(true, usize::MAX);
        }
        // TrieDB should have completed processing all the async callbacks at this point
        if completed_counter.load(SeqCst) != num_accounts {
            warn!(
                "TrieDB poll count went over limit for {} lookups",
                num_accounts
            );
            return None;
        }
        Some(block_on(eth_account_receivers))
    }
}

impl StateBackend<SecpSignature, BlsSignatureCollection<PubKey>> for TriedbReader {
    fn get_account_statuses<'a>(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
        eth_addresses: impl Iterator<Item = &'a Address>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        let statuses = if is_finalized
            && self
                .raw_read_latest_finalized_block()
                .is_some_and(|latest_finalized| seq_num <= &latest_finalized)
        {
            trace!(?seq_num, "triedb read finalized");
            // check finalized

            // block <= latest
            let Some(statuses) =
                self.get_accounts_async(seq_num, Version::Finalized, eth_addresses)
            else {
                return Err(StateBackendError::NotAvailableYet);
            };

            let earliest = self
                .raw_read_earliest_finalized_block()
                .expect("earliest must exist if latest does");
            if seq_num < &earliest {
                // block < earliest
                return Err(StateBackendError::NeverAvailable);
            }
            // block >= earliest
            statuses
        } else {
            trace!(?seq_num, ?block_id, "triedb read proposed");

            // make sure the block is committed before reading accounts
            let Some(_header) = self.get_proposed_eth_header(block_id, seq_num) else {
                return Err(StateBackendError::NotAvailableYet);
            };

            let Some(statuses) =
                self.get_accounts_async(seq_num, Version::Proposal(*block_id), eth_addresses)
            else {
                return Err(StateBackendError::NotAvailableYet);
            };
            statuses
        };

        self.state_backend_total_lookups
            .fetch_add(statuses.len() as u64, std::sync::atomic::Ordering::SeqCst);

        Ok(statuses)
    }

    fn get_execution_result(
        &self,
        block_id: &BlockId,
        seq_num: &SeqNum,
        is_finalized: bool,
    ) -> Result<EthHeader, StateBackendError> {
        if is_finalized
            && self
                .raw_read_latest_finalized_block()
                .is_some_and(|latest_finalized| seq_num <= &latest_finalized)
        {
            trace!(?seq_num, "triedb read eth header finalized");
            // check finalized

            // block <= latest
            let Some(header) = self.get_finalized_eth_header(seq_num) else {
                return Err(StateBackendError::NotAvailableYet);
            };
            Ok(header)
        } else {
            // check proposed
            trace!(?seq_num, ?block_id, "triedb read eth header proposed");

            let Some(header) = self.get_proposed_eth_header(block_id, seq_num) else {
                return Err(StateBackendError::NotAvailableYet);
            };
            Ok(header)
        }
    }

    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        self.get_earliest_finalized_block()
    }

    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        self.get_latest_finalized_block()
    }

    fn read_valset_at_block(
        &self,
        block_num: SeqNum,
        requested_epoch: Epoch,
    ) -> Vec<(PubKey, BlsPubKey, Stake)> {
        self.handle.read_valset_at_block(block_num, requested_epoch)
    }

    fn total_db_lookups(&self) -> u64 {
        self.state_backend_total_lookups
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}
