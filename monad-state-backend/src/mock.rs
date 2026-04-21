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

use std::collections::BTreeMap;

use alloy_consensus::Header;
use alloy_primitives::Address;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::{AccountKey, EthAccount, EthHeader};
use monad_types::{Balance, BlockId, Nonce, SeqNum, Stake};
use monad_validator::signature_collection::{SignatureCollection, SignatureCollectionPubKeyType};

use crate::{StateBackend, StateBackendError};

#[derive(Debug, Default, Clone)]
pub struct NopStateBackend {
    pub nonces: BTreeMap<Address, Nonce>,
    pub balances: BTreeMap<Address, Balance>,
}

impl<ST, SCT> StateBackend<ST, SCT> for NopStateBackend
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn get_account_statuses<'a>(
        &self,
        _block_id: &BlockId,
        _seq_num: &SeqNum,
        _is_finalized: bool,
        account_keys: impl Iterator<Item = &'a AccountKey>,
    ) -> Result<Vec<Option<EthAccount>>, StateBackendError> {
        Ok(account_keys
            .map(|account_key| {
                Some(EthAccount {
                    balance: account_key
                        .namespace
                        .is_none()
                        .then(|| self.balances.get(&account_key.address).cloned())
                        .flatten()
                        .unwrap_or_default(),
                    nonce: account_key
                        .namespace
                        .is_none()
                        .then(|| self.nonces.get(&account_key.address).cloned())
                        .flatten()
                        .unwrap_or_default(),
                    code_hash: None,
                    is_delegated: false,
                })
            })
            .collect())
    }

    fn get_execution_result(
        &self,
        _block_id: &BlockId,
        _seq_num: &SeqNum,
        _is_finalized: bool,
    ) -> Result<EthHeader, StateBackendError> {
        Ok(EthHeader(Header::default()))
    }

    /// Fetches earliest block from storage backend
    fn raw_read_earliest_finalized_block(&self) -> Option<SeqNum> {
        None
    }

    /// Fetches latest block from storage backend
    fn raw_read_latest_finalized_block(&self) -> Option<SeqNum> {
        None
    }

    fn read_valset_at_block(
        &self,
        block_num: SeqNum,
        requested_epoch: monad_types::Epoch,
    ) -> Vec<(
        <SCT as SignatureCollection>::NodeIdPubKey,
        SignatureCollectionPubKeyType<SCT>,
        Stake,
    )> {
        vec![]
    }

    fn total_db_lookups(&self) -> u64 {
        0
    }
}
