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

use crate::{
    ffi::{
        monad_c_address, monad_c_bytes32, monad_c_eth_account_state, monad_c_eth_txn_header,
        monad_c_eth_txn_receipt, monad_c_uint256_ne, monad_exec_block_start,
    },
    ExecutedStorageAccess, ExecutedTxnAccessListEntry, ExecutedTxnCallFrame, ExecutedTxnLog,
    ExecutedTxnSignedAuthorization,
};

#[derive(Debug)]
pub(super) struct BlockReassemblyState {
    pub start: monad_exec_block_start,
    pub txns: Box<[Option<TxnReassemblyState>]>,
}

#[derive(Debug)]
pub(super) struct TxnReassemblyState {
    pub hash: monad_c_bytes32,
    pub sender: monad_c_address,
    pub header: monad_c_eth_txn_header,
    pub input: Box<[u8]>,
    pub access_list: Vec<ExecutedTxnAccessListEntry>,
    pub authorization_list: Vec<ExecutedTxnSignedAuthorization>,
    pub output: Option<TxnOutputReassemblyState>,
}

#[derive(Debug)]
pub(super) struct TxnOutputReassemblyState {
    pub receipt: monad_c_eth_txn_receipt,
    pub logs: Box<[Option<ExecutedTxnLog>]>,
    pub call_frames: Option<Box<[Option<ExecutedTxnCallFrame>]>>,
    pub account_accesses: Option<Box<[Option<AccountAccessReassemblyState>]>>,
}

#[derive(Debug)]
pub(super) struct AccountAccessReassemblyState {
    pub address: monad_c_address,
    pub is_balance_modified: bool,
    pub is_nonce_modified: bool,
    pub prestate: monad_c_eth_account_state,
    pub modified_balance: monad_c_uint256_ne,
    pub modified_nonce: u64,
    pub storage_accesses: Box<[Option<ExecutedStorageAccess>]>,
    pub transient_accesses: Box<[Option<ExecutedStorageAccess>]>,
}
