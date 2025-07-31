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
        monad_c_address, monad_c_bytes32, monad_c_eth_txn_header, monad_exec_block_start,
        monad_exec_txn_evm_output,
    },
    ExecutedTxnCallFrame, ExecutedTxnLog,
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
    pub logs: Vec<ExecutedTxnLog>,
    pub output: Option<monad_exec_txn_evm_output>,
    pub call_frames: Option<Vec<ExecutedTxnCallFrame>>,
}
