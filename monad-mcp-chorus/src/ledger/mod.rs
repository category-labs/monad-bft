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

//! The finalized-block ledger and the tx format carried in mini-proposal
//! payloads. Env-independent: ids and roots are plain ints/bytes and the
//! commit proof is opaque bytes.

pub mod block;
mod json;
pub mod store;
pub mod tx;

pub use block::{
    BlockMeta, CommittedLane, FinalizationPath, LEDGER_VERSION, LaneMeta, LaneRoot, NewBlock,
    NewLane, lane_json,
};
pub use store::{
    BLOCKS_DIR, LedgerError, LedgerFollower, LedgerReader, LedgerWriter, META_FILE, META_JSON_FILE,
    PROOF_FILE, Since, block_dir_name, lane_file_name, lane_json_file_name, parse_block_dir_name,
};
// demo(tx-timeline): exports decode_sealed_batch
pub use tx::{
    Address, BatchBuilder, Hash, MAX_TX_PAYLOAD, Tx, TxError, decode_batch, decode_sealed_batch,
    encode_batch, keccak256, payload_hash, tx_hash,
};
