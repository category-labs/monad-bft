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

//! Single source of truth for BFT-archive key construction.
//!
//! Schema overview:
//! - `bft/ledger/headers/<shard>/<id>` — header per BlockId, sharded by first
//!   `HEX_SHARD_CHARS` of the hex id.
//! - `bft/ledger/bodies/<shard>/<id>` — body per body Hash, same sharding.
//! - `bft/index/<page>` — paged index. Page `p` covers seq_nums
//!   `[p * INDEX_PAGE_SIZE, (p+1) * INDEX_PAGE_SIZE)`. Each page is a
//!   contiguous blob of `INDEX_ENTRY_BYTES`-byte block ids.
//! - `bft/index_legacy_perkey/<seq_num>` — per-key index used only by the
//!   local migration backend; kept distinct from the paged path so the two
//!   schemes never collide.
//! - `bft/index_markers/<head_num>` — sub-chain progress markers used by the
//!   migration indexer.
//! - `bft/ledger/latest_uploaded` — single-key latest uploaded seq_num.
//! - `bft_block/<id>.{header,body}` — legacy unsharded source layout.

use monad_types::{BlockId, Hash};

pub const HEX_SHARD_CHARS: usize = 4;
pub const INDEX_PAGE_SIZE: u64 = 1_000;
pub const INDEX_ENTRY_BYTES: usize = 32;

const HEADERS_PREFIX: &str = "bft/ledger/headers/";
const BODIES_PREFIX: &str = "bft/ledger/bodies/";
const INDEX_PAGE_PREFIX: &str = "bft/index/";
const INDEX_PER_KEY_PREFIX: &str = "bft/index_legacy_perkey/";
const MARKERS_PREFIX: &str = "bft/index_markers/";
const LATEST_UPLOADED_KEY: &str = "bft/ledger/latest_uploaded";
const LEGACY_PREFIX: &str = "bft_block/";
const LEGACY_HEADER_SUFFIX: &str = ".header";
const LEGACY_BODY_SUFFIX: &str = ".body";

pub fn headers_prefix() -> &'static str {
    HEADERS_PREFIX
}

pub fn bodies_prefix() -> &'static str {
    BODIES_PREFIX
}

pub fn index_page_prefix() -> &'static str {
    INDEX_PAGE_PREFIX
}

pub fn index_per_key_prefix() -> &'static str {
    INDEX_PER_KEY_PREFIX
}

pub fn markers_prefix() -> &'static str {
    MARKERS_PREFIX
}

pub fn latest_uploaded_key() -> &'static str {
    LATEST_UPLOADED_KEY
}

pub fn legacy_prefix() -> &'static str {
    LEGACY_PREFIX
}

fn shard_for_hex(hex: &str) -> &str {
    let end = hex.len().min(HEX_SHARD_CHARS);
    &hex[..end]
}

pub fn header_path(id: &BlockId) -> String {
    let hex = hex::encode(id.0);
    format!("{HEADERS_PREFIX}{}/{hex}", shard_for_hex(&hex))
}

pub fn body_path(body_id: &Hash) -> String {
    let hex = hex::encode(body_id);
    format!("{BODIES_PREFIX}{}/{hex}", shard_for_hex(&hex))
}

pub fn index_page_for(seq_num: u64) -> u64 {
    seq_num / INDEX_PAGE_SIZE
}

pub fn index_page_offset(seq_num: u64) -> usize {
    ((seq_num % INDEX_PAGE_SIZE) as usize) * INDEX_ENTRY_BYTES
}

pub fn index_page_path(seq_num: u64) -> String {
    format!("{INDEX_PAGE_PREFIX}{}", index_page_for(seq_num))
}

pub fn index_page_path_for_page(page: u64) -> String {
    format!("{INDEX_PAGE_PREFIX}{page}")
}

pub fn index_per_key_path(seq_num: u64) -> String {
    format!("{INDEX_PER_KEY_PREFIX}{seq_num}")
}

pub fn marker_path(head_num: u64) -> String {
    format!("{MARKERS_PREFIX}{head_num}")
}

pub fn legacy_header_path(id: &BlockId) -> String {
    format!("{LEGACY_PREFIX}{}{LEGACY_HEADER_SUFFIX}", hex::encode(id.0))
}

pub fn legacy_body_path(body_id: &Hash) -> String {
    format!("{LEGACY_PREFIX}{}{LEGACY_BODY_SUFFIX}", hex::encode(body_id))
}

fn parse_hex_id(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 {
        return None;
    }
    let bytes = hex::decode(hex).ok()?;
    bytes.try_into().ok()
}

pub fn parse_legacy_header_path(key: &str) -> Option<BlockId> {
    let hex = key.strip_prefix(LEGACY_PREFIX)?.strip_suffix(LEGACY_HEADER_SUFFIX)?;
    Some(BlockId(Hash(parse_hex_id(hex)?)))
}

pub fn parse_legacy_body_path(key: &str) -> Option<Hash> {
    let hex = key.strip_prefix(LEGACY_PREFIX)?.strip_suffix(LEGACY_BODY_SUFFIX)?;
    Some(Hash(parse_hex_id(hex)?))
}

pub fn parse_index_per_key_path(key: &str) -> Option<u64> {
    key.strip_prefix(INDEX_PER_KEY_PREFIX)?.parse().ok()
}

pub fn parse_marker_path(key: &str) -> Option<u64> {
    key.strip_prefix(MARKERS_PREFIX)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_id(byte: u8) -> BlockId {
        BlockId(Hash([byte; 32]))
    }

    #[test]
    fn header_path_is_sharded() {
        let id = block_id(0xAB);
        let path = header_path(&id);
        let hex = hex::encode(id.0);
        assert_eq!(path, format!("bft/ledger/headers/{}/{hex}", &hex[..4]));
    }

    #[test]
    fn body_path_is_sharded() {
        let body = Hash([0x12; 32]);
        let path = body_path(&body);
        let hex = hex::encode(body);
        assert_eq!(path, format!("bft/ledger/bodies/{}/{hex}", &hex[..4]));
    }

    #[test]
    fn legacy_header_path_round_trip() {
        let id = block_id(0xCD);
        let key = legacy_header_path(&id);
        assert_eq!(parse_legacy_header_path(&key), Some(id));
    }

    #[test]
    fn legacy_body_path_round_trip() {
        let body = Hash([0xEF; 32]);
        let key = legacy_body_path(&body);
        assert_eq!(parse_legacy_body_path(&key), Some(body));
    }

    #[test]
    fn parse_legacy_rejects_wrong_suffix() {
        assert_eq!(parse_legacy_header_path("bft_block/00.body"), None);
        assert_eq!(parse_legacy_body_path("bft_block/00.header"), None);
    }

    #[test]
    fn index_page_math() {
        assert_eq!(index_page_for(0), 0);
        assert_eq!(index_page_for(999), 0);
        assert_eq!(index_page_for(1_000), 1);
        assert_eq!(index_page_for(1_999), 1);
        assert_eq!(index_page_offset(0), 0);
        assert_eq!(index_page_offset(1), INDEX_ENTRY_BYTES);
        assert_eq!(index_page_offset(999), 999 * INDEX_ENTRY_BYTES);
        assert_eq!(index_page_offset(1_000), 0);
    }

    #[test]
    fn index_page_path_aligns_with_page_for() {
        assert_eq!(index_page_path(0), "bft/index/0");
        assert_eq!(index_page_path(999), "bft/index/0");
        assert_eq!(index_page_path(1_000), "bft/index/1");
        assert_eq!(index_page_path_for_page(7), "bft/index/7");
    }

    #[test]
    fn index_per_key_path_round_trip() {
        let key = index_per_key_path(42);
        assert_eq!(key, "bft/index_legacy_perkey/42");
        assert_eq!(parse_index_per_key_path(&key), Some(42));
    }

    #[test]
    fn marker_path_round_trip() {
        let key = marker_path(123);
        assert_eq!(key, "bft/index_markers/123");
        assert_eq!(parse_marker_path(&key), Some(123));
    }
}
