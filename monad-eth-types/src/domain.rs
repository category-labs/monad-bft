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

//! Private domain types. See docs/domains-protocol.md in monad-execution.

use alloy_primitives::{B256, U256};
use alloy_rlp::{RlpDecodable, RlpEncodable};

/// The low 16 bits of a domain chain id are the network chain id.
const DOMAIN_CHAIN_ID_SUFFIX_MASK: u64 = 0xffff;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct InvalidDomainChainId {
    pub chain_id: u64,
    pub network_chain_id: u64,
}

/// Classifies a chain id: `Ok(None)` for the network's own chain id,
/// `Ok(Some(chain_id))` for a private domain, an error otherwise.
pub fn domain_from_chain_id(
    chain_id: u64,
    network_chain_id: u64,
) -> Result<Option<u64>, InvalidDomainChainId> {
    assert!(
        network_chain_id <= DOMAIN_CHAIN_ID_SUFFIX_MASK,
        "domain chain id suffix requires network_chain_id <= 0xffff"
    );
    if chain_id == network_chain_id {
        return Ok(None);
    }
    if chain_id & DOMAIN_CHAIN_ID_SUFFIX_MASK != network_chain_id {
        return Err(InvalidDomainChainId {
            chain_id,
            network_chain_id,
        });
    }
    Ok(Some(chain_id))
}

/// Header of a private domain block as derived from one L1 block, before
/// execution. Execution stores it RLP encoded, one per domain, holding the
/// domain's latest block.
#[derive(Debug, Clone, PartialEq, Eq, RlpEncodable, RlpDecodable)]
pub struct ProposedDomainHeader {
    pub domain_chain_id: u64,
    /// Domain block height: 0 is genesis. Not the L1 block number.
    pub number: u64,
    pub l1_block_number: u64,
    pub l1_block_id: B256,
    /// Copied from the L1 block.
    pub timestamp: u64,
    pub mix_hash: B256,
    pub gas_limit: u64,
    pub base_fee_per_gas: U256,
    /// Ordered trie root of the block's transactions.
    pub transactions_root: B256,
}

#[cfg(test)]
mod test {
    use alloy_primitives::{b256, hex};
    use alloy_rlp::{Decodable, Encodable};

    use super::*;

    const NETWORK_CHAIN_ID: u64 = 10143;

    #[test]
    fn classifies_chain_ids() {
        let domain = (0x1234 << 16) | NETWORK_CHAIN_ID;
        assert_eq!(
            domain_from_chain_id(NETWORK_CHAIN_ID, NETWORK_CHAIN_ID),
            Ok(None)
        );
        assert_eq!(
            domain_from_chain_id(domain, NETWORK_CHAIN_ID),
            Ok(Some(domain))
        );
        assert!(domain_from_chain_id(domain + 1, NETWORK_CHAIN_ID).is_err());
    }

    /// The same vector as execution's test_domain_block_rlp.cpp, so both sides
    /// agree on the encoding.
    #[test]
    fn header_known_answer() {
        let header = ProposedDomainHeader {
            domain_chain_id: 0x510000279f,
            number: 7,
            l1_block_number: 1000,
            l1_block_id: B256::repeat_byte(0x11),
            timestamp: 1_700_000_000,
            mix_hash: B256::repeat_byte(0x22),
            gas_limit: 300_000_000,
            base_fee_per_gas: U256::from(100_000_000_000u64),
            transactions_root: b256!(
                "56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421"
            ),
        };
        let expected = hex!(
            "f87d85510000279f078203e8a011111111111111111111111111111111111111111111\
             11111111111111111111846553f100a022222222222222222222222222222222222222\
             222222222222222222222222228411e1a30085174876e800a056e81f171bcc55a6ff83\
             45e692c0f86e5b48e01b996cadc001622fb5e363b421"
        );

        let mut encoded = vec![];
        header.encode(&mut encoded);
        assert_eq!(encoded, expected);
        assert_eq!(
            ProposedDomainHeader::decode(&mut expected.as_slice()).unwrap(),
            header
        );
    }
}
