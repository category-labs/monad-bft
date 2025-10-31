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

use monad_consensus_types::nonce_usage::NonceUsageMap;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_validator::signature_collection::SignatureCollection;

use crate::EthValidatedBlock;

pub trait NonceUsageRetrievable {
    fn get_nonce_usages(&self) -> NonceUsageMap;
}

impl<ST, SCT> NonceUsageRetrievable for EthValidatedBlock<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn get_nonce_usages(&self) -> NonceUsageMap {
        self.nonce_usages.clone()
    }
}

impl<ST, SCT> NonceUsageRetrievable for Vec<&EthValidatedBlock<ST, SCT>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn get_nonce_usages(&self) -> NonceUsageMap {
        let mut blocks = self.iter().rev();

        let Some(mut nonce_usages) = blocks.next().map(|block| block.nonce_usages.clone()) else {
            return NonceUsageMap::default();
        };

        for block in blocks {
            nonce_usages.merge_with_previous_block(block.nonce_usages.map.iter());
        }

        nonce_usages
    }
}

#[cfg(test)]
mod test {
    use alloy_primitives::{Address, FixedBytes};
    use itertools::Itertools;
    use monad_consensus_types::nonce_usage::{NonceUsage, NonceUsageMap};
    use monad_crypto::NopSignature;
    use monad_testutil::signing::MockSignatures;
    use monad_types::{GENESIS_ROUND, GENESIS_SEQ_NUM};

    use crate::{nonce_usage::NonceUsageRetrievable, test::make_test_block, EthValidatedBlock};

    type SignatureType = NopSignature;
    type SignatureCollectionType = MockSignatures<SignatureType>;

    use proptest::prelude::*;

    fn nonce_usage_strategy() -> impl Strategy<Value = NonceUsage> {
        prop_oneof![
            (0..64u64).prop_map(NonceUsage::Known),
            prop::collection::vec_deque(0..64u64, 0..8).prop_map(NonceUsage::Possible)
        ]
    }

    proptest! {
        #[test]
        fn proptest_nonce_usage_merge_with_previous(
            nonce_usage_previous in nonce_usage_strategy(),
            nonce_usage in nonce_usage_strategy()
        ) {
            let mut new_nonce_usage= nonce_usage.clone();

            new_nonce_usage.merge_with_previous(&nonce_usage_previous);

            match nonce_usage {
                NonceUsage::Known(nonce) => assert_eq!(new_nonce_usage, NonceUsage::Known(nonce)),
                NonceUsage::Possible(possible_nonces) => match nonce_usage_previous {
                    NonceUsage::Known(previous_nonce) => {
                        assert_eq!(
                            new_nonce_usage,
                            NonceUsage::Known(
                                possible_nonces.iter().fold(previous_nonce, |nonce, possible_nonce| {
                                    if nonce + 1 == *possible_nonce {
                                        nonce + 1
                                    } else {
                                        nonce
                                    }
                                })
                            )
                        );
                    },
                    NonceUsage::Possible(previous_possible_nonces) => {
                        assert_eq!(
                            new_nonce_usage,
                            NonceUsage::Possible(previous_possible_nonces.into_iter().chain(possible_nonces.into_iter()).collect())
                        );
                    },
                },
            }
        }
    }

    proptest! {
        #[test]
        fn proptest_nonce_usage_apply_to_account_nonce(
            account_nonce in 0..64u64,
            nonce_usage in nonce_usage_strategy()
        ) {
            let new_account_nonce = nonce_usage.apply_to_account_nonce(account_nonce);

            match nonce_usage {
                NonceUsage::Known(nonce) => assert_eq!(new_account_nonce, nonce + 1),
                NonceUsage::Possible(possible_nonces) => {
                    assert_eq!(
                        new_account_nonce,
                        possible_nonces.iter().fold(account_nonce, |account_nonce, possible_nonce| {
                            if account_nonce == *possible_nonce {
                                account_nonce + 1
                            } else {
                                account_nonce
                            }
                        })
                    )
                },
            }
        }
    }

    fn nonce_usage_map_strategy() -> impl Strategy<Value = NonceUsageMap> {
        prop::collection::btree_map(
            any::<u8>().prop_map(|b| Address(FixedBytes([b; 20]))),
            nonce_usage_strategy(),
            0..8,
        )
        .prop_map(|map| NonceUsageMap { map })
    }

    proptest! {
        #[test]
        fn proptest_nonce_usage_map_merge_with_previous_block(
            nonce_usage_map_previous in nonce_usage_map_strategy(),
            nonce_usage_map in nonce_usage_map_strategy()
        ) {
            let mut new_nonce_usage_map = nonce_usage_map.clone();

            new_nonce_usage_map.merge_with_previous_block(nonce_usage_map_previous.map.iter());

            for address in nonce_usage_map_previous.map.keys().chain(nonce_usage_map.map.keys()).unique() {
                let expected = match nonce_usage_map.get(address).cloned() {
                    None => nonce_usage_map_previous.get(address).unwrap().clone(),

                    Some(NonceUsage::Known(nonce)) => NonceUsage::Known(nonce),

                    Some(NonceUsage::Possible(possible_nonces)) => match nonce_usage_map_previous.get(address) {
                        None => NonceUsage::Possible(possible_nonces),

                        Some(previous_nonce_usage) => {
                            let mut nonce_usage = NonceUsage::Possible(possible_nonces);
                            nonce_usage.merge_with_previous(previous_nonce_usage);
                            nonce_usage
                        },
                    }
                };

                assert_eq!(new_nonce_usage_map.get(address).unwrap(), &expected);
            }
        }
    }

    #[test]
    fn test_nonce_usage_retrievable_empty() {
        assert!(
            Vec::<&EthValidatedBlock<SignatureType, SignatureCollectionType>>::default()
                .get_nonce_usages()
                .map
                .is_empty()
        );

        assert!(
            vec![&make_test_block(GENESIS_ROUND, GENESIS_SEQ_NUM, vec![])]
                .get_nonce_usages()
                .map
                .is_empty()
        );
    }

    // TODO(andr-dev): Add prop tests for NonceUsageRetrievable non-empty vec
}
