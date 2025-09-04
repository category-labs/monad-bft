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
    collections::{btree_map::Entry as BTreeMapEntry, VecDeque},
    marker::PhantomData,
};

use alloy_consensus::{
    constants::EMPTY_WITHDRAWALS,
    proofs::calculate_transaction_root,
    transaction::{Recovered, Transaction},
    TxEnvelope, EMPTY_OMMER_ROOT_HASH,
};
use alloy_rlp::Encodable;
use monad_chain_config::{
    revision::{ChainParams, ChainRevision},
    ChainConfig,
};
use monad_consensus_types::{
    block::{BlockPolicy, ConsensusBlockHeader, ConsensusFullBlock, TxnFee, TxnFees},
    block_validator::{BlockValidationError, BlockValidator},
    payload::ConsensusBlockBody,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_block_policy::{
    compute_max_txn_cost, compute_txn_max_gas_cost,
    nonce_usage::{NonceUsage, NonceUsageMap},
    validation::static_validate_transaction,
    EthBlockPolicy, EthValidatedBlock,
};
use monad_eth_types::{EthBlockBody, EthExecutionProtocol, ExtractEthAddress, ProposedEthHeader};
use monad_secp::RecoverableAddress;
use monad_state_backend::StateBackend;
use monad_system_calls::{
    validator::SystemTransactionValidator, SystemTransaction, SYSTEM_SENDER_ETH_ADDRESS,
};
use monad_types::Balance;
use monad_validator::signature_collection::{SignatureCollection, SignatureCollectionPubKeyType};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use tracing::{debug, trace, trace_span, warn};

type SystemTransactions = Vec<SystemTransaction>;
type ValidatedTxns = Vec<Recovered<TxEnvelope>>;

/// Validates transactions as valid Ethereum transactions and also validates that
/// the list of transactions will create a valid Ethereum block
pub struct EthBlockValidator<ST, SCT>(PhantomData<(ST, SCT)>)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>;

impl<ST, SCT> Default for EthBlockValidator<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn default() -> Self {
        Self(PhantomData)
    }
}

// FIXME: add specific error returns for the different failures
impl<ST, SCT, SBT, CCT, CRT>
    BlockValidator<ST, SCT, EthExecutionProtocol, EthBlockPolicy<ST, SCT, CCT, CRT>, SBT, CCT, CRT>
    for EthBlockValidator<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    SBT: StateBackend<ST, SCT>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
    CCT: ChainConfig<CRT>,
    CRT: ChainRevision,
{
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(seq_num = header.seq_num.as_u64())
    )]
    fn validate(
        &self,
        header: ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        body: ConsensusBlockBody<EthExecutionProtocol>,
        author_pubkey: Option<&SignatureCollectionPubKeyType<SCT>>,
        chain_config: &CCT,
    ) -> Result<
        <EthBlockPolicy<ST, SCT, CCT, CRT> as BlockPolicy<
            ST,
            SCT,
            EthExecutionProtocol,
            SBT,
            CCT,
            CRT,
        >>::ValidatedBlock,
        BlockValidationError,
    > {
        let chain_params = chain_config
            .get_chain_revision(header.block_round)
            .chain_params();

        Self::validate_block_header(&header, &body, author_pubkey, chain_params)?;

        if let Ok((system_txns, validated_txns, nonce_usages, txn_fees)) =
            Self::validate_block_body(&header, &body, chain_config)
        {
            let block = ConsensusFullBlock::new(header, body)?;
            Ok(EthValidatedBlock {
                block,
                system_txns,
                validated_txns,
                nonce_usages,
                txn_fees,
            })
        } else {
            Err(BlockValidationError::PayloadError)
        }
    }
}

impl<ST, SCT> EthBlockValidator<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    CertificateSignaturePubKey<ST>: ExtractEthAddress,
{
    fn validate_block_header(
        header: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        body: &ConsensusBlockBody<EthExecutionProtocol>,
        author_pubkey: Option<&SignatureCollectionPubKeyType<SCT>>,
        chain_params: &ChainParams,
    ) -> Result<(), BlockValidationError> {
        if header.block_body_id != body.get_id() {
            return Err(BlockValidationError::HeaderPayloadMismatchError);
        }

        if let Some(author_pubkey) = author_pubkey {
            if let Err(e) = header
                .round_signature
                .verify(header.block_round, author_pubkey)
            {
                warn!("Invalid randao_reveal signature, reason: {:?}", e);
                return Err(BlockValidationError::RandaoError);
            };
        }

        let ProposedEthHeader {
            ommers_hash,
            beneficiary: _,
            transactions_root,
            withdrawals_root,
            difficulty,
            number,
            gas_limit,
            timestamp,
            mix_hash,
            nonce,
            base_fee_per_gas,
            extra_data,
            blob_gas_used,
            excess_blob_gas,
            parent_beacon_block_root,
            requests_hash,
        } = &header.execution_inputs;

        if ommers_hash != EMPTY_OMMER_ROOT_HASH {
            return Err(BlockValidationError::HeaderError);
        }
        if transactions_root != calculate_transaction_root(&body.execution_body.transactions) {
            return Err(BlockValidationError::HeaderError);
        }
        if withdrawals_root != EMPTY_WITHDRAWALS {
            return Err(BlockValidationError::HeaderError);
        }
        if difficulty != &0 {
            return Err(BlockValidationError::HeaderError);
        }
        if number != &header.seq_num.0 {
            return Err(BlockValidationError::HeaderError);
        }
        if gas_limit != &chain_params.proposal_gas_limit {
            return Err(BlockValidationError::HeaderError);
        }
        if u128::from(*timestamp) != header.timestamp_ns / 1_000_000_000 {
            return Err(BlockValidationError::HeaderError);
        }
        if *mix_hash != header.round_signature.get_hash().0 {
            return Err(BlockValidationError::HeaderError);
        }
        if nonce != &[0_u8; 8] {
            return Err(BlockValidationError::HeaderError);
        }
        if extra_data != &[0_u8; 32] {
            return Err(BlockValidationError::HeaderError);
        }
        if blob_gas_used != &0 {
            return Err(BlockValidationError::HeaderError);
        }
        if excess_blob_gas != &0 {
            return Err(BlockValidationError::HeaderError);
        }
        if parent_beacon_block_root != &[0_u8; 32] {
            return Err(BlockValidationError::HeaderError);
        }
        if requests_hash != &[0_u8; 32] {
            return Err(BlockValidationError::HeaderError);
        }

        Ok(())
    }

    fn validate_block_body<CCT, CRT>(
        header: &ConsensusBlockHeader<ST, SCT, EthExecutionProtocol>,
        body: &ConsensusBlockBody<EthExecutionProtocol>,
        chain_config: &CCT,
    ) -> Result<(SystemTransactions, ValidatedTxns, NonceUsageMap, TxnFees), BlockValidationError>
    where
        CCT: ChainConfig<CRT>,
        CRT: ChainRevision,
    {
        let chain_params = chain_config
            .get_chain_revision(header.block_round)
            .chain_params();
        let chain_id = chain_config.chain_id();

        let execution_chain_params = {
            let timestamp_s: u64 = (header.timestamp_ns / 1_000_000_000)
                .try_into()
                // we don't assert because timestamp_ns is untrusted
                .unwrap_or(u64::MAX);

            chain_config
                .get_execution_chain_revision(timestamp_s)
                .execution_chain_params()
        };

        let EthBlockBody {
            transactions,
            ommers,
            withdrawals,
        } = &body.execution_body;

        if !ommers.is_empty() {
            return Err(BlockValidationError::PayloadError);
        }

        if !withdrawals.is_empty() {
            return Err(BlockValidationError::PayloadError);
        }

        // early return if number of transactions exceed limit
        // no need to individually validate transactions
        if transactions.len() > chain_params.tx_limit {
            return Err(BlockValidationError::TxnError);
        }

        // recovering the signers verifies that these are valid signatures
        let recovered_txns: VecDeque<Recovered<TxEnvelope>> = transactions
            .into_par_iter()
            .map(|tx| {
                let _span = trace_span!("validator: recover signer").entered();
                let signer = tx.secp256k1_recover()?;
                Ok(Recovered::new_unchecked(tx.clone(), signer))
            })
            .collect::<Result<_, monad_secp::Error>>()
            .map_err(|_err| BlockValidationError::TxnError)?;

        let (system_txns, eth_txns) =
            match SystemTransactionValidator::validate_and_extract_system_transactions(
                header,
                recovered_txns,
                chain_config,
            ) {
                Ok((system_txns, eth_txns)) => (system_txns, eth_txns),
                Err(err) => {
                    debug!(?err, "system transaction validator error");

                    return Err(BlockValidationError::SystemTxnError);
                }
            };

        let mut nonce_usages = NonceUsageMap::default();

        // duplicate check. this is also done in SystemTransactionValidator
        for sys_txn in &system_txns {
            let maybe_old_nonce_usage = nonce_usages.add_known(sys_txn.signer(), sys_txn.nonce());
            // A block is invalid if we see a smaller or equal nonce
            // after the first or if there is a nonce gap
            if let Some(old_nonce_usage) = maybe_old_nonce_usage {
                let Some(expected_nonce) = sys_txn.nonce().checked_sub(1) else {
                    return Err(BlockValidationError::SystemTxnError);
                };

                match old_nonce_usage {
                    NonceUsage::Known(old_nonce) => {
                        if expected_nonce != old_nonce {
                            return Err(BlockValidationError::SystemTxnError);
                        }
                    }
                    NonceUsage::Possible(_) => {}
                }
            }
        }

        let mut txn_fees: TxnFees = TxnFees::default();

        for eth_txn in eth_txns.iter() {
            if static_validate_transaction(eth_txn, chain_id, chain_params, execution_chain_params)
                .is_err()
            {
                return Err(BlockValidationError::TxnError);
            }

            if eth_txn.max_fee_per_gas() < header.base_fee.into() {
                return Err(BlockValidationError::TxnError);
            }

            let maybe_old_nonce_usage = nonce_usages.add_known(eth_txn.signer(), eth_txn.nonce());
            // txn iteration is following the same order as they are in the
            // block. A block is invalid if we see a smaller or equal nonce
            // after the first or if there is a nonce gap
            if let Some(old_nonce_usage) = maybe_old_nonce_usage {
                let Some(expected_nonce) = eth_txn.nonce().checked_sub(1) else {
                    return Err(BlockValidationError::TxnError);
                };

                match old_nonce_usage {
                    NonceUsage::Known(old_nonce) => {
                        if expected_nonce != old_nonce {
                            return Err(BlockValidationError::TxnError);
                        }
                    }
                    NonceUsage::Possible(_) => {}
                }
            }

            let txn_fee_entry = txn_fees
                .entry(eth_txn.signer())
                .and_modify(|e| {
                    e.max_gas_cost = e
                        .max_gas_cost
                        .saturating_add(compute_txn_max_gas_cost(eth_txn, header.base_fee));
                    e.max_txn_cost = e.max_txn_cost.saturating_add(compute_max_txn_cost(eth_txn));
                })
                .or_insert(TxnFee {
                    first_txn_value: eth_txn.value(),
                    first_txn_gas: compute_txn_max_gas_cost(eth_txn, header.base_fee),
                    max_gas_cost: Balance::ZERO,
                    max_txn_cost: compute_max_txn_cost(eth_txn),
                    is_delegated: false,
                });

            trace!(seq_num = ?header.seq_num, address = ?eth_txn.signer(), nonce = ?eth_txn.nonce(), ?txn_fee_entry, "TxnFeeEntry");

            if eth_txn.is_eip7702() {
                if let Some(auth_list) = eth_txn.authorization_list() {
                    for (authority, authorization) in auth_list.iter().flat_map(|authorization| {
                        authorization
                            .recover_authority()
                            .ok()
                            .map(|authority| (authority, authorization.inner()))
                    }) {
                        trace!(address =? authorization.address, nonce =? authorization.nonce, ?authority, "Signed authority");

                        // do not allow system account from sending authorization
                        if authority == SYSTEM_SENDER_ETH_ADDRESS {
                            return Err(BlockValidationError::TxnError);
                        }

                        if authorization.chain_id != 0_u64
                            && authorization.chain_id != chain_config.chain_id()
                        {
                            continue;
                        }

                        match nonce_usages.entry(authority) {
                            BTreeMapEntry::Occupied(nonce_usage) => match nonce_usage.into_mut() {
                                NonceUsage::Known(nonce) => {
                                    if *nonce + 1 == authorization.nonce {
                                        *nonce += 1;
                                    }
                                }
                                NonceUsage::Possible(possible_nonces) => {
                                    possible_nonces.push_front(authorization.nonce);
                                }
                            },
                            BTreeMapEntry::Vacant(nonce_usage) => {
                                nonce_usage.insert(NonceUsage::Possible(VecDeque::from_iter([
                                    authorization.nonce,
                                ])));
                            }
                        }

                        let txn_fee = txn_fees.entry(authority).or_default();
                        txn_fee.is_delegated = true;
                    }
                }
            }
        }

        let total_gas: u64 = eth_txns.iter().map(|tx| tx.gas_limit()).sum();
        let system_txns_size: usize = system_txns.iter().map(|tx| tx.length()).sum();
        let user_txns_size: usize = eth_txns.iter().map(|tx| tx.length()).sum();
        let proposal_size = system_txns_size + user_txns_size;
        debug!(
            total_gas,
            proposal_size,
            txs = transactions.len(),
            "proposal stats"
        );

        if total_gas > chain_params.proposal_gas_limit {
            return Err(BlockValidationError::TxnError);
        }

        if proposal_size as u64 > chain_params.proposal_byte_limit {
            return Err(BlockValidationError::TxnError);
        }

        Ok((system_txns, eth_txns, nonce_usages, txn_fees))
    }
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeMap, time::Duration};

    use alloy_consensus::Signed;
    use alloy_eips::eip7702::SignedAuthorization;
    use alloy_primitives::{Address, FixedBytes, PrimitiveSignature, B256, U256};
    use itertools::{FoldWhile, Itertools};
    use monad_chain_config::{
        revision::{ChainParams, MockChainRevision},
        MockChainConfig,
    };
    use monad_consensus_types::{
        payload::{ConsensusBlockBodyId, ConsensusBlockBodyInner, RoundSignature},
        quorum_certificate::QuorumCertificate,
    };
    use monad_crypto::{certificate_signature::CertificateKeyPair, NopKeyPair, NopSignature};
    use monad_eth_testutil::{
        compute_expected_nonce_usages, generate_consensus_test_block, make_eip7702_tx,
        make_legacy_tx, make_signed_authorization, recover_tx, secret_to_eth_address,
        ConsensusTestBlock,
    };
    use monad_state_backend::InMemoryStateInner;
    use monad_testutil::signing::MockSignatures;
    use monad_types::{Epoch, NodeId, Round, SeqNum, GENESIS_SEQ_NUM};
    use proptest::prelude::*;

    use super::*;

    const BASE_FEE: u128 = 100_000_000_000;
    const BASE_FEE_TREND: u64 = 0;
    const BASE_FEE_MOMENT: u64 = 0;

    const PROPOSAL_GAS_LIMIT: u64 = 300_000_000;
    const PROPOSAL_SIZE_LIMIT: u64 = 4_000_000;
    const MAX_RESERVE_BALANCE: u128 = 100_000_000_000_000_000_000;

    fn get_header(
        payload_id: ConsensusBlockBodyId,
    ) -> ConsensusBlockHeader<NopSignature, MockSignatures<NopSignature>, EthExecutionProtocol>
    {
        let nop_keypair = NopKeyPair::from_bytes(&mut [0_u8; 32]).unwrap();
        ConsensusBlockHeader::new(
            NodeId::new(nop_keypair.pubkey()),
            Epoch(1),
            Round(1),
            Vec::new(), // delayed_execution_results
            ProposedEthHeader::default(),
            payload_id,
            QuorumCertificate::genesis_qc(),
            GENESIS_SEQ_NUM + SeqNum(1),
            1,
            RoundSignature::new(Round(1), &nop_keypair),
            BASE_FEE as u64,
            BASE_FEE_TREND,
            BASE_FEE_MOMENT,
        )
    }

    #[test]
    fn test_invalid_block_with_nonce_gap() {
        // txn1 with nonce 1 while txn2 with nonce 3 (there is a nonce gap)
        let txn1 = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 1, 10);
        let txn2 = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 3, 10);

        // create a block with the above transactions
        let txs = vec![txn1, txn2];
        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: Vec::new(),
            },
        });
        let header = get_header(payload.get_id());

        // block validation should return error
        let result =
            EthBlockValidator::<NopSignature, MockSignatures<NopSignature>>::validate_block_body(
                &header,
                &payload,
                &MockChainConfig::DEFAULT,
            );
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_block_over_gas_limit() {
        // total gas used is 400_000_000 which is higher than block gas limit
        let txn1 = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 200_000_000, 1, 10);
        let txn2 = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 200_000_000, 2, 10);

        // create a block with the above transactions
        let txs = vec![txn1, txn2];
        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: Vec::new(),
            },
        });
        let header = get_header(payload.get_id());

        // block validation should return error
        let result =
            EthBlockValidator::<NopSignature, MockSignatures<NopSignature>>::validate_block_body(
                &header,
                &payload,
                &MockChainConfig::DEFAULT,
            );
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_block_over_tx_limit() {
        // tx limit per block is 1
        let txn1 = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 1, 10);
        let txn2 = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 2, 10);

        // create a block with the above transactions
        let txs = vec![txn1, txn2];
        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: Vec::new(),
            },
        });
        let header = get_header(payload.get_id());

        // block validation should return error
        let result =
            EthBlockValidator::<NopSignature, MockSignatures<NopSignature>>::validate_block_body(
                &header,
                &payload,
                &MockChainConfig::new(&ChainParams {
                    tx_limit: 1,
                    proposal_gas_limit: PROPOSAL_GAS_LIMIT,
                    proposal_byte_limit: PROPOSAL_SIZE_LIMIT,
                    max_reserve_balance: MAX_RESERVE_BALANCE,
                    vote_pace: Duration::ZERO,

                    validate_system_txs: true,
                    eip_7702: false,
                }),
            );
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_block_over_size_limit() {
        // proposal limit is 4MB
        let txn1 = make_legacy_tx(
            B256::repeat_byte(0xAu8),
            BASE_FEE,
            300_000_000,
            1,
            PROPOSAL_SIZE_LIMIT as usize,
        );

        // create a block with the above transactions
        let txs = vec![txn1];
        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: Vec::new(),
            },
        });
        let header = get_header(payload.get_id());

        // block validation should return error
        let result =
            EthBlockValidator::<NopSignature, MockSignatures<NopSignature>>::validate_block_body(
                &header,
                &payload,
                &MockChainConfig::DEFAULT,
            );
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    #[test]
    fn test_invalid_eip2_signature() {
        let valid_txn = make_legacy_tx(B256::repeat_byte(0xAu8), BASE_FEE, 30_000, 1, 10);

        // create a block with the above transaction
        let txs = vec![valid_txn.clone()];
        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: Vec::new(),
            },
        });
        let header = get_header(payload.get_id());

        // block validation should return Ok
        let result =
            EthBlockValidator::<NopSignature, MockSignatures<NopSignature>>::validate_block_body(
                &header,
                &payload,
                &MockChainConfig::DEFAULT,
            );
        assert!(result.is_ok());

        // ECDSA signature is malleable
        // given a signature, we can form a second signature by computing additive inverse of s and flips v
        let original_signature = valid_txn.signature();
        let secp256k1_n = U256::from_str_radix(
            "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141",
            16,
        )
        .unwrap();
        let new_s = secp256k1_n.saturating_sub(original_signature.s());

        // form the new signature and transaction
        let invalid_signature = PrimitiveSignature::from_scalars_and_parity(
            original_signature.r().into(),
            new_s.into(),
            !original_signature.v(),
        );
        let inner_tx = valid_txn.as_legacy().unwrap().tx();
        let invalid_txn: TxEnvelope =
            Signed::new_unchecked(inner_tx.clone(), invalid_signature, *valid_txn.tx_hash()).into();

        // both transactions recover to the same signer
        assert_eq!(
            valid_txn.recover_signer().unwrap(),
            invalid_txn.recover_signer().unwrap()
        );

        // create a block with the above transaction
        let txs = vec![invalid_txn];
        let payload = ConsensusBlockBody::new(ConsensusBlockBodyInner {
            execution_body: EthBlockBody {
                transactions: txs,
                ommers: Vec::new(),
                withdrawals: Vec::new(),
            },
        });
        let header = get_header(payload.get_id());

        // block validation should return Err
        let result =
            EthBlockValidator::<NopSignature, MockSignatures<NopSignature>>::validate_block_body(
                &header,
                &payload,
                &MockChainConfig::DEFAULT,
            );
        assert!(matches!(result, Err(BlockValidationError::TxnError)));
    }

    // TODO write tests for rest of eth-block-validator stuff

    prop_compose! {
        fn signed_authorization_strategy()(authority in 1..=4u8, address in 1..=4u8, nonce in 0..8u64)
        -> SignedAuthorization {
            // TODO(andr-dev): Make invalid chain id authorization
            make_signed_authorization(
                FixedBytes([authority; 32]),
                secret_to_eth_address(FixedBytes([address; 32])),
                nonce,
            )
        }
    }

    fn eip7702_tx_strategy(
        tx_signer: FixedBytes<32>,
        nonce: u64,
    ) -> impl Strategy<Value = Recovered<TxEnvelope>> {
        (1..=8usize)
            .prop_flat_map(|authorizations| {
                prop::collection::vec(signed_authorization_strategy(), authorizations)
            })
            .prop_map(move |authorization_list| {
                recover_tx(make_eip7702_tx(
                    tx_signer,
                    BASE_FEE,
                    0,
                    1_000_000,
                    nonce,
                    authorization_list,
                    0,
                ))
            })
    }

    fn block_with_eip7702_txs_strategy(
        tx_signer: FixedBytes<32>,
        starting_nonce: u64,
    ) -> impl Strategy<Value = ConsensusTestBlock<NopSignature, MockSignatures<NopSignature>>> {
        (1..=16u64)
            .prop_map(move |nonce_offset| starting_nonce + nonce_offset)
            .prop_flat_map(move |len| {
                (0..len)
                    .map(|nonce| eip7702_tx_strategy(tx_signer, nonce))
                    .collect::<Vec<_>>()
            })
            .prop_map(|txs| {
                generate_consensus_test_block(
                    Round(1),
                    SeqNum(1),
                    BASE_FEE.try_into().unwrap(),
                    &MockChainConfig::DEFAULT,
                    txs,
                )
            })
    }

    fn random_block_with_eip7702_txs_strategy(
    ) -> impl Strategy<Value = ConsensusTestBlock<NopSignature, MockSignatures<NopSignature>>> {
        (4..=5u8, 0..=8u64).prop_flat_map(|(signer, starting_nonce)| {
            block_with_eip7702_txs_strategy(FixedBytes([signer; 32]), starting_nonce)
        })
    }

    proptest! {
        #[test]
        fn proptest_validate_authorization_lists(block in random_block_with_eip7702_txs_strategy()) {
            let validator = EthBlockValidator::<NopSignature, MockSignatures<NopSignature>>::default();

            let (header, body) = block.block.split();

            let expect_success = block.validated_txns.iter().fold_while(Ok(BTreeMap::<Address, u64>::default()), |map, tx| {
                match map {
                    Err(()) => unreachable!(),
                    Ok(mut map) => {
                        match map.get_mut(tx.signer_ref()) {
                            None => {
                                map.insert(tx.signer(), tx.nonce() + 1);
                            },
                            Some(nonce) => {
                                if *nonce != tx.nonce() {
                                    return FoldWhile::Done(Err(()));
                                }

                                *nonce += 1;
                            }
                        }

                        for authorization in tx.authorization_list().into_iter().flatten() {
                            let authority = authorization.recover_authority().unwrap();

                            let Some(nonce) = map.get_mut(&authority) else {
                                continue;
                            };

                            if *nonce != authorization.nonce {
                                continue;
                            }

                            *nonce += 1;
                        }

                        FoldWhile::Continue(Ok(map))
                    }
                }
            }).into_inner().is_ok();

            let author = header.author.pubkey();

            let result = BlockValidator::<
                NopSignature,
                MockSignatures<NopSignature>,
                EthExecutionProtocol,
                EthBlockPolicy<_, _, _, _>,
                InMemoryStateInner<_, _>,
                MockChainConfig,
                MockChainRevision
            >::validate(&validator, header, body, Some(&author), &MockChainConfig::DEFAULT);

            match result {
                Err(error) => {
                    assert!(!expect_success, "EthBlockValidator failed when expected success, error: {error:?}");
                }
                Ok(block) => {
                    assert!(expect_success);

                    let expected_nonce_usages = compute_expected_nonce_usages(&block.validated_txns);

                    assert_eq!(block.nonce_usages, expected_nonce_usages);
                }
            }
        }
    }
}
