use alloy_consensus::TxEnvelope;
use alloy_primitives::Address;
use alloy_rlp::{RlpDecodable, RlpEncodable};
use monad_consensus_types::{
    block::ConsensusFullBlock,
    payload::{EthExecutionProtocol, Ommer, Withdrawal},
    signature_collection::SignatureCollection,
};
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};

#[derive(RlpEncodable, RlpDecodable)]
pub struct ExecutableBlockHeader {
    bft_block_id: [u8; 32],
    pub round: u64,
    pub parent_round: u64,

    beneficiary: Address,
    difficulty: u64,
    number: u64, // == seq_num
    gas_limit: u64,
    timestamp: u64,
    mix_hash: [u8; 32],
    nonce: [u8; 8],
    extra_data: [u8; 32],
    base_fee_per_gas: u64,
    // cancun
    withdrawals_root: [u8; 32],
    blob_gas_used: u64,
    excess_blob_gas: u64,
    parent_beacon_block_root: [u8; 32],
}

#[derive(RlpEncodable, RlpDecodable)]
pub struct ExecutableBlock {
    pub header: ExecutableBlockHeader,

    transactions: Vec<TxEnvelope>,
    ommers: Vec<Ommer>,
    withdrawals: Vec<Withdrawal>,
}

impl<ST, SCT> From<&ConsensusFullBlock<ST, SCT, EthExecutionProtocol>> for ExecutableBlock
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    fn from(full_block: &ConsensusFullBlock<ST, SCT, EthExecutionProtocol>) -> Self {
        Self {
            header: ExecutableBlockHeader {
                bft_block_id: full_block.get_id().0 .0,
                round: full_block.get_round().0,
                parent_round: full_block.get_parent_round().0,
                beneficiary: full_block.header().execution_inputs.beneficiary,
                difficulty: full_block.header().execution_inputs.difficulty,
                number: full_block.header().execution_inputs.number,
                gas_limit: full_block.header().execution_inputs.gas_limit,
                timestamp: full_block.header().execution_inputs.timestamp,
                mix_hash: full_block.header().execution_inputs.mix_hash,
                nonce: full_block.header().execution_inputs.nonce,
                extra_data: full_block.header().execution_inputs.extra_data,
                base_fee_per_gas: full_block.header().execution_inputs.base_fee_per_gas,
                withdrawals_root: full_block.header().execution_inputs.withdrawals_root,
                blob_gas_used: full_block.header().execution_inputs.blob_gas_used,
                excess_blob_gas: full_block.header().execution_inputs.excess_blob_gas,
                parent_beacon_block_root: full_block
                    .header()
                    .execution_inputs
                    .parent_beacon_block_root,
            },
            transactions: full_block.body().execution_body.transactions.clone(),
            ommers: full_block.body().execution_body.ommers.clone(),
            withdrawals: full_block.body().execution_body.withdrawals.clone(),
        }
    }
}

impl ExecutableBlock {
    pub fn bft_block_id(&self) -> &[u8; 32] {
        &self.header.bft_block_id
    }

    pub fn number(&self) -> u64 {
        self.header.number
    }

    pub fn num_tx(&self) -> usize {
        self.transactions.len()
    }

    pub fn transactions(&self) -> &Vec<TxEnvelope> {
        &self.transactions
    }
}

pub fn format_block_id(block_id: &[u8; 32]) -> String {
    hex::encode(block_id)
}
