use std::{
    fs::{self, File},
    io::{ErrorKind, Write},
    marker::PhantomData,
    ops::Deref,
    path::PathBuf,
};

use alloy_primitives::{keccak256, Bloom, Bytes, FixedBytes, U256};
use alloy_rlp::Encodable;
use monad_consensus_types::{
    block::{Block as MonadBlock, BlockType},
    payload::{ExecutionArtifacts, FullTransactionList},
    signature_collection::SignatureCollection,
};
use monad_crypto::hasher::{Hasher, HasherType};
use monad_eth_tx::EthFullTransactionList;
use monad_executor::Executor;
use monad_executor_glue::ExecutionLedgerCommand;
use monad_types::SeqNum;
use reth_primitives::{BlockBody, Header};

/// A ledger for committed Ethereum blocks
/// Blocks are RLP encoded and written to a file which is read by Execution client
pub struct MonadFileLedger<SCT> {
    file: File,

    phantom: PhantomData<SCT>,
}

impl<SCT> Default for MonadFileLedger<SCT>
where
    SCT: SignatureCollection + Clone,
{
    fn default() -> Self {
        Self::new(
            tempfile::tempdir()
                .unwrap()
                .into_path()
                .join("monad_file_ledger"),
        )
    }
}

impl<SCT> MonadFileLedger<SCT>
where
    SCT: SignatureCollection + Clone,
{
    pub fn new(file_path: PathBuf) -> Self {
        Self {
            file: File::create(file_path).unwrap(),

            phantom: PhantomData,
        }
    }
}

impl<SCT> Executor for MonadFileLedger<SCT>
where
    SCT: SignatureCollection + Clone,
{
    type Command = ExecutionLedgerCommand<SCT>;

    fn replay(&mut self, mut commands: Vec<Self::Command>) {
        commands.retain(|cmd| match cmd {
            // we match on all commands to be explicit
            ExecutionLedgerCommand::LedgerCommit(..) => true,
        });
        self.exec(commands)
    }

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                ExecutionLedgerCommand::LedgerCommit(full_blocks) => {
                    for full_block in full_blocks {
                        self.file.write_all(&encode_full_block(full_block)).unwrap();
                    }
                }
            }
        }
    }
}

/// A ledger for committed Ethereum blocks
/// Blocks are RLP encoded and written to their own individual file, named by the block
/// number
pub struct MonadBlockFileLedger<SCT> {
    dir_path: PathBuf,
    phantom: PhantomData<SCT>,
}

impl<SCT> MonadBlockFileLedger<SCT>
where
    SCT: SignatureCollection + Clone,
{
    pub fn new(dir_path: PathBuf) -> Self {
        match fs::create_dir(&dir_path) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => panic!("{}", e),
        }
        Self {
            dir_path,
            phantom: PhantomData,
        }
    }

    fn write_block(&self, seq_num: SeqNum, buf: &[u8]) -> std::io::Result<()> {
        let mut file_path = PathBuf::from(&self.dir_path);
        file_path.push(format!("{}", seq_num.0));

        let mut f = File::create(file_path).unwrap();
        f.write_all(buf).unwrap();

        Ok(())
    }
}

impl<SCT> Executor for MonadBlockFileLedger<SCT>
where
    SCT: SignatureCollection + Clone,
{
    type Command = ExecutionLedgerCommand<SCT>;

    fn replay(&mut self, mut commands: Vec<Self::Command>) {
        commands.retain(|cmd| match cmd {
            // we match on all commands to be explicit
            ExecutionLedgerCommand::LedgerCommit(..) => true,
        });
        self.exec(commands)
    }

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                ExecutionLedgerCommand::LedgerCommit(full_blocks) => {
                    for full_block in full_blocks {
                        self.write_block(full_block.get_seq_num(), &encode_full_block(full_block))
                            .unwrap();
                    }
                }
            }
        }
    }
}

/// Create an RLP encoded Ethereum block from a Monad consensus block
fn encode_full_block<SCT: SignatureCollection>(block: MonadBlock<SCT>) -> Vec<u8> {
    // let (monad_block, monad_full_txs) = block.split();

    // use the full transactions to create the eth block body
    let block_body = generate_block_body(&block.payload.txns);

    // the payload inside the monad block will be used to generate the eth header
    let header = generate_header(block, &block_body);

    let mut header_bytes = Vec::default();
    header.encode(&mut header_bytes);

    let header_hash = keccak256(header_bytes);

    let block = block_body.create_block(header);

    let mut buf = Vec::from(header_hash.0);

    block.encode(&mut buf);

    buf
}

/// Produce the body of an Ethereum Block from a list of full transactions
fn generate_block_body(monad_full_txs: &FullTransactionList) -> BlockBody {
    let transactions = EthFullTransactionList::rlp_decode(monad_full_txs.bytes().clone())
        .unwrap()
        .0
        .into_iter()
        .map(|tx| tx.into_signed())
        .collect();

    BlockBody {
        transactions,
        ommers: Vec::default(),
        withdrawals: Some(Vec::default()),
    }
}

// TODO-2: Review integration with execution team
/// Use data from the MonadBlock to generate an Ethereum Header
fn generate_header<SCT: SignatureCollection>(
    monad_block: MonadBlock<SCT>,
    block_body: &BlockBody,
) -> Header {
    let ExecutionArtifacts {
        parent_hash,
        state_root,
        transactions_root,
        receipts_root,
        logs_bloom,
        gas_used,
    } = monad_block.payload.header;

    let mut randao_reveal_hasher = HasherType::new();

    randao_reveal_hasher.update(monad_block.payload.randao_reveal.0);

    Header {
        parent_hash: FixedBytes(*parent_hash.deref()),
        ommers_hash: block_body.calculate_ommers_root(),
        beneficiary: monad_block.payload.beneficiary.0,
        state_root: FixedBytes(*state_root.deref()),
        transactions_root: FixedBytes(transactions_root.0),
        receipts_root: FixedBytes(receipts_root.0),
        withdrawals_root: block_body.calculate_withdrawals_root(),
        logs_bloom: Bloom(FixedBytes(logs_bloom.0)),
        difficulty: U256::ZERO,
        number: monad_block.payload.seq_num.0,
        // TODO-1: need to get the actual sum gas limit from the list of transactions being used
        gas_limit: 15_000_000,
        gas_used: gas_used.0,
        // TODO-1: Add to BFT proposal
        timestamp: 0,
        mix_hash: randao_reveal_hasher.hash().0.into(),
        nonce: 0,
        base_fee_per_gas: None,
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None,
        extra_data: Bytes::default(),
    }
}

#[cfg(test)]
mod test {
    use monad_consensus_types::{
        block::Block,
        ledger::CommitResult,
        payload::{Bloom, ExecutionArtifacts, FullTransactionList, Gas, Payload, RandaoReveal},
        quorum_certificate::{QcInfo, QuorumCertificate},
        state_root_hash::StateRootHash,
        voting::{Vote, VoteInfo},
    };
    use monad_crypto::{
        certificate_signature::{CertificateKeyPair, CertificateSignature},
        hasher::Hash,
        NopSignature,
    };
    use monad_eth_types::{EthAddress, EMPTY_RLP_TX_LIST};
    use monad_multi_sig::MultiSig;
    use monad_types::{BlockId, NodeId, Round, SeqNum};

    use crate::encode_full_block;

    #[test]
    fn encode_full_block_header_hash() {
        let pubkey =
            <<NopSignature as CertificateSignature>::KeyPairType as CertificateKeyPair>::from_bytes(&mut [127; 32])
                .unwrap()
                .pubkey();

        let block = Block::<MultiSig<NopSignature>>::new(
            NodeId::new(pubkey),
            Round(0),
            &Payload {
                txns: FullTransactionList::new(vec![EMPTY_RLP_TX_LIST].into()),
                header: ExecutionArtifacts {
                    parent_hash: StateRootHash::default(),
                    state_root: StateRootHash::default(),
                    transactions_root: Hash::default(),
                    receipts_root: Hash::default(),
                    logs_bloom: Bloom::zero(),
                    gas_used: Gas::default(),
                },
                seq_num: SeqNum(0),
                beneficiary: EthAddress::default(),
                randao_reveal: RandaoReveal::default(),
            },
            &QuorumCertificate::new(
                QcInfo {
                    vote: Vote {
                        vote_info: VoteInfo {
                            id: BlockId(Hash([0x00_u8; 32])),
                            round: Round(0),
                            parent_id: BlockId(Hash([0x00_u8; 32])),
                            parent_round: Round(0),
                            seq_num: SeqNum(0),
                        },
                        ledger_commit_info: CommitResult::NoCommit,
                    },
                },
                MultiSig::default(),
            ),
        );

        let bytes = encode_full_block(block);

        // Check that encode_full_block starts with keccak header hash
        assert!(bytes.starts_with(&[
            0xb0, 0xc0, 0xc2, 0xe1, 0x0f, 0xcb, 0xe1, 0x51, 0xb7, 0x49, 0x98, 0x80, 0xdd, 0xfd,
            0x08, 0x2f, 0x65, 0x66, 0xa5, 0x07, 0xaf, 0x00, 0x15, 0xc1, 0xbd, 0xe7, 0x61, 0x00,
            0xbc, 0x39, 0x04, 0xc8,
        ]));
    }
}
