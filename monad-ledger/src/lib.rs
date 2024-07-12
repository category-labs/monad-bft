use std::{
    fs::{self, File},
    io::{ErrorKind, Write},
    marker::PhantomData,
    ops::Deref,
    path::PathBuf,
};

use alloy_primitives::{Bloom, Bytes, FixedBytes, U256};
use alloy_rlp::Encodable;
use monad_blockdb::BlockDb;
use monad_consensus_types::{
    block::{Block as MonadBlock, BlockType},
    payload::{ExecutionArtifacts, FullTransactionList},
    signature_collection::SignatureCollection,
};
use monad_crypto::hasher::{Hasher, HasherType};
use monad_eth_tx::EthFullTransactionList;
use monad_executor::Executor;
use monad_executor_glue::ExecutionLedgerCommand;
use monad_proto::proto::block::ProtoBlock;
use monad_types::SeqNum;
use prost::Message;
use reth_primitives::{Block, BlockBody, Header};

/// Protocol parameters that go into Eth block header
pub struct EthHeaderParam {
    pub gas_limit: u64,
}

/// A ledger for committed Ethereum blocks
/// Blocks are RLP encoded and written to their own individual file, named by the block
/// number
pub struct MonadBlockFileLedger<SCT> {
    dir_path: PathBuf,
    blockdb: BlockDb,
    header_param: EthHeaderParam,
    phantom: PhantomData<SCT>,
}

impl<SCT> MonadBlockFileLedger<SCT>
where
    SCT: SignatureCollection + Clone,
{
    pub fn new(dir_path: PathBuf, blockdb: BlockDb, header_param: EthHeaderParam) -> Self {
        match fs::create_dir(&dir_path) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => panic!("{}", e),
        }
        Self {
            dir_path,
            blockdb,
            header_param,
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

    fn create_eth_block(&self, block: &MonadBlock<SCT>) -> Block {
        // use the full transactions to create the eth block body
        let block_body = generate_block_body(&block.payload.txns);

        // the payload inside the monad block will be used to generate the eth header
        let header = generate_header(&self.header_param, block, &block_body);

        let mut header_bytes = Vec::default();
        header.encode(&mut header_bytes);

        block_body.create_block(header)
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
                    let eth_blocks: Vec<Block> = full_blocks
                        .iter()
                        .map(|b| self.create_eth_block(b))
                        .collect();
                    let encoded_blocks: Vec<(SeqNum, Vec<u8>)> =
                        std::iter::zip(eth_blocks.iter(), full_blocks.iter())
                            .map(|(eth, monad)| (monad.get_seq_num(), encode_eth_block(eth)))
                            .collect();

                    let env = self.blockdb.clone();
                    tokio::task::spawn_blocking(move || {
                        for (eth_block, bft_block) in
                            std::iter::zip(eth_blocks.into_iter(), full_blocks.iter())
                        {
                            let bft_id = bft_block.get_id();
                            let pblock: ProtoBlock = bft_block.into();
                            let data = pblock.encode_to_vec();

                            env.write_eth_and_bft_blocks(eth_block, bft_id, &data);
                        }
                    });

                    for (seqnum, b) in encoded_blocks {
                        self.write_block(seqnum, &b).unwrap();
                    }
                }
            }
        }
    }
}

fn encode_eth_block(block: &Block) -> Vec<u8> {
    let mut buf = Vec::default();
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
    header_param: &EthHeaderParam,
    monad_block: &MonadBlock<SCT>,
    block_body: &BlockBody,
) -> Header {
    let ExecutionArtifacts {
        parent_hash: _,
        state_root,
        transactions_root,
        receipts_root,
        logs_bloom,
        gas_used,
    } = monad_block.payload.header;

    let mut randao_reveal_hasher = HasherType::new();

    randao_reveal_hasher.update(monad_block.payload.randao_reveal.0.clone());

    Header {
        parent_hash: monad_block.get_parent_id().0 .0.into(),
        ommers_hash: block_body.calculate_ommers_root(),
        beneficiary: monad_block.payload.beneficiary.0,
        state_root: FixedBytes(*state_root.deref()),
        transactions_root: FixedBytes(transactions_root.0),
        receipts_root: FixedBytes(receipts_root.0),
        withdrawals_root: block_body.calculate_withdrawals_root(),
        logs_bloom: Bloom(FixedBytes(logs_bloom.0)),
        difficulty: U256::ZERO,
        number: monad_block.payload.seq_num.0,
        gas_limit: header_param.gas_limit,
        gas_used: gas_used.0,
        // TODO-1: Add to BFT proposal
        timestamp: 0,
        mix_hash: randao_reveal_hasher.hash().0.into(),
        nonce: 0,
        // TODO: calculate base fee according to EIP1559
        base_fee_per_gas: Some(1000),
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None,
        extra_data: Bytes::default(),
    }
}
