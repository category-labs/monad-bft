use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs::{self, File},
    io::{ErrorKind, Write},
    marker::PhantomData,
    ops::{Deref, Div},
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

use alloy_primitives::{Bloom, FixedBytes, U256};
use alloy_rlp::{Decodable, Encodable};
use futures::Stream;
use monad_block_persist::{BlockPersist, FileBlockPersist};
use monad_blocksync::messages::message::{
    BlockSyncHeadersResponse, BlockSyncPayloadResponse, BlockSyncResponseMessage,
};
use monad_consensus_types::{
    block::{Block, BlockRange, BlockType, FullBlock as MonadBlock},
    payload::{ExecutionProtocol, FullTransactionList, Payload, PayloadId, TransactionPayload},
    quorum_certificate::GENESIS_BLOCK_ID,
    signature_collection::SignatureCollection,
};
use monad_crypto::{
    certificate_signature::{CertificateSignaturePubKey, CertificateSignatureRecoverable},
    hasher::{Hasher, HasherType},
};
use monad_eth_types::{EthSignedTransaction, BASE_FEE_PER_GAS};
use monad_executor::{Executor, ExecutorMetrics, ExecutorMetricsChain};
use monad_executor_glue::{BlockSyncEvent, LedgerCommand, MonadEvent};
use monad_types::{BlockId, Round, SeqNum};
use reth_primitives::{Block as EthBlock, BlockBody, Header};
use tracing::{info, trace};

/// Protocol parameters that go into Eth block header
pub struct EthHeaderParam {
    pub gas_limit: u64,
}

/// A ledger for committed Ethereum blocks
/// Blocks are RLP encoded and written to their own individual file, named by the block
/// number
pub struct MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    eth_block_path: PathBuf,
    bft_block_persist: FileBlockPersist<ST, SCT>,
    header_param: EthHeaderParam,

    metrics: ExecutorMetrics,
    last_commit: Option<SeqNum>,

    block_cache_size: usize,
    block_header_cache: HashMap<BlockId, Block<SCT>>,
    block_payload_cache: HashMap<PayloadId, Payload>,
    block_cache_index: BTreeMap<Round, (BlockId, PayloadId)>,

    fetches_tx: tokio::sync::mpsc::UnboundedSender<BlockSyncResponseMessage<SCT>>,
    fetches: tokio::sync::mpsc::UnboundedReceiver<BlockSyncResponseMessage<SCT>>,

    phantom: PhantomData<ST>,
}

const GAUGE_EXECUTION_LEDGER_NUM_COMMITS: &str = "monad.execution_ledger.num_commits";
const GAUGE_EXECUTION_LEDGER_NUM_TX_COMMITS: &str = "monad.execution_ledger.num_tx_commits";
const GAUGE_EXECUTION_LEDGER_BLOCK_NUM: &str = "monad.execution_ledger.block_num";

impl<ST, SCT> MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    pub fn new(
        eth_block_path: PathBuf,
        bft_block_path: PathBuf,
        payload_path: PathBuf,
        header_param: EthHeaderParam,
    ) -> Self {
        match fs::create_dir(&eth_block_path) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => panic!("{}", e),
        }
        match fs::create_dir(&bft_block_path) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => panic!("{}", e),
        }
        match fs::create_dir(&payload_path) {
            Ok(_) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => panic!("{}", e),
        }
        let (fetches_tx, fetches) = tokio::sync::mpsc::unbounded_channel();
        Self {
            eth_block_path: eth_block_path.clone(),
            bft_block_persist: FileBlockPersist::new(bft_block_path, payload_path, eth_block_path),
            header_param,

            metrics: Default::default(),
            last_commit: Default::default(),

            block_cache_size: 100, // TODO configurable
            block_header_cache: Default::default(),
            block_payload_cache: Default::default(),
            block_cache_index: Default::default(),

            fetches_tx,
            fetches,

            phantom: PhantomData,
        }
    }

    pub fn last_commit(&self) -> Option<SeqNum> {
        self.last_commit
    }

    fn update_cache(&mut self, monad_block: MonadBlock<SCT>) {
        let block_id = monad_block.get_id();
        let payload_id = monad_block.get_payload_id();
        let block_round = monad_block.get_round();

        self.block_header_cache.insert(block_id, monad_block.block);
        self.block_payload_cache
            .insert(payload_id, monad_block.payload);
        self.block_cache_index
            .insert(block_round, (block_id, payload_id));

        if self.block_cache_index.len() > self.block_cache_size {
            let (_, (block_id, payload_id)) = self.block_cache_index.pop_first().expect("nonempty");
            self.block_header_cache.remove(&block_id);
            self.block_payload_cache.remove(&payload_id);
        }
    }

    fn write_eth_block(&self, seq_num: SeqNum, buf: &[u8]) -> std::io::Result<()> {
        let mut file_path = PathBuf::from(&self.eth_block_path);
        file_path.push(format!("{}", seq_num.0));

        let mut f = File::create(file_path).unwrap();
        f.write_all(buf).unwrap();

        Ok(())
    }

    fn create_eth_block(&mut self, block: &MonadBlock<SCT>) -> EthBlock {
        assert!(!block.is_empty_block());
        if let TransactionPayload::List(txns) = &block.payload.txns {
            // use the full transactions to create the eth block body
            let block_body = generate_block_body(txns);

            // the payload inside the monad block will be used to generate the eth header

            let header = generate_header(&self.header_param, block, &block_body);

            let mut header_bytes = Vec::default();
            header.encode(&mut header_bytes);

            block_body.create_block(header)
        } else {
            unreachable!()
        }
    }

    fn encode_eth_blocks(
        &self,
        full_blocks: &[(SeqNum, Option<EthBlock>, MonadBlock<SCT>)],
    ) -> Vec<(SeqNum, Vec<u8>)> {
        full_blocks
            .iter()
            .filter_map(|(seqnum, maybe_eth_block, _)| {
                maybe_eth_block
                    .as_ref()
                    .map(|eth_block| (*seqnum, encode_eth_block(eth_block)))
            })
            .collect()
    }

    fn create_and_zip_with_eth_blocks(
        &mut self,
        full_bft_blocks: Vec<MonadBlock<SCT>>,
    ) -> Vec<(SeqNum, Option<EthBlock>, MonadBlock<SCT>)> {
        full_bft_blocks
            .into_iter()
            .map(|b| {
                if b.is_empty_block() {
                    (b.get_seq_num(), None, b)
                } else {
                    self.update_cache(b.clone());
                    (b.get_seq_num(), Some(self.create_eth_block(&b)), b)
                }
            })
            .collect()
    }

    fn write_bft_blocks(&self, full_blocks: &Vec<(SeqNum, Option<EthBlock>, MonadBlock<SCT>)>) {
        // unwrap because failure to persist a finalized block is fatal error
        for (_, _, bft_full_block) in full_blocks {
            // write payload first so that header always points to payload that exists
            self.bft_block_persist
                .write_bft_payload(&bft_full_block.payload)
                .unwrap();
            self.bft_block_persist
                .write_bft_block(&bft_full_block.block)
                .unwrap();
        }
    }

    fn trace_and_metrics(
        &mut self,
        full_blocks: &Vec<(SeqNum, Option<EthBlock>, MonadBlock<SCT>)>,
    ) {
        for block in full_blocks {
            if let (_, Some(eth_block), _) = block {
                self.metrics[GAUGE_EXECUTION_LEDGER_NUM_COMMITS] += 1;
                self.metrics[GAUGE_EXECUTION_LEDGER_NUM_TX_COMMITS] += eth_block.body.len() as u64;
                self.metrics[GAUGE_EXECUTION_LEDGER_BLOCK_NUM] = eth_block.number;
                info!(
                    num_tx = eth_block.body.len(),
                    block_num = eth_block.number,
                    "committed block"
                );

                for t in &eth_block.body {
                    trace!(txn_hash = ?t.hash(), "txn committed");
                }
            }
        }
    }

    fn ledger_fetch_headers(&self, block_range: BlockRange) -> BlockSyncHeadersResponse<SCT> {
        let mut next_block_id = block_range.last_block_id;

        let mut headers = VecDeque::new();
        loop {
            // TODO add max number of headers to read
            let block_header =
                if let Some(cached_header) = self.block_header_cache.get(&next_block_id) {
                    cached_header.clone()
                } else if let Ok(block) = self.bft_block_persist.read_bft_block(&next_block_id) {
                    block
                } else {
                    trace!(?block_range, "requested headers not available in ledger");
                    return BlockSyncHeadersResponse::NotAvailable(block_range);
                };

            if block_header.get_seq_num() < block_range.root_seq_num {
                // if headers is empty here, then block range is invalid
                break;
            }

            next_block_id = block_header.get_parent_id();
            headers.push_front(block_header);

            if next_block_id == GENESIS_BLOCK_ID {
                // don't try fetching genesis block
                break;
            }
        }

        trace!(?block_range, "found requested headers in ledger");
        BlockSyncHeadersResponse::Found((block_range, headers.into()))
    }

    fn ledger_fetch_payload(&self, payload_id: PayloadId) -> BlockSyncPayloadResponse {
        if let Some(cached_payload) = self.block_payload_cache.get(&payload_id) {
            // payload in cache
            trace!(?payload_id, "found requested payload in ledger cache");
            BlockSyncPayloadResponse::Found(cached_payload.clone())
        } else if let Ok(payload) = self.bft_block_persist.read_bft_payload(&payload_id) {
            // payload read from block persist
            trace!(
                ?payload_id,
                "found requested payload in ledger blockpersist"
            );
            BlockSyncPayloadResponse::Found(payload)
        } else {
            trace!(?payload_id, "requested payload not available in ledger");
            BlockSyncPayloadResponse::NotAvailable(payload_id)
        }
    }
}

impl<ST, SCT> Executor for MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Command = LedgerCommand<SCT>;

    fn exec(&mut self, commands: Vec<Self::Command>) {
        for command in commands {
            match command {
                LedgerCommand::LedgerCommit(full_blocks) => {
                    let full_blocks = self.create_and_zip_with_eth_blocks(full_blocks);
                    self.trace_and_metrics(&full_blocks);

                    let encoded_eth_blocks = self.encode_eth_blocks(&full_blocks);
                    // this can panic because failure to persist a finalized block is fatal error
                    self.write_bft_blocks(&full_blocks);

                    for (seqnum, b) in encoded_eth_blocks {
                        self.write_eth_block(seqnum, &b).unwrap();
                        self.last_commit = Some(seqnum);
                    }

                    for (_, _, full_block) in full_blocks {
                        self.update_cache(full_block);
                    }
                }
                LedgerCommand::LedgerFetchHeaders(block_range) => {
                    // TODO cap max concurrent LedgerFetch? DOS vector
                    let fetches_tx = self.fetches_tx.clone();
                    let response = BlockSyncResponseMessage::HeadersResponse(
                        self.ledger_fetch_headers(block_range),
                    );
                    fetches_tx
                        .send(response)
                        .expect("failed to write to fetches_tx");
                }
                LedgerCommand::LedgerFetchPayload(payload_id) => {
                    let fetches_tx = self.fetches_tx.clone();
                    let response = BlockSyncResponseMessage::PayloadResponse(
                        self.ledger_fetch_payload(payload_id),
                    );
                    fetches_tx
                        .send(response)
                        .expect("failed to write to fetches_tx");
                }
            }
        }
    }

    fn metrics(&self) -> ExecutorMetricsChain {
        self.metrics.as_ref().into()
    }
}

impl<ST, SCT> Stream for MonadBlockFileLedger<ST, SCT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    type Item = MonadEvent<ST, SCT>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.fetches.poll_recv(cx).map(|response| {
            let response = response.expect("fetches_tx never dropped");
            Some(MonadEvent::BlockSyncEvent(BlockSyncEvent::SelfResponse {
                response,
            }))
        })
    }
}

fn encode_eth_block(block: &EthBlock) -> Vec<u8> {
    let mut buf = Vec::default();
    block.encode(&mut buf);
    buf
}

/// Produce the body of an Ethereum Block from a list of full transactions
fn generate_block_body(monad_full_txs: &FullTransactionList) -> BlockBody {
    let transactions =
        Vec::<EthSignedTransaction>::decode(&mut monad_full_txs.bytes().as_ref()).unwrap();

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
    let ExecutionProtocol {
        state_root,
        seq_num,
        beneficiary,
        randao_reveal,
    } = monad_block.block.execution.clone();

    let mut randao_reveal_hasher = HasherType::new();
    randao_reveal_hasher.update(randao_reveal);

    Header {
        parent_hash: monad_block.get_parent_id().0 .0.into(),
        ommers_hash: block_body.calculate_ommers_root(),
        beneficiary: beneficiary.0,
        state_root: FixedBytes(*state_root.deref()),
        transactions_root: FixedBytes::default(),
        receipts_root: FixedBytes::default(),
        withdrawals_root: block_body.calculate_withdrawals_root(),
        logs_bloom: Bloom(FixedBytes::default()),
        difficulty: U256::ZERO,
        number: seq_num.0,
        gas_limit: header_param.gas_limit,
        gas_used: 0,
        // timestamp in consensus proposal is in Unix milliseconds
        // but we commit the block in Unix seconds for integration compatibility
        timestamp: monad_block.get_timestamp().div(1000),
        mix_hash: randao_reveal_hasher.hash().0.into(),
        nonce: 0,
        // TODO: calculate base fee according to EIP1559
        // Remember to remove hardcoded value in monad-eth-block-validator
        // and in monad-eth-txpool
        base_fee_per_gas: Some(BASE_FEE_PER_GAS),
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None,
        extra_data: monad_block.get_id().0 .0.into(),
    }
}
