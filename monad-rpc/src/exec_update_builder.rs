use std::{collections::VecDeque, mem::size_of, sync::atomic::Ordering};

use alloy_primitives::{Bloom, Bytes, B256, B64, U256, U64};
use monad_exec_event::{event::monad_event_descriptor, event_reader::*, event_types};
use reth_rpc_types::{Header, Transaction, TransactionReceipt};

/// Whenever an account balance is updated in TrieDB, one of these
/// events is produced
type BalanceUpdate = event_types::account_balance;

/// Whenever an account's storage is updated in TrieDB, one of these
/// events is produced
type StorageUpdate = event_types::account_storage;

/// All info about each transaction that is aggregated into a BlockUpdate is
/// stored in this object
#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct TransactionInfo {
    pub header: Transaction,
    pub receipt: TransactionReceipt,
    pub balance_updates: Vec<BalanceUpdate>,
    pub storage_updates: Vec<StorageUpdate>,
}

/// Notification sent when a block has been committed to the blockchain
#[derive(Debug, Clone)]
pub struct BlockUpdate {
    pub header: Header,
    pub txns: Vec<Option<TransactionInfo>>,
}

/// This holds the event reader state and the fixed-sized buffers that
/// the next incoming event is copied into; all of this data is immutable
/// outside of the code that calls reader.copy_next()
struct ReaderState<'meta, 'shm> {
    reader: EventReader<'meta, 'shm>,
    block_header_table: &'shm [event_types::block_exec_header],
    event: monad_event_descriptor,
    payload_buf: Vec<u8>,
}

/// This object tracks the block updates which are being reassembled, and
/// those that are already assembled but not yet handed back to the call
struct UpdateState {
    reassembly_updates: [Option<Box<BlockUpdate>>; 4096],
    ready_updates: VecDeque<Box<BlockUpdate>>,
}

/// Object which captures low-level execution events and builds complete
/// BlockUpdate objects out of them
pub struct ExecUpdateBuilder<'meta, 'shm> {
    reader_state: ReaderState<'meta, 'shm>,
    update_state: UpdateState,
}

/// The result of polling the update builder for the next block update.
/// We either get ownership of an assembled, commited block
/// (Box<BlockUpdate>), or one is not ready (Empty), or our thread was
/// not fast enough and we "lost" events. Losing events can take the form
/// of a sequence number gap or a payload expiration, see the architectural
/// documentation (`event.md`) in C repository for an explanation.
pub enum PollResult {
    Empty,
    Gap { last_seqno: u64, cur_seqno: u64 },
    PayloadExpired(u64),
    Update(Box<BlockUpdate>),
}

impl<'meta, 'shm> ExecUpdateBuilder<'meta, 'shm> {
    pub fn new(
        reader: EventReader<'meta, 'shm>,
        block_header_table: &'shm [event_types::block_exec_header],
    ) -> ExecUpdateBuilder<'meta, 'shm> {
        const NONE_INIT: Option<Box<BlockUpdate>> = None;
        ExecUpdateBuilder {
            reader_state: ReaderState {
                reader,
                block_header_table,
                event: unsafe {
                    std::mem::MaybeUninit::<monad_event_descriptor>::zeroed().assume_init()
                },
                payload_buf: vec![0; 1 << 25],
            },
            update_state: UpdateState {
                reassembly_updates: [NONE_INIT; 4096],
                ready_updates: VecDeque::new(),
            },
        }
    }

    pub fn poll(&'_ mut self) -> PollResult {
        if let Some(update) = self.update_state.ready_updates.pop_front() {
            // An update is ready immediately; hand it back
            PollResult::Update(update)
        } else {
            // No update ready; poll for execution events, trying to reassemble
            // them into a complete BlockUpdate. This will eagerly assemble as
            // many BlockUpdates as can be completed given the number of events
            // produced so far. It turns the first one, if one was assembled
            self.poll_events()
        }
    }

    fn poll_events(&mut self) -> PollResult {
        loop {
            let rs = &mut self.reader_state;
            let copy_result = rs
                .reader
                .copy_next(&mut rs.event, rs.payload_buf.as_mut_slice());
            match copy_result {
                CopyResult::NotReady => break,
                r @ CopyResult::Gap | r @ CopyResult::PayloadExpired => {
                    // Destroy any in-progress reassembly updates and
                    // all drop all ready updates
                    for ru in &mut self.update_state.reassembly_updates {
                        ru.take();
                    }
                    self.update_state.ready_updates.clear();

                    // Reset the reader and report the gap or payload expiration
                    return if let CopyResult::Gap = r {
                        PollResult::Gap {
                            last_seqno: self.reader_state.reader.last_seqno,
                            cur_seqno: self.reader_state.reader.reset(),
                        }
                    } else {
                        PollResult::PayloadExpired(rs.event.seqno.load(Ordering::Relaxed))
                    };
                }
                CopyResult::Ready => {
                    // Rebind "rs" as immutable, the processing code does not need to
                    // change it.
                    let rs = &self.reader_state;
                    assert!(rs.event.get_length() <= rs.payload_buf.len() as u32);
                    Self::reassemble_block_event(rs, &mut self.update_state);
                }
            };
        }

        if let Some(ready) = self.update_state.ready_updates.pop_front() {
            PollResult::Update(ready)
        } else {
            PollResult::Empty
        }
    }

    fn reassemble_block_event(r: &ReaderState, u: &mut UpdateState) {
        use monad_exec_event::event_types::monad_event_type::*;
        match r.event.event_type {
            BLOCK_START => Self::act_on_block_start(r, u),
            BLOCK_END => Self::act_on_block_end(r, u),
            BLOCK_FINALIZE => Self::act_on_block_finalize(r, u),
            TXN_START => Self::act_on_txn_start(r, u),
            TXN_LOG => Self::act_on_txn_log(r, u),
            TXN_RESTART => Self::act_on_txn_restart(r, u),
            TXN_END => Self::act_on_txn_end(r, u),
            _ => (),
        }
    }

    fn act_on_block_start(reader_state: &ReaderState, update_state: &mut UpdateState) {
        let rs = reader_state;
        let block_flow_id = rs.event.get_block_flow_id() as usize;
        let block_header = &rs.block_header_table[block_flow_id];
        if let Some(_) = update_state.reassembly_updates[block_flow_id].take() {
            // This should not happen: if the block ID were legitimately
            // replaced, then we should have seen it be finalized. If
            // we missed the finalization because of a gap, then we should
            // have manually destroyed the reassembly object in the gap
            // recovery logic. So this is some kind of bug.
            panic!("block flow ID {block_flow_id} reused, but something is already there?");
        };
        let block_update = BlockUpdate {
            header: create_alloy_block_header(block_header),
            txns: vec![None; block_header.txn_count as usize],
        };
        _ = update_state.reassembly_updates[block_flow_id].insert(Box::new(block_update));
    }

    fn act_on_block_end(reader_state: &ReaderState, update_state: &mut UpdateState) {
        let rs = reader_state;
        let block_flow_id = rs.event.get_block_flow_id() as usize;
        if let None = update_state.reassembly_updates[block_flow_id] {
            // In this case, we are seeing a BLOCK_END without having seen the
            // start of it, which happens when we first join in the middle of
            // execution running, or because of gaps; we just ignore this
            return;
        }
        let block_update = update_state.reassembly_updates[block_flow_id]
            .as_mut()
            .unwrap();
        let r: &event_types::block_exec_result =
            &unsafe { *(rs.payload_buf.as_ptr() as *const event_types::block_exec_result) };
        block_update.header.logs_bloom = Bloom::from(&r.logs_bloom);
        block_update.header.state_root = r.state_root;
        block_update.header.transactions_root = r.transactions_root;
        block_update.header.receipts_root = r.receipts_root;
        block_update.header.withdrawals_root = Some(r.withdrawals_root);
        block_update.header.gas_used = U256::from(r.gas_used);
    }

    fn act_on_block_finalize(reader_state: &ReaderState, update_state: &mut UpdateState) {
        let rs = reader_state;
        let block_flow_id = rs.event.get_block_flow_id() as usize;
        if let Some(complete) = update_state.reassembly_updates[block_flow_id].take() {
            update_state.ready_updates.push_back(complete);
        }
    }

    fn act_on_txn_start(reader_state: &ReaderState, update_state: &mut UpdateState) {
        let rs = reader_state;
        let block_flow_id = rs.event.get_block_flow_id() as usize;
        if let None = update_state.reassembly_updates[block_flow_id] {
            return; // Started listening outside a boundary
        }
        let block_update = update_state.reassembly_updates[block_flow_id]
            .as_mut()
            .unwrap();
        let txn_no = rs.event.get_txn_num() as usize;
        let c_header: &event_types::txn_header =
            &unsafe { *(rs.payload_buf.as_ptr() as *const event_types::txn_header) };
        if let None = block_update.txns[txn_no] {
            // Our first time seeing this transaction; replace the
            // Option with a default-initialized container for holding txn
            // info
            block_update.txns[txn_no] = Some(TransactionInfo::default());
        }
        let txn_info = block_update.txns[txn_no].as_mut().unwrap();
        // TODO(ken): need to check all of these
        txn_info.header.nonce = U64::from(c_header.nonce);
        txn_info.header.block_number = block_update.header.number;
        txn_info.header.transaction_index = Some(U256::from(txn_no));
        // TODO(ken): we don't know who it's from; then sender address is
        //    recovered from the signature, and hasn't been done yet
        //    at the time TXN_START is sent. We need more events, revisit
        //    this before moving out of drafts
        //txn_info.header.from
        txn_info.header.to = Some(c_header.to);
        txn_info.header.value = c_header.value;
        txn_info.header.gas = U256::from(c_header.gas_limit);
        // TODO(ken): wanted U128 here not U256?
        //txn_info.header.max_fee_per_gas = ...
        txn_info.header.signature = Some(reth_rpc_types::Signature {
            r: c_header.r,
            s: c_header.s,
            v: U256::from(c_header.y_parity),
            y_parity: Some(reth_rpc_types::Parity(c_header.y_parity == 1)),
        });
        // TODO(ken): this is not correct...
        //txn_info.header.chain_id = Some(U64::from(c_header.chain_id.to_u64));
        txn_info.header.transaction_type = Some(U64::from(c_header.txn_type));
        // TODO(ken): the transaction input is not part of the event
        //    payload; should it be?
    }

    fn act_on_txn_log(reader_state: &ReaderState, update_state: &mut UpdateState) {
        if let (Some(txn_info)) = Self::try_get_txn(reader_state, update_state) {
            let payload_base = reader_state.payload_buf.as_ptr();
            let c_log_header: &event_types::txn_log =
                &unsafe { *(payload_base as *const event_types::txn_log) };
            let topics: &[B256] = unsafe {
                let topic_base =
                    payload_base.wrapping_add(size_of::<event_types::txn_log>()) as *const B256;
                std::slice::from_raw_parts(topic_base, c_log_header.topic_count as usize)
            };
            let data: &[u8] = unsafe {
                let data_base = payload_base.wrapping_add(
                    size_of::<event_types::txn_log>() * c_log_header.topic_count as usize,
                );
                std::slice::from_raw_parts(data_base, c_log_header.data_length as usize)
            };
            let mut log = reth_rpc_types::Log::default();

            log.address = c_log_header.address;
            log.topics = Vec::from(topics);
            log.data = Bytes::copy_from_slice(data);
            // TODO(ken): there's a lot of deduplicated header stuff that goes inside
            //     a reth Log, but we're not populating that now; this will be done
            //     later
            txn_info.receipt.logs.push(log);
        }
    }

    fn act_on_txn_restart(reader_state: &ReaderState, update_state: &mut UpdateState) {
        if let (Some(txn_info)) = Self::try_get_txn(reader_state, update_state) {
            // We may have previously seen logs during an earlier speculative
            // execution of this transaction. This transaction's execution is
            // being restarted, clear of all these logs because they will be
            // emitted again.
            txn_info.receipt.logs.clear();
        }
    }

    fn act_on_txn_end(reader_state: &ReaderState, update_state: &mut UpdateState) {
        if let (Some(txn_info)) = Self::try_get_txn(reader_state, update_state) {
            let c_receipt: &event_types::txn_receipt =
                &unsafe { *(reader_state.payload_buf.as_ptr() as *const event_types::txn_receipt) };
            txn_info.receipt.status_code = Some(U64::from(c_receipt.status));
            txn_info.receipt.gas_used = Some(U256::from(c_receipt.gas_used));
        }
    }

    fn try_get_block<'a>(
        reader_state: &ReaderState,
        update_state: &'a mut UpdateState,
    ) -> Option<&'a mut Box<BlockUpdate>> {
        let rs = reader_state;
        let block_flow_id = rs.event.get_block_flow_id() as usize;
        // In the None case, we are seeing a block-related object update without
        // having seen the start of it, which happens when we first join in the
        // middle of execution running, or because of gaps; we just ignore this
        update_state.reassembly_updates[block_flow_id].as_mut()
    }
    fn try_get_txn<'a>(
        reader_state: &ReaderState,
        update_state: &'a mut UpdateState,
    ) -> Option<&'a mut TransactionInfo> {
        if let Some(block_update) = Self::try_get_block(reader_state, update_state) {
            let txn_no = reader_state.event.get_txn_num() as usize;
            block_update.txns[txn_no].as_mut()
        } else {
            None
        }
    }
}

fn create_alloy_block_header(event_header: &event_types::block_exec_header) -> Header {
    Header {
        hash: None,
        parent_hash: event_header.parent_hash,
        uncles_hash: event_header.ommers_hash,
        miner: event_header.beneficiary,
        state_root: B256::ZERO,
        transactions_root: B256::ZERO,
        receipts_root: B256::ZERO,
        logs_bloom: Bloom::ZERO,
        difficulty: U256::from(event_header.difficulty),
        number: Some(U256::from(event_header.number)),
        gas_limit: U256::from(event_header.gas_limit),
        gas_used: U256::ZERO,
        timestamp: U256::from(event_header.timestamp),
        extra_data: Bytes::new(),
        mix_hash: Some(event_header.mix_hash),
        nonce: Some(B64::from(&event_header.nonce)),
        base_fee_per_gas: Some(event_header.base_fee_per_gas),
        withdrawals_root: Some(B256::ZERO),
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None,
    }
}

// Declaration of public interface for the fake event test server, which
// is a C library
#[link(name = "monad_event_test_server")]
extern "C" {
    fn monad_event_test_server_create_from_bytes(
        socket_path: *const libc::c_char,
        output: *const libc::c_void,
        capture_data: *const u8,
        capture_len: usize,
        unmap_on_close: u8,
        server_p: *mut *const libc::c_void,
    ) -> libc::c_int;

    fn monad_event_server_destroy(queue: *const libc::c_void);

    fn monad_event_test_server_accept_one(queue: *const libc::c_void) -> libc::c_int;
}

pub struct TestServer {
    test_server: *const libc::c_void,
    socket_file_path: PathBuf,
}

use std::{ffi::CString, os::unix::fs::FileTypeExt, path::PathBuf};

use monad_exec_event::{event::EventRingType, event_client::*};

impl TestServer {
    pub fn create(socket_path: &str, capture_bytes: &'static [u8]) -> TestServer {
        let socket_file_path = std::path::PathBuf::from(socket_path);

        // If the socket path already exists and is definitely a socket,
        // unlink it; we're trying to prevent EADDRINUSE with previous
        // servers
        if let Ok(metadata) = socket_file_path.metadata() {
            if metadata.file_type().is_socket() {
                _ = std::fs::remove_file(socket_file_path.as_path());
            }
        }

        let socket_path_cstr =
            CString::new(socket_path.as_bytes()).expect("embedded nul in socket_path?");
        let output: *const libc::c_void = std::ptr::null();
        let mut server: *const libc::c_void = std::ptr::null_mut();

        let r = unsafe {
            monad_event_test_server_create_from_bytes(
                socket_path_cstr.as_ptr(),
                std::ptr::null(),
                capture_bytes.as_ptr(),
                capture_bytes.len(),
                /*unmap_on_close=*/ 0,
                &mut server,
            )
        };
        assert_eq!(r, 0);
        TestServer {
            test_server: server,
            socket_file_path,
        }
    }

    pub fn prepare_accept_one(&mut self) {
        let r = unsafe { monad_event_test_server_accept_one(self.test_server) };
        assert_eq!(r, 0);
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        unsafe { monad_event_server_destroy(self.test_server) };
        // Cleanup the socket file, we don't care if it exists
        _ = std::fs::remove_file(self.socket_file_path.as_path());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic_test() {
        let socket_address = "/tmp/monad_rpc_exec_event.sock";
        let basic_test_shmem_contents = include_bytes!("test_data/exec_events.shm");

        let mut test_server = TestServer::create(socket_address, basic_test_shmem_contents);
        test_server.prepare_accept_one();

        let options = ConnectOptions {
            socket_path: PathBuf::from(socket_address),
            timeout: libc::timeval {
                tv_sec: 1,
                tv_usec: 0,
            },
        };
        let mut event_proc = EventProc::connect(&options).unwrap();
        let imported_ring = ImportedEventRing::import(event_proc, EventRingType::Exec).unwrap();
        let mut event_reader = EventReader::new(&imported_ring);
        event_reader.last_seqno = 0;
        let mut update_builder =
            ExecUpdateBuilder::new(event_reader, imported_ring.parent.block_header_table);

        let mut update_count: u32 = 0;
        loop {
            // TODO(ken): decide how to actually verify this for real
            if let PollResult::Update(update) = update_builder.poll() {
                println!("{update:#?}");
            }
            update_count += 1;
            if update_count == 10000 {
                break;
            }
        }
    }
}
