use monad_event_ring::{
    ffi::{monad_event_ring_type, monad_event_ring_type_MONAD_EVENT_RING_TYPE_EXEC},
    EventDescriptorInfo, EventRingType,
};

use self::bytes::{ref_from_bytes, ref_from_bytes_with_trailing};
use crate::ffi::{
    self, g_monad_exec_event_metadata_hash, monad_exec_account_access,
    monad_exec_account_access_list_header, monad_exec_block_finalized, monad_exec_block_header,
    monad_exec_block_qc, monad_exec_block_reject, monad_exec_block_result,
    monad_exec_block_verified, monad_exec_evm_error, monad_exec_flow_type_MONAD_FLOW_ACCOUNT_INDEX,
    monad_exec_flow_type_MONAD_FLOW_BLOCK_SEQNO, monad_exec_flow_type_MONAD_FLOW_TXN_ID,
    monad_exec_storage_access, monad_exec_txn_call_frame, monad_exec_txn_evm_output,
    monad_exec_txn_log, monad_exec_txn_reject, monad_exec_txn_start,
};

mod bytes;

/// Marker type that implements [`EventRingType`] for monad execution events.
#[derive(Debug)]
pub struct ExecEventRingType;

/// Owned rust enum for monad execution events.
///
/// This type uses the bindgen generated monad-execution C types to enable efficient memcpys of
/// event ring payloads.
///
/// See [`ExecEventsRef`] for the zero-copy ref version.
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub enum ExecEvents {
    BlockStart(monad_exec_block_header),
    BlockReject(monad_exec_block_reject),
    BlockPerfEvmEnter,
    BlockPerfEvmExit,
    BlockEnd(monad_exec_block_result),
    BlockQC(monad_exec_block_qc),
    BlockFinalized(monad_exec_block_finalized),
    BlockVerified(monad_exec_block_verified),
    TxnStart {
        txn_index: u32,
        txn_start: monad_exec_txn_start,
        data_bytes: Box<[u8]>,
    },
    TxnReject {
        txn_index: u32,
        reject: monad_exec_txn_reject,
    },
    TxnPerfEvmEnter,
    TxnPerfEvmExit,
    TxnEvmOutput {
        txn_index: u32,
        output: monad_exec_txn_evm_output,
    },
    TxnLog {
        txn_index: u32,
        txn_log: monad_exec_txn_log,
        topic_bytes: Box<[u8]>,
        data_bytes: Box<[u8]>,
    },
    TxnCallFrame {
        txn_index: u32,
        txn_call_frame: monad_exec_txn_call_frame,
        input_bytes: Box<[u8]>,
        return_bytes: Box<[u8]>,
    },
    TxnEnd,
    AccountAccessListHeader(monad_exec_account_access_list_header),
    AccountAccess(monad_exec_account_access),
    StorageAccess(monad_exec_storage_access),
    EvmError(monad_exec_evm_error),
}

/// Ref rust enum for monad execution events.
///
/// This enum should only be used with the zero-copy event ring API to enable zero-copy access to
/// event ring payloads.
///
/// See [`ExecEvents`] for the owned version.
#[allow(missing_docs)]
#[derive(Copy, Clone, Debug)]
pub enum ExecEventsRef<'reader> {
    BlockStart(&'reader monad_exec_block_header),
    BlockReject(&'reader monad_exec_block_reject),
    BlockPerfEvmEnter,
    BlockPerfEvmExit,
    BlockEnd(&'reader monad_exec_block_result),
    BlockQC(&'reader monad_exec_block_qc),
    BlockFinalized(&'reader monad_exec_block_finalized),
    BlockVerified(&'reader monad_exec_block_verified),
    TxnStart {
        txn_index: u32,
        txn_start: &'reader monad_exec_txn_start,
        data_bytes: &'reader [u8],
    },
    TxnReject {
        txn_index: u32,
        reject: &'reader monad_exec_txn_reject,
    },
    TxnPerfEvmEnter,
    TxnPerfEvmExit,
    TxnEvmOutput {
        txn_index: u32,
        output: &'reader monad_exec_txn_evm_output,
    },
    TxnLog {
        txn_index: u32,
        txn_log: &'reader monad_exec_txn_log,
        topic_bytes: &'reader [u8],
        data_bytes: &'reader [u8],
    },
    TxnCallFrame {
        txn_index: u32,
        txn_call_frame: &'reader monad_exec_txn_call_frame,
        input_bytes: &'reader [u8],
        return_bytes: &'reader [u8],
    },
    TxnEnd,
    AccountAccessListHeader(&'reader monad_exec_account_access_list_header),
    AccountAccess(&'reader monad_exec_account_access),
    StorageAccess(&'reader monad_exec_storage_access),
    EvmError(&'reader monad_exec_evm_error),
}

impl<'reader> ExecEventsRef<'reader> {
    /// Converts the [`ExecEventsRef`] to its owned variant [`ExecEvents`].
    pub fn into_owned(self) -> ExecEvents {
        match self {
            Self::BlockStart(block_start) => ExecEvents::BlockStart(*block_start),
            Self::BlockReject(block_reject) => ExecEvents::BlockReject(*block_reject),
            Self::BlockPerfEvmEnter => ExecEvents::BlockPerfEvmEnter,
            Self::BlockPerfEvmExit => ExecEvents::BlockPerfEvmExit,
            Self::BlockEnd(block_end) => ExecEvents::BlockEnd(*block_end),
            Self::BlockQC(block_qc) => ExecEvents::BlockQC(*block_qc),
            Self::BlockFinalized(block_finalized) => ExecEvents::BlockFinalized(*block_finalized),
            Self::BlockVerified(block_verified) => ExecEvents::BlockVerified(*block_verified),
            Self::TxnStart {
                txn_index,
                txn_start,
                data_bytes,
            } => ExecEvents::TxnStart {
                txn_index,
                txn_start: *txn_start,
                data_bytes: data_bytes.to_vec().into_boxed_slice(),
            },
            Self::TxnReject { txn_index, reject } => ExecEvents::TxnReject {
                txn_index,
                reject: *reject,
            },
            Self::TxnPerfEvmEnter => ExecEvents::TxnPerfEvmEnter,
            Self::TxnPerfEvmExit => ExecEvents::TxnPerfEvmExit,
            Self::TxnEvmOutput { txn_index, output } => ExecEvents::TxnEvmOutput {
                txn_index,
                output: *output,
            },
            Self::TxnLog {
                txn_index,
                txn_log,
                topic_bytes,
                data_bytes,
            } => ExecEvents::TxnLog {
                txn_index,
                txn_log: *txn_log,
                topic_bytes: topic_bytes.to_vec().into_boxed_slice(),
                data_bytes: data_bytes.to_vec().into_boxed_slice(),
            },
            Self::TxnCallFrame {
                txn_index,
                txn_call_frame,
                input_bytes,
                return_bytes,
            } => ExecEvents::TxnCallFrame {
                txn_index,
                txn_call_frame: *txn_call_frame,
                input_bytes: input_bytes.to_vec().into_boxed_slice(),
                return_bytes: return_bytes.to_vec().into_boxed_slice(),
            },
            Self::TxnEnd => ExecEvents::TxnEnd,
            Self::AccountAccessListHeader(account_access_list_header) => {
                ExecEvents::AccountAccessListHeader(*account_access_list_header)
            }
            Self::AccountAccess(account_access) => ExecEvents::AccountAccess(*account_access),
            Self::StorageAccess(storage_access) => ExecEvents::StorageAccess(*storage_access),
            Self::EvmError(evm_error) => ExecEvents::EvmError(*evm_error),
        }
    }
}

/// Flow info for execution events.
pub struct ExecEventRingFlowInfo {
    block_seqno: u64,
    txn_idx: Option<u32>,
    account_idx: u64,
}

impl EventRingType for ExecEventRingType {
    fn ring_ctype() -> monad_event_ring_type {
        monad_event_ring_type_MONAD_EVENT_RING_TYPE_EXEC
    }

    fn ring_metadata_hash() -> &'static [u8; 32] {
        unsafe { &g_monad_exec_event_metadata_hash }
    }

    type FlowInfo = ExecEventRingFlowInfo;

    fn transmute_flow_info(user: [u64; 4]) -> Self::FlowInfo {
        Self::FlowInfo {
            block_seqno: user[monad_exec_flow_type_MONAD_FLOW_BLOCK_SEQNO as usize],
            txn_idx: user[monad_exec_flow_type_MONAD_FLOW_TXN_ID as usize]
                .checked_sub(1)
                .map(|txn_idx| txn_idx.try_into().unwrap()),
            account_idx: user[monad_exec_flow_type_MONAD_FLOW_ACCOUNT_INDEX as usize],
        }
    }

    type Event = ExecEvents;
    type EventRef<'reader> = ExecEventsRef<'reader>;

    fn raw_to_event_ref<'reader>(
        info: EventDescriptorInfo<Self>,
        bytes: &'reader [u8],
    ) -> Self::EventRef<'reader> {
        match info.event_type {
            ffi::monad_exec_event_type_MONAD_EXEC_NONE => {
                panic!("ExecEventRingType encountered NONE event_type");
            }
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_START => {
                ExecEventsRef::BlockStart(ref_from_bytes(bytes).expect("BlockStart event valid"))
            }
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_REJECT => {
                ExecEventsRef::BlockReject(ref_from_bytes(bytes).expect("BlockReject event valid"))
            }
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_PERF_EVM_ENTER => {
                assert_eq!(bytes.len(), 0, "BlockEvmEnter payload is empty");
                ExecEventsRef::BlockPerfEvmEnter
            }
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_PERF_EVM_EXIT => {
                assert_eq!(bytes.len(), 0, "BlockEvmExit payload is empty");
                ExecEventsRef::BlockPerfEvmExit
            }
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_END => {
                ExecEventsRef::BlockEnd(ref_from_bytes(bytes).expect("BlockEnd event valid"))
            }
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_QC => {
                ExecEventsRef::BlockQC(ref_from_bytes(bytes).expect("BlockQC event valid"))
            }
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_FINALIZED => ExecEventsRef::BlockFinalized(
                ref_from_bytes(bytes).expect("BlockFinalized event valid"),
            ),
            ffi::monad_exec_event_type_MONAD_EXEC_BLOCK_VERIFIED => ExecEventsRef::BlockVerified(
                ref_from_bytes(bytes).expect("BlockVerified event valid"),
            ),
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_START => {
                let (txn_start, [data_bytes]) =
                    ref_from_bytes_with_trailing::<monad_exec_txn_start, 1>(bytes, |txn_start| {
                        [txn_start.txn_header.data_length.try_into().unwrap()]
                    })
                    .expect("TxnStart event valid");

                ExecEventsRef::TxnStart {
                    txn_index: info
                        .flow_info
                        .txn_idx
                        .expect("TxnStart event has txn_idx in flow_info"),
                    txn_start,
                    data_bytes,
                }
            }
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_REJECT => ExecEventsRef::TxnReject {
                txn_index: info
                    .flow_info
                    .txn_idx
                    .expect("TxnReject event has txn_idx in flow_info"),
                reject: ref_from_bytes(bytes).expect("TxnReject event valid"),
            },
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_PERF_EVM_ENTER => {
                assert_eq!(bytes.len(), 0, "TxnEvmEnter payload is empty");
                ExecEventsRef::TxnPerfEvmEnter
            }
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_PERF_EVM_EXIT => {
                assert_eq!(bytes.len(), 0, "TxnEvmExit payload is empty");
                ExecEventsRef::TxnPerfEvmExit
            }
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_EVM_OUTPUT => ExecEventsRef::TxnEvmOutput {
                txn_index: info
                    .flow_info
                    .txn_idx
                    .expect("TxnEvmOutput event has txn_idx in flow_info"),
                output: ref_from_bytes(bytes).expect("TxnEvmOutput event valid"),
            },
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_LOG => {
                let (txn_log, [topic_bytes, data_bytes]) =
                    ref_from_bytes_with_trailing::<monad_exec_txn_log, 2>(bytes, |txn_log| {
                        [
                            Into::<usize>::into(txn_log.topic_count)
                                .checked_mul(size_of::<ffi::monad_c_bytes32>())
                                .unwrap(),
                            txn_log.data_length.try_into().unwrap(),
                        ]
                    })
                    .expect("TxnLog event valid");

                ExecEventsRef::TxnLog {
                    txn_index: info
                        .flow_info
                        .txn_idx
                        .expect("TxnLog event has txn_idx in flow_info"),
                    txn_log,
                    topic_bytes,
                    data_bytes,
                }
            }
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_CALL_FRAME => {
                let (txn_call_frame, [input_bytes, return_bytes]) =
                    ref_from_bytes_with_trailing::<monad_exec_txn_call_frame, 2>(
                        bytes,
                        |txn_call_frame| {
                            [
                                txn_call_frame.input_length.try_into().unwrap(),
                                txn_call_frame.return_length.try_into().unwrap(),
                            ]
                        },
                    )
                    .expect("TxnCallFrame event valid");

                ExecEventsRef::TxnCallFrame {
                    txn_index: info
                        .flow_info
                        .txn_idx
                        .expect("TxnCallFrame event has txn_idx in flow_info"),
                    txn_call_frame,
                    input_bytes,
                    return_bytes,
                }
            }
            ffi::monad_exec_event_type_MONAD_EXEC_TXN_END => {
                assert_eq!(bytes.len(), 0, "TxnEnd payload is empty");
                ExecEventsRef::TxnEnd
            }
            ffi::monad_exec_event_type_MONAD_EXEC_ACCOUNT_ACCESS_LIST_HEADER => {
                ExecEventsRef::AccountAccessListHeader(
                    ref_from_bytes(bytes).expect("AccountAccessListHeader event valid"),
                )
            }
            ffi::monad_exec_event_type_MONAD_EXEC_ACCOUNT_ACCESS => ExecEventsRef::AccountAccess(
                ref_from_bytes(bytes).expect("AccountAccess event valid"),
            ),
            ffi::monad_exec_event_type_MONAD_EXEC_STORAGE_ACCESS => ExecEventsRef::StorageAccess(
                ref_from_bytes(bytes).expect("StorageAccess event valid"),
            ),
            ffi::monad_exec_event_type_MONAD_EXEC_EVM_ERROR => {
                ExecEventsRef::EvmError(ref_from_bytes(bytes).expect("EvmError event valid"))
            }
            event_type => panic!("ExecEventRingType encountered unknown event_type {event_type}"),
        }
    }

    fn event_ref_to_event<'reader>(event_ref: Self::EventRef<'reader>) -> Self::Event {
        Self::EventRef::into_owned(event_ref)
    }
}

#[cfg(test)]
mod test {
    use monad_event_ring::{EventNextResult, SnapshotEventRing, TypedEventRing};

    use crate::ExecEventRingType;

    #[test]
    fn basic_test() {
        const SNAPSHOT_NAME: &str = "ETHEREUM_MAINNET_30B_15M";
        const SNAPSHOT_ZSTD_BYTES: &[u8] =
            include_bytes!("../../test/data/exec-events-emn-30b-15m.zst");

        let snapshot = SnapshotEventRing::<ExecEventRingType>::new_from_zstd_bytes(
            SNAPSHOT_ZSTD_BYTES,
            SNAPSHOT_NAME,
        )
        .unwrap();

        let mut event_reader = snapshot.create_reader();

        loop {
            match event_reader.next() {
                EventNextResult::Gap => panic!("snapshot cannot gap"),
                EventNextResult::NotReady => break,
                EventNextResult::Ready(event_descriptor) => {
                    let event = event_descriptor.try_read();

                    eprintln!("event: {event:#?}");
                }
            }
        }
    }
}
