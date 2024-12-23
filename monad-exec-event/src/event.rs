//! Definitions of fundamental event objects shared between readers and
//! writers. Most of these are resident in shared memory segments.

use std::sync::atomic::AtomicU64;
use bit_field::BitField;

use crate::event_types::monad_event_type;

/// Describes which event ring an event is recorded to; different categories
/// of events are recorded to different rings
pub enum EventRingType {
    Exec,
    Trace,
}

/// Descriptor for a single event; this fixed-size object is passed via a
/// shared memory ring between threads, potentially in different processes;
/// the rest of the (variably-sized) event is called the "event payload", and
/// lives in a shared memory heap that can be accessed using this descriptor
#[allow(non_camel_case_types)]
#[repr(C)]
pub struct monad_event_descriptor {
    pub seqno: AtomicU64,
    pub event_type: monad_event_type,
    pub payload_page: u16,
    pub offset: u32,
    pub scope_len_src_bitfields: u32,
    pub flow_bitfields: u32,
    pub epoch_nanos: u64,
}

impl monad_event_descriptor {
    pub fn is_pop_scope(&self) -> bool {
        self.scope_len_src_bitfields.get_bit(0)
    }

    pub fn get_length(&self) -> u32 {
        self.scope_len_src_bitfields.get_bits(1..24)
    }

    pub fn get_source_id(&self) -> u8 {
        self.scope_len_src_bitfields.get_bits(24..) as u8
    }

    pub fn get_block_flow_id(&self) -> u16 {
        self.flow_bitfields.get_bits(0..12) as u16
    }

    pub fn get_txn_num(&self) -> u32 {
        self.flow_bitfields.get_bits(12..)
    }
}

// This is not the full definition of the structure, but this is the
// only cache line the reader is allowed to read
#[allow(non_camel_case_types)]
#[repr(C, align(64))]
pub(crate) struct monad_event_payload_page {
    pub(crate) overwrite_seqno: AtomicU64,
}

/// Default location of the UNIX domain socket address for the event server
/// endpoint
pub const DEFAULT_SOCKET_PATH: &str = "/tmp/monad_event.sock";
