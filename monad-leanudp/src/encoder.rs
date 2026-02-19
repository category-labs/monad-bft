use bytes::Bytes;
use monad_executor::ExecutorMetrics;
use thiserror::Error;

use crate::{
    metrics::{
        COUNTER_LEANUDP_ENCODE_BYTES, COUNTER_LEANUDP_ENCODE_ERROR_TOO_LARGE,
        COUNTER_LEANUDP_ENCODE_FRAGMENTS, COUNTER_LEANUDP_ENCODE_MESSAGES,
    },
    FragmentType, PacketHeader, LEANUDP_HEADER_SIZE,
};

/// Maximum fragments per message.
pub(crate) const MAX_FRAGMENTS: usize = 128;

const WIREAUTH_HEADER_SIZE: usize = 32;
const IP_HEADER_SIZE: usize = 20;
const UDP_HEADER_SIZE: usize = 8;
const NETWORK_OVERHEAD: usize = WIREAUTH_HEADER_SIZE + IP_HEADER_SIZE + UDP_HEADER_SIZE;

/// Compute max fragment payload for a given MTU.
pub fn max_payload_for_mtu(mtu: usize) -> usize {
    mtu.saturating_sub(NETWORK_OVERHEAD + LEANUDP_HEADER_SIZE)
}

#[derive(Debug, Error)]
pub enum EncodeError {
    #[error(
        "payload too large: {payload_len} bytes requires {fragment_count} fragments, max is {MAX_FRAGMENTS}"
    )]
    PayloadTooLarge {
        payload_len: usize,
        fragment_count: usize,
    },
}

pub struct EncoderMetrics<'a>(pub &'a ExecutorMetrics);

pub struct Encoder {
    max_fragment_payload: usize,
    next_msg_id: u32,
    metrics: ExecutorMetrics,
}

impl Encoder {
    pub(crate) fn new(max_fragment_payload: usize) -> Self {
        Self {
            max_fragment_payload: max_fragment_payload.saturating_sub(LEANUDP_HEADER_SIZE),
            next_msg_id: 0,
            metrics: ExecutorMetrics::default(),
        }
    }

    pub fn fragment(&mut self, payload: Bytes) -> Result<FragmentIter, EncodeError> {
        let payload_len = payload.len();
        let count = payload_len.max(1).div_ceil(self.max_fragment_payload);

        if count > MAX_FRAGMENTS {
            self.metrics[COUNTER_LEANUDP_ENCODE_ERROR_TOO_LARGE] += 1;
            return Err(EncodeError::PayloadTooLarge {
                payload_len,
                fragment_count: count,
            });
        }

        let msg_id = self.next_msg_id;
        self.next_msg_id = self.next_msg_id.wrapping_add(1);

        self.metrics[COUNTER_LEANUDP_ENCODE_MESSAGES] += 1;
        self.metrics[COUNTER_LEANUDP_ENCODE_FRAGMENTS] += count as u64;
        self.metrics[COUNTER_LEANUDP_ENCODE_BYTES] += payload_len as u64;

        Ok(FragmentIter {
            payload,
            max_payload: self.max_fragment_payload,
            msg_id,
            current: 0,
            count,
        })
    }

    pub fn max_payload_size(&self) -> usize {
        self.max_fragment_payload.saturating_mul(MAX_FRAGMENTS)
    }

    pub fn executor_metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    pub fn metrics(&self) -> EncoderMetrics<'_> {
        EncoderMetrics(&self.metrics)
    }
}

#[derive(Debug)]
pub struct FragmentIter {
    payload: Bytes,
    max_payload: usize,
    msg_id: u32,
    current: usize,
    count: usize,
}

impl Iterator for FragmentIter {
    type Item = (PacketHeader, Bytes);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.count {
            return None;
        }

        let i = self.current;
        let start = i.saturating_mul(self.max_payload);
        let end = start
            .saturating_add(self.max_payload)
            .min(self.payload.len());
        let data = self.payload.slice(start..end);

        let seq_num = i as u16;
        let is_start = i == 0;
        let is_end = i == self.count - 1;
        let fragment_type = match (is_start, is_end) {
            (true, true) => FragmentType::Complete,
            (true, false) => FragmentType::Start,
            (false, true) => FragmentType::End,
            (false, false) => FragmentType::Middle,
        };
        let header = PacketHeader::new(self.msg_id, seq_num, fragment_type);

        self.current += 1;
        Some((header, data))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count - self.current;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for FragmentIter {}
