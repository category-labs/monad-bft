use crate::store::traits::{BlobTableId, ScannableTableId, TableId};

pub trait PointTableSpec {
    const TABLE: TableId;
}

pub trait ScannableTableSpec {
    const TABLE: ScannableTableId;
}

pub trait BlobTableSpec {
    const TABLE: BlobTableId;
}

pub fn aligned_u64_start(raw: u64, span: u64) -> u64 {
    (raw / span) * span
}

pub fn u64_key(raw: u64) -> Vec<u8> {
    raw.to_be_bytes().to_vec()
}

pub fn stream_page_key(stream_id: &str, page_start_local: u32) -> Vec<u8> {
    let mut key = format!("{stream_id}/").into_bytes();
    key.extend_from_slice(&u64::from(page_start_local).to_be_bytes());
    key
}

pub fn page_prefix(page_start_local: u32) -> Vec<u8> {
    let mut key = u64::from(page_start_local).to_be_bytes().to_vec();
    key.push(b'/');
    key
}

pub fn page_stream_key(page_start_local: u32, stream_id: &str) -> Vec<u8> {
    let mut key = page_prefix(page_start_local);
    key.extend_from_slice(stream_id.as_bytes());
    key
}
