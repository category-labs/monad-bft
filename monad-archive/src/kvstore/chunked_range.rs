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

//! Range math shared by the byte-range read paths of the CHUNKING backends
//! (Mongo documents, Dynamo items): both split oversized values into
//! `{key}_chunk_{i}` entries of a fixed per-backend chunk size, with a main
//! entry carrying the chunk count. A range read fetches only the covering
//! chunks and slices.
//!
//! Range semantics (shared with `monad_chain_data::store::BlobStore`'s
//! `read_range`): the end clamps to EOF; a start strictly past EOF, or past
//! the end bound, is an error; a missing key is `None` at the caller.

use std::collections::BTreeMap;

use bytes::Bytes;
use eyre::{bail, Result};

/// Slices `[start, end_exclusive)` out of one whole object.
pub(crate) fn slice_range(object: &Bytes, start: usize, end_exclusive: usize) -> Result<Bytes> {
    if start > end_exclusive || start > object.len() {
        bail!(
            "invalid byte range {start}..{end_exclusive} for a {}-byte object",
            object.len()
        );
    }
    Ok(object.slice(start..end_exclusive.min(object.len())))
}

/// Chunk indices that must be fetched to serve `[start, end_exclusive)` of a
/// `chunk_count`-chunk object, or `None` when the range is satisfiable as
/// empty without any fetch. When `start` lands at/after the last chunk's
/// nominal offset the last chunk is always included, so the assembler can
/// learn the object's total length for its EOF check.
pub(crate) fn covering_chunks(
    start: usize,
    end_exclusive: usize,
    chunk_count: usize,
    chunk_size: usize,
) -> Result<Option<std::ops::RangeInclusive<usize>>> {
    if start > end_exclusive {
        bail!("invalid byte range {start}..{end_exclusive}");
    }
    let last = chunk_count - 1;
    let first_wanted = start / chunk_size;
    if start == end_exclusive {
        // Empty read: no bytes needed, but a start past the last chunk's
        // nominal offset still needs the EOF check below.
        if first_wanted < last || (first_wanted == last && start == last * chunk_size) {
            return Ok(None);
        }
        return Ok(Some(last..=last));
    }
    let last_wanted = (end_exclusive - 1) / chunk_size;
    Ok(Some(first_wanted.min(last)..=last_wanted.min(last)))
}

/// Concatenates `[start, end_exclusive)` from the fetched covering chunks.
/// Every non-last chunk must be exactly `chunk_size` bytes (the writers'
/// invariant); the last chunk's length defines the object's total length for
/// EOF clamping and the past-EOF start check.
pub(crate) fn assemble_chunked_range(
    fetched: &BTreeMap<usize, Bytes>,
    chunk_count: usize,
    chunk_size: usize,
    start: usize,
    end_exclusive: usize,
) -> Result<Bytes> {
    let last = chunk_count - 1;
    for (&index, chunk) in fetched {
        if index < last && chunk.len() != chunk_size {
            bail!(
                "chunk {index} has {} bytes, expected {chunk_size}",
                chunk.len()
            );
        }
    }
    if let Some(last_chunk) = fetched.get(&last) {
        let total = last * chunk_size + last_chunk.len();
        if start > total {
            bail!("invalid byte range {start}..{end_exclusive} for a {total}-byte object");
        }
    }
    let mut out = Vec::new();
    for (&index, chunk) in fetched {
        let chunk_start = index * chunk_size;
        let lo = start.saturating_sub(chunk_start).min(chunk.len());
        let hi = end_exclusive.saturating_sub(chunk_start).min(chunk.len());
        if lo < hi {
            out.extend_from_slice(&chunk[lo..hi]);
        }
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference implementation of the chunked range plan + assembly against
    /// a whole in-memory object, exercising the same code paths the live
    /// read paths use after their entry fetches.
    fn read_via_chunks(
        object: &[u8],
        chunk_size: usize,
        start: usize,
        end_exclusive: usize,
    ) -> Result<Bytes> {
        let chunks: Vec<Bytes> = object
            .chunks(chunk_size)
            .map(Bytes::copy_from_slice)
            .collect();
        assert!(chunks.len() > 1, "test object must actually chunk");
        let Some(fetch) = covering_chunks(start, end_exclusive, chunks.len(), chunk_size)? else {
            return Ok(Bytes::new());
        };
        let fetched: BTreeMap<usize, Bytes> = fetch
            .filter(|&i| i < chunks.len())
            .map(|i| (i, chunks[i].clone()))
            .collect();
        assemble_chunked_range(&fetched, chunks.len(), chunk_size, start, end_exclusive)
    }

    /// Every (start, end) pair over a 3.5-chunk object must agree with the
    /// trivial whole-object slice under the range contract.
    #[test]
    fn chunked_reads_match_whole_object_slices() {
        const CS: usize = 8;
        let object: Vec<u8> = (0..28u8).collect(); // chunks of 8,8,8,4
        for start in 0..=object.len() + 2 {
            for end in start..=object.len() + 4 {
                let direct = if start > object.len() {
                    None
                } else {
                    Some(&object[start..end.min(object.len())])
                };
                match (read_via_chunks(&object, CS, start, end), direct) {
                    (Ok(bytes), Some(expected)) => {
                        assert_eq!(bytes.as_ref(), expected, "range {start}..{end}");
                    }
                    (Err(_), None) => {}
                    (got, _) => panic!("range {start}..{end}: {got:?}"),
                }
            }
        }
        // start > end_exclusive is always an error.
        assert!(read_via_chunks(&object, CS, 5, 4).is_err());
    }

    /// The fetch plan never reads more chunks than the range needs: a range
    /// inside one chunk fetches exactly that chunk.
    #[test]
    fn covering_chunks_is_minimal() {
        assert_eq!(covering_chunks(0, 4, 4, 8).unwrap(), Some(0..=0));
        assert_eq!(covering_chunks(9, 15, 4, 8).unwrap(), Some(1..=1));
        assert_eq!(covering_chunks(7, 9, 4, 8).unwrap(), Some(0..=1));
        // Ranges past the last chunk clamp to it (EOF clamp + total check).
        assert_eq!(covering_chunks(25, 100, 4, 8).unwrap(), Some(3..=3));
        assert_eq!(covering_chunks(40, 50, 4, 8).unwrap(), Some(3..=3));
        // Empty in-bounds reads fetch nothing.
        assert_eq!(covering_chunks(8, 8, 4, 8).unwrap(), None);
        assert_eq!(covering_chunks(0, 0, 4, 8).unwrap(), None);
        // An empty read past the last chunk's nominal offset still needs the
        // last chunk to learn the total for the past-EOF check.
        assert_eq!(covering_chunks(30, 30, 4, 8).unwrap(), Some(3..=3));
    }

    /// A short non-last chunk is data corruption, not an EOF.
    #[test]
    fn short_interior_chunk_errors() {
        let fetched: BTreeMap<usize, Bytes> =
            [(0usize, Bytes::from(vec![0u8; 5]))].into_iter().collect();
        assert!(assemble_chunked_range(&fetched, 3, 8, 0, 4).is_err());
    }
}
