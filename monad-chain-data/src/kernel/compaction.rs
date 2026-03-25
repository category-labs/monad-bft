use std::collections::BTreeMap;

use crate::error::{Error, Result};

pub fn sealed_ranges(
    from_next: u64,
    to_next: u64,
    span: u64,
    align: impl Fn(u64) -> u64,
) -> Vec<u64> {
    if to_next <= from_next {
        return Vec::new();
    }
    let mut current = align(from_next);
    let mut out = Vec::new();
    loop {
        let end = current.saturating_add(span);
        if end > to_next {
            break;
        }
        if end > from_next {
            out.push(current);
        }
        current = current.saturating_add(span);
    }
    out
}

pub fn compact_directory_sub_bucket_from_fragments<F>(
    fragments: &[F],
    start_block: impl Fn(&F) -> u64,
    first_id: impl Fn(&F) -> u64,
    end_id_exclusive: impl Fn(&F) -> u64,
) -> Option<(u64, Vec<u64>)> {
    if fragments.is_empty() {
        return None;
    }
    let mut first_ids = Vec::with_capacity(fragments.len() + 1);
    for fragment in fragments {
        first_ids.push(first_id(fragment));
    }
    let sentinel = fragments.last().map(end_id_exclusive)?;
    first_ids.push(sentinel);
    Some((start_block(&fragments[0]), first_ids))
}

pub fn compact_directory_bucket_from_sub_buckets<S>(
    sub_buckets: &[S],
    start_block: impl Fn(&S) -> u64,
    count: impl Fn(&S) -> usize,
    first_id_at: impl Fn(&S, usize) -> u64,
    missing_sentinel_error: &'static str,
    inconsistent_error: &'static str,
    missing_start_error: &'static str,
) -> Result<Option<(u64, Vec<u64>)>> {
    let mut boundaries = BTreeMap::<u64, u64>::new();
    let mut sentinel = None::<u64>;

    for sub_bucket in sub_buckets {
        for index in 0..count(sub_bucket).saturating_sub(1) {
            let block_num = start_block(sub_bucket) + index as u64;
            let first_id = first_id_at(sub_bucket, index);
            match boundaries.entry(block_num) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(first_id);
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    if *entry.get() != first_id {
                        return Err(Error::Decode(inconsistent_error));
                    }
                }
            }
        }

        let last_index = count(sub_bucket)
            .checked_sub(1)
            .ok_or(Error::Decode(missing_sentinel_error))?;
        let last = first_id_at(sub_bucket, last_index);
        sentinel = Some(match sentinel {
            Some(current) => current.max(last),
            None => last,
        });
    }

    let Some(sentinel) = sentinel else {
        return Ok(None);
    };
    if boundaries.is_empty() {
        return Ok(None);
    }

    let start_block = *boundaries
        .keys()
        .next()
        .ok_or(Error::Decode(missing_start_error))?;
    let mut first_ids = boundaries.into_values().collect::<Vec<_>>();
    first_ids.push(sentinel);
    Ok(Some((start_block, first_ids)))
}
