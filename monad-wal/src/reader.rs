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

use std::{
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{BufReader, Read},
    marker::PhantomData,
    ops::RangeInclusive,
    path::PathBuf,
};

use monad_types::Deserializable;

use crate::{
    wal::{EventHeaderType, EVENT_HEADER_LEN},
    WALError,
};

const WAL_READ_BUFFER_SIZE: usize = 1024 * 1024; // 1MB

/// Config for a write-ahead-log
#[derive(Clone)]
pub struct WALReaderConfig<M> {
    file_path: PathBuf,

    _marker: PhantomData<M>,
}

impl<M> WALReaderConfig<M>
where
    M: Deserializable<[u8]> + Debug,
{
    pub fn new(file_path: PathBuf) -> Self {
        Self {
            file_path,
            _marker: PhantomData,
        }
    }

    pub fn build(self) -> Result<WALReader<M>, WALError> {
        let file = OpenOptions::new().read(true).open(self.file_path)?;

        Ok(WALReader {
            _marker: PhantomData,
            reader: BufReader::with_capacity(WAL_READ_BUFFER_SIZE, file),
        })
    }
}

#[derive(Debug)]
pub struct WALReader<M> {
    _marker: PhantomData<M>,
    reader: BufReader<File>,
}

impl<M> WALReader<M>
where
    M: Deserializable<[u8]> + Debug,
{
    pub fn load_one_raw(&mut self) -> Result<Vec<u8>, std::io::Error> {
        let mut len_buf = [0u8; EVENT_HEADER_LEN];
        self.reader.read_exact(&mut len_buf)?;
        let len = EventHeaderType::from_le_bytes(len_buf);
        let mut buf = vec![0u8; len as usize];
        self.reader.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn load_one(&mut self) -> Result<M, WALError> {
        let buf = self.load_one_raw()?;
        let msg = M::deserialize(&buf).map_err(|e| WALError::DeserError(Box::new(e)))?;
        Ok(msg)
    }
}

pub fn events_iter_raw<M>(mut reader: WALReader<M>) -> impl Iterator<Item = Vec<u8>>
where
    M: Deserializable<[u8]> + Debug,
{
    std::iter::repeat(()).map_while(move |()| match reader.load_one_raw() {
        Ok(event) => Some(event),
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => None,
        Err(err) => panic!("error reading WAL: {:?}", err),
    })
}

pub fn events_iter<M>(mut reader: WALReader<M>) -> impl Iterator<Item = M>
where
    M: Deserializable<[u8]> + Debug,
{
    std::iter::repeat(()).map_while(move |()| match reader.load_one() {
        Ok(event) => Some(event),
        Err(WALError::IOError(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => None,
        Err(err) => panic!("error reading WAL: {:?}", err),
    })
}

pub fn events_iter_in_range<E, Ts>(
    events_iters: impl Iterator<Item = impl Iterator<Item = E>>,
    event_to_ts: impl Fn(&E) -> Ts + Copy,
    range: RangeInclusive<Ts>,
) -> impl Iterator<Item = E>
where
    Ts: Copy + Ord + 'static,
{
    let end = *range.end();
    let mut fused_events = events_iters
        .map(|events_iter| events_iter.peekable())
        // we can immediately drop any logs that only contain events past the end time
        // equivalently, we only keep logs that contain events before the end time
        .filter_map(|mut events_iter| {
            let first_event = events_iter.peek()?;
            let first_event_ts = event_to_ts(first_event);
            if first_event_ts <= end {
                Some((first_event_ts, events_iter))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // sort logs by first event timestamp
    fused_events.sort_by_key(|(first_event_ts, _)| *first_event_ts);

    let start = *range.start();
    let truncate_before = fused_events
        .iter()
        // find the last log that has its first event timestamp <= start time
        // the significance of this is that we can drop all logs before it
        .rposition(|(first_event_ts, _)| *first_event_ts <= start)
        // if all logs have first event timestamp > start time, we can't drop any
        .unwrap_or(0);
    if truncate_before > 0 {
        // drop all logs before that log
        fused_events.drain(0..truncate_before);
    }

    fused_events
        .into_iter()
        .map(|(_, events)| events)
        .flatten()
        .skip_while(move |event| event_to_ts(event) < start)
        .take_while(move |event| event_to_ts(event) <= end)
}

#[cfg(test)]
mod test {
    use std::ops::RangeInclusive;

    use test_case::test_case;

    use crate::reader::events_iter_in_range;

    #[test_case(
        vec![
            vec![1, 2, 3],
            vec![4, 5, 6],
            vec![7, 8, 9],
            vec![10, 11, 12],
        ];
        "events 1"
    )]
    #[test_case(
        vec![
            vec![],
            vec![1, 2, 3],
            vec![10, 11, 12],
            vec![4, 5, 6],
        ];
        "events 2"
    )]
    fn test_events_iter_all(logs: Vec<Vec<usize>>) {
        let ranges = {
            let sorted_timestamps = {
                let mut timestamps = logs.iter().flatten().copied().collect::<Vec<_>>();
                timestamps.push(usize::MIN);
                timestamps.push(usize::MAX);
                timestamps.sort();
                timestamps
            };
            let mut ranges = Vec::new();
            for &start in &sorted_timestamps {
                for &end in &sorted_timestamps {
                    if start > end {
                        continue;
                    }
                    ranges.push(start..=end);
                }
            }
            ranges
        };

        for range in ranges {
            assert_events_iter_range(logs.clone(), range);
        }
    }

    fn assert_events_iter_range(logs: Vec<Vec<usize>>, range: RangeInclusive<usize>) {
        let mut expected_events: Vec<_> = logs
            .iter()
            .flatten()
            .copied()
            .filter(|i| range.contains(i))
            .collect();
        expected_events.sort();
        let events: Vec<_> = events_iter_in_range(
            logs.into_iter().map(|log| log.into_iter()),
            |i| *i,
            range.clone(),
        )
        .collect();
        assert_eq!(expected_events, events, "failed for range {:?}", range);
    }
}
