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
    collections::VecDeque,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{BufReader, Read},
    marker::PhantomData,
    ops::RangeInclusive,
    path::PathBuf,
};

use monad_types::Deserializable;

use crate::{
    wal::{discover_chunks, DiscoveredChunk, EventHeaderType, EVENT_HEADER_LEN},
    WALError,
};

const WAL_READ_BUFFER_SIZE: usize = 1024 * 1024; // 1MB

#[derive(Debug)]
struct ChunkReader {
    reader: BufReader<File>,
    exhausted: bool,
}

impl ChunkReader {
    fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    fn load_one_raw(&mut self) -> Result<Option<Vec<u8>>, std::io::Error> {
        if self.exhausted {
            return Ok(None);
        }

        let Some(len_buf) = self.read_header()? else {
            self.exhausted = true;
            return Ok(None);
        };

        let len = EventHeaderType::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        self.reader.read_exact(&mut buf)?;
        Ok(Some(buf))
    }

    fn read_header(&mut self) -> Result<Option<[u8; EVENT_HEADER_LEN]>, std::io::Error> {
        let mut len_buf = [0u8; EVENT_HEADER_LEN];
        let bytes_read = self.reader.read(&mut len_buf)?;
        if bytes_read == 0 {
            return Ok(None);
        }

        self.reader.read_exact(&mut len_buf[bytes_read..])?;
        Ok(Some(len_buf))
    }
}

impl TryFrom<DiscoveredChunk> for ChunkReader {
    type Error = WALError;

    fn try_from(chunk: DiscoveredChunk) -> Result<Self, Self::Error> {
        let file = OpenOptions::new().read(true).open(chunk.path)?;
        Ok(Self {
            reader: BufReader::with_capacity(WAL_READ_BUFFER_SIZE, file),
            exhausted: false,
        })
    }
}

#[derive(Debug)]
pub struct WALReader<M> {
    _marker: PhantomData<M>,
    readers: VecDeque<ChunkReader>,
}

impl<M> WALReader<M>
where
    M: Deserializable<[u8]> + Debug,
{
    pub fn new(dir_path: PathBuf) -> Result<Self, WALError> {
        let discovered = discover_chunks(&dir_path)?;
        Self::from_discovered_chunks(discovered)
    }

    pub fn from_discovered_chunks(chunks: Vec<DiscoveredChunk>) -> Result<Self, WALError> {
        let readers = chunks
            .into_iter()
            .map(ChunkReader::try_from)
            .collect::<Result<VecDeque<_>, _>>()?;

        Ok(Self {
            _marker: PhantomData,
            readers,
        })
    }

    pub fn load_one_raw(&mut self) -> Result<Option<Vec<u8>>, std::io::Error> {
        loop {
            let Some(reader) = self.readers.front_mut() else {
                return Ok(None);
            };
            if reader.is_exhausted() {
                self.readers.pop_front();
                continue;
            }

            match reader.load_one_raw() {
                Ok(Some(buf)) => return Ok(Some(buf)),
                Ok(None) => {
                    self.readers.pop_front();
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn load_one(&mut self) -> Result<Option<M>, WALError> {
        self.load_one_raw()?
            .map(|buf| M::deserialize(&buf).map_err(|e| WALError::DeserError(Box::new(e))))
            .transpose()
    }
}

pub fn events_iter_raw<M>(mut reader: WALReader<M>) -> impl Iterator<Item = Vec<u8>>
where
    M: Deserializable<[u8]> + Debug,
{
    std::iter::from_fn(move || match reader.load_one_raw() {
        Ok(event) => event,
        Err(err) => panic!("error reading WAL: {:?}", err),
    })
}

pub fn events_iter<M>(mut reader: WALReader<M>) -> impl Iterator<Item = M>
where
    M: Deserializable<[u8]> + Debug,
{
    std::iter::from_fn(move || match reader.load_one() {
        Ok(event) => event,
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
        .flat_map(|(_, events)| events)
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
