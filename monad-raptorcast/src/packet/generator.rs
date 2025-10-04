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

use std::ops::Range;

use bytes::BytesMut;
use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Stake};

use super::{BuildError, Chunk, ChunkGenerator, PacketLayout, Recipient, Result};
use crate::util::Redundancy;

pub(crate) struct Replicated<PT: PubKey> {
    // each recipient receives all the same chunks, used by broadcast
    // target and point-to-point target
    recipients: Vec<NodeId<PT>>,
}

impl<PT: PubKey> Replicated<PT> {
    pub fn from_unicast(node_id: NodeId<PT>) -> Self {
        Self {
            recipients: vec![node_id],
        }
    }

    pub fn from_broadcast(recipients: Vec<NodeId<PT>>) -> Self {
        Self { recipients }
    }
}

impl<PT: PubKey> ChunkGenerator<PT> for Replicated<PT> {
    fn unique_chunk_id(&self) -> bool {
        self.recipients.len() <= 1
    }

    fn generate_chunks(
        &self,
        message_len: usize,
        redundancy: Redundancy,
        layout: PacketLayout,
    ) -> Result<Vec<Chunk<PT>>> {
        if self.recipients.is_empty() {
            tracing::warn!("no recipients specified for chunk generator");
            return Ok(vec![]);
        }

        let mut num_symbols = message_len.div_ceil(layout.symbol_len());
        num_symbols = redundancy
            .scale(num_symbols)
            .ok_or(BuildError::TooManyChunks)?;

        let num_chunks = num_symbols * self.recipients.len();

        let mut buffer = BytesMut::zeroed(num_chunks * layout.segment_len());
        let mut all_chunks = Vec::with_capacity(num_chunks);

        for node_id in &self.recipients {
            let recipient = Recipient::new(*node_id);

            // each node get a full copy of all symbols
            split_off_chunks_into(
                &mut all_chunks,
                &mut buffer,
                &recipient,
                0..num_symbols,
                layout.segment_len(),
            );
        }

        debug_assert_eq!(all_chunks.len(), num_chunks);
        debug_assert!(buffer.is_empty());

        Ok(all_chunks)
    }
}

pub(crate) struct Partitioned<PT: PubKey> {
    weighted_nodes: Vec<(NodeId<PT>, Stake)>,
}

impl<PT: PubKey> Partitioned<PT> {
    pub fn from_validator_set(validator_set: Vec<(NodeId<PT>, Stake)>) -> Self {
        Self { weighted_nodes: validator_set }
    }

    pub fn from_homogeneous_peers(peers: Vec<NodeId<PT>>) -> Self {
        let weighted_nodes = peers.into_iter().map(|p| (p, Stake::ONE)).collect();
        Self { weighted_nodes }
    }
}

impl<PT: PubKey> ChunkGenerator<PT> for Partitioned<PT> {
    fn unique_chunk_id(&self) -> bool {
        true
    }

    fn generate_chunks(
        &self,
        message_len: usize,
        redundancy: Redundancy,
        layout: PacketLayout,
    ) -> Result<Vec<Chunk<PT>>> {
        if self.weighted_nodes.is_empty() {
            tracing::warn!("no nodes specified for partitioned chunk generator");
            return Ok(vec![]);
        }

        let mut num_chunks = message_len.div_ceil(layout.symbol_len());

        num_chunks = redundancy
            .scale(num_chunks)
            .ok_or(BuildError::TooManyChunks)?;

        let mut buffer = BytesMut::zeroed(num_chunks * layout.segment_len());
        let mut all_chunks = Vec::with_capacity(num_chunks);

        let total_stake = self.weighted_nodes.iter().map(|(_, s)| *s).sum::<Stake>();
        if total_stake == Stake::ZERO {
            return Err(BuildError::ZeroTotalStake);
        }

        let mut running_stake = Stake::ZERO;
        for (node_id, stake) in &self.weighted_nodes {
            let start_id = (num_chunks as f64 * (running_stake / total_stake)) as usize;
            running_stake += *stake;
            let end_id = (num_chunks as f64 * (running_stake / total_stake)) as usize;
            if start_id == end_id {
                continue;
            }

            let recipient = Recipient::new(*node_id);
            split_off_chunks_into(
                &mut all_chunks,
                &mut buffer,
                &recipient,
                start_id..end_id,
                layout.segment_len(),
            );
        }

        debug_assert_eq!(all_chunks.len(), num_chunks);
        debug_assert!(buffer.is_empty());

        Ok(all_chunks)
    }
}

// each validator gets an additional rounding chunk
pub(crate) struct StakeBasedWithRC<PT: PubKey> {
    validator_set: Vec<(NodeId<PT>, Stake)>,
    // the priority of the rounding chunk
    rounding_chunk_priority: Option<u8>,
}

impl<PT: PubKey> StakeBasedWithRC<PT> {
    #[expect(unused)]
    pub fn from_validator_set(validator_set: Vec<(NodeId<PT>, Stake)>) -> Self {
        Self {
            validator_set,
            rounding_chunk_priority: None,
        }
    }

    #[expect(unused)]
    pub fn with_rc_priority(self, priority: u8) -> Self {
        Self {
            validator_set: self.validator_set,
            rounding_chunk_priority: Some(priority),
        }
    }
}

impl<PT: PubKey> ChunkGenerator<PT> for StakeBasedWithRC<PT> {
    fn unique_chunk_id(&self) -> bool {
        true
    }

    fn generate_chunks(
        &self,
        message_len: usize,
        redundancy: Redundancy,
        layout: PacketLayout,
    ) -> Result<Vec<Chunk<PT>>> {
        let mut num_chunks = message_len.div_ceil(layout.symbol_len());

        num_chunks = redundancy
            .scale(num_chunks)
            .ok_or(BuildError::TooManyChunks)?; // c_h

        let total_stake = self.validator_set.iter().map(|(_, s)| *s).sum::<Stake>();
        if total_stake == Stake::ZERO {
            return Err(BuildError::ZeroTotalStake);
        }

        let num_validators = self.validator_set.len();
        let obligations = self
            .validator_set
            .iter()
            .map(|(_, s)| num_chunks as f64 * (*s / total_stake));

        let mut ic = vec![0usize; num_validators]; // ic := truncate(o)
        let mut rc = vec![false; num_validators]; // rc := fraction(o)

        let mut total_num_chunks = 0;
        for (i, o) in obligations.enumerate() {
            ic[i] = o as usize;
            rc[i] = o.fract() > 0.0;
            total_num_chunks += ic[i] + rc[i] as usize;
        }

        let mut buffer = BytesMut::zeroed(total_num_chunks * layout.segment_len());
        let mut all_chunks = Vec::with_capacity(total_num_chunks);

        let mut curr_chunk_id = 0;
        for (i, (node_id, _stake)) in self.validator_set.iter().enumerate() {
            let next_chunk_id = curr_chunk_id + ic[i] + rc[i] as usize;
            if next_chunk_id == curr_chunk_id {
                continue;
            }

            let recipient = Recipient::new(*node_id);

            split_off_chunks_into(
                &mut all_chunks,
                &mut buffer,
                &recipient,
                curr_chunk_id..next_chunk_id,
                layout.segment_len(),
            );

            if let Some(p) = self.rounding_chunk_priority {
                let rounding_chunk = all_chunks.last_mut();
                if rc[i] {
                    rounding_chunk
                        .expect("must exist with rc>0")
                        .set_priority(p);
                }
            }
            curr_chunk_id = next_chunk_id;
        }

        debug_assert_eq!(all_chunks.len(), total_num_chunks);
        debug_assert!(buffer.is_empty());

        Ok(all_chunks)
    }
}

fn split_off_chunks_into<PT: PubKey>(
    output: &mut Vec<Chunk<PT>>,
    buffer: &mut BytesMut,
    recipient: &Recipient<PT>,
    chunk_ids: Range<usize>,
    segment_len: usize,
) {
    debug_assert!(
        buffer.len() >= segment_len * chunk_ids.len(),
        "insufficient buffer space"
    );

    for chunk_id in chunk_ids {
        let segment = buffer.split_to(segment_len);
        output.push(Chunk::new(chunk_id, recipient.clone(), segment));
    }
}
