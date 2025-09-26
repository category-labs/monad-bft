use std::ops::Range;

use bytes::BytesMut;
use monad_crypto::certificate_signature::PubKey;
use monad_types::{NodeId, Stake};

use super::{BuildError, Chunk, ChunkGenerator, PacketLayout, Recipient, Result};
use crate::util::Redundancy;

pub(crate) struct Uniform<PT: PubKey> {
    // each recipient receives all the same chunks
    recipients: Vec<NodeId<PT>>,
}

impl<PT: PubKey> Uniform<PT> {
    pub fn from_unicast(node_id: NodeId<PT>) -> Self {
        Self {
            recipients: vec![node_id],
        }
    }

    pub fn from_broadcast(recipients: Vec<NodeId<PT>>) -> Self {
        Self { recipients }
    }
}

impl<PT: PubKey> ChunkGenerator<PT> for Uniform<PT> {
    fn generate_chunks(
        &self,
        message_len: usize,
        redundancy: Redundancy,
        layout: PacketLayout,
    ) -> Result<Vec<Chunk<PT>>> {
        let mut num_chunks = message_len.div_ceil(layout.symbol_len());

        num_chunks = redundancy
            .scale(num_chunks)
            .ok_or(BuildError::TooManyChunks)?;
        num_chunks *= self.recipients.len();

        let mut buffer = BytesMut::zeroed(num_chunks * layout.segment_len());
        let mut all_chunks = Vec::with_capacity(num_chunks);

        for node_id in &self.recipients {
            let recipient = Recipient::new(*node_id);

            // each node get a full copy of all symbols
            let chunks =
                split_off_chunks(&mut buffer, &recipient, 0..num_chunks, layout.segment_len());
            all_chunks.extend(chunks);
        }

        Ok(all_chunks)
    }
}

pub(crate) struct StakeBased<PT: PubKey> {
    // validator set sans self
    validator_set: Vec<(NodeId<PT>, Stake)>,
}

impl<PT: PubKey> StakeBased<PT> {
    pub fn from_validator_set(validator_set: Vec<(NodeId<PT>, Stake)>) -> Self {
        Self { validator_set }
    }
}

impl<PT: PubKey> ChunkGenerator<PT> for StakeBased<PT> {
    fn generate_chunks(
        &self,
        message_len: usize,
        redundancy: Redundancy,
        layout: PacketLayout,
    ) -> Result<Vec<Chunk<PT>>> {
        let mut num_chunks = message_len.div_ceil(layout.symbol_len());

        num_chunks = redundancy
            .scale(num_chunks)
            .ok_or(BuildError::TooManyChunks)?;

        let mut buffer = BytesMut::zeroed(num_chunks * layout.segment_len());
        let mut all_chunks = Vec::with_capacity(num_chunks);
        let mut running_stake = Stake::ZERO;
        let total_stake = self.validator_set.iter().map(|(_, s)| *s).sum::<Stake>();
        if total_stake == Stake::ZERO {
            return Err(BuildError::ZeroTotalStake);
        }

        for (node_id, stake) in &self.validator_set {
            let recipient = Recipient::new(*node_id);
            let start_id = (num_chunks as f64 * (running_stake / total_stake)) as usize;
            running_stake += *stake;
            let end_id = (num_chunks as f64 * (running_stake / total_stake)) as usize;

            let chunks = split_off_chunks(
                &mut buffer,
                &recipient,
                start_id..end_id,
                layout.segment_len(),
            );
            all_chunks.extend(chunks);
        }

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
    pub fn from_validator_set(validator_set: Vec<(NodeId<PT>, Stake)>) -> Self {
        Self {
            validator_set,
            rounding_chunk_priority: None,
        }
    }

    pub fn with_rc_priority(self, priority: u8) -> Self {
        Self {
            validator_set: self.validator_set,
            rounding_chunk_priority: Some(priority),
        }
    }
}

impl<PT: PubKey> ChunkGenerator<PT> for StakeBasedWithRC<PT> {
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
            let recipient = Recipient::new(*node_id);
            let next_chunk_id = curr_chunk_id + ic[i] + rc[i] as usize;

            let mut chunks = split_off_chunks(
                &mut buffer,
                &recipient,
                curr_chunk_id..next_chunk_id,
                layout.segment_len(),
            );

            if let Some(p) = self.rounding_chunk_priority {
                let rounding_chunk = chunks.last_mut();
                if rc[i] {
                    rounding_chunk
                        .expect("must exists with rc>0")
                        .set_priority(p);
                }
            }
            all_chunks.extend(chunks);

            curr_chunk_id = next_chunk_id;
        }

        Ok(all_chunks)
    }
}

fn split_off_chunks<PT: PubKey>(
    buffer: &mut BytesMut,
    recipient: &Recipient<PT>,
    chunk_ids: Range<usize>,
    segment_len: usize,
) -> Vec<Chunk<PT>> {
    debug_assert!(buffer.len() >= segment_len * chunk_ids.len());

    chunk_ids
        .map(|i| {
            let segment = buffer.split_to(segment_len);
            Chunk::new(i, recipient.clone(), segment)
        })
        .collect()
}
