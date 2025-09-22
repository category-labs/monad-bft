mod assembler;
mod generator;

use std::{collections::HashMap, net::SocketAddr};

pub(crate) use assembler::{assemble, BroadcastType, Chunk, PacketLayout, Recipient};
use bytes::Bytes;
use monad_crypto::certificate_signature::{
    CertificateKeyPair as _, CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_types::NodeId;
use rand::seq::SliceRandom as _;

use crate::util::{BuildTarget, Redundancy};

#[derive(Debug)]
pub struct UdpMessage {
    pub dest: SocketAddr,
    pub payload: Bytes,
    pub stride: usize,
    pub priority: Option<u8>, // 0 - highest priority
}

#[derive(Debug)]
pub enum BuildError {
    // merkle tree depth does not fit in u4
    MerkleTreeTooDeep,
    // chunk id does not fit in u16
    ChunkIdOverflow,
    // failed to create encoder
    EncoderCreationFailed,
    // too many chunks
    TooManyChunks,
    // total stake is zero
    ZeroTotalStake,
}

pub(crate) trait ChunkGenerator<PT: PubKey> {
    fn generate_chunks(
        &self,
        message_len: usize,
        redundancy: Redundancy,
        layout: PacketLayout,
    ) -> Result<Vec<Chunk<PT>>>;
}

pub(crate) trait PeerLookup<PT: PubKey> {
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr>;
}

type Result<A, E = BuildError> = std::result::Result<A, E>;

// A compatible interface to crate::udp::build_messages that uses
// packet::assemble underneath.
#[allow(clippy::too_many_arguments)]
pub fn build_messages<ST, R>(
    key: &ST::KeyPairType,
    segment_size: u16,
    app_message: Bytes,
    redundancy: Redundancy,
    epoch_no: u64,
    unix_ts_ms: u64,
    build_target: BuildTarget<ST>,
    known_addresses: &HashMap<NodeId<CertificateSignaturePubKey<ST>>, SocketAddr>,
    rng: &mut R,
) -> Vec<(SocketAddr, Bytes)>
where
    ST: CertificateSignatureRecoverable,
    R: rand::Rng,
{
    use generator::{StakeBased, Uniform};

    let broadcast_type = match build_target {
        BuildTarget::Broadcast { .. } => BroadcastType::Primary,
        BuildTarget::Raptorcast { .. } => BroadcastType::Primary,
        BuildTarget::FullNodeRaptorCast { .. } => BroadcastType::Secondary,
        _ => BroadcastType::Unicast,
    };
    let peer_lookup = known_addresses;

    let generator: Box<dyn ChunkGenerator<_>> = match build_target {
        BuildTarget::PointToPoint(to) => Box::new(Uniform::from_unicast(*to)),
        BuildTarget::Broadcast(nodes_view) => Box::new(Uniform::from_broadcast(
            nodes_view.iter().copied().collect(),
        )),
        BuildTarget::Raptorcast(validators_view) => {
            let mut validator_set: Vec<_> = validators_view
                .iter()
                .map(|(node_id, stake)| (*node_id, stake))
                .collect();
            validator_set.shuffle(rng);
            Box::new(StakeBased::from_validator_set(validator_set))
        }
        BuildTarget::FullNodeRaptorCast(group) => {
            let self_id = NodeId::new(key.pubkey());
            let seed = rng.gen::<usize>();
            let nodes = group
                .iter_skip_self_and_author(&self_id, seed)
                .copied()
                .collect();
            Box::new(Uniform::from_broadcast(nodes))
        }
    };

    let packets = assemble::<ST, _, _>(
        key,
        segment_size as usize,
        app_message,
        redundancy,
        epoch_no,
        unix_ts_ms,
        broadcast_type,
        &*generator,
        peer_lookup,
    );

    let packets = match packets {
        Ok(p) => p,
        Err(e) => {
            tracing::error!("Failed to build packets: {:?}", e);
            return vec![];
        }
    };

    packets
        .into_iter()
        .map(|msg| (msg.dest, msg.payload))
        .collect()
}

impl<PT: PubKey> PeerLookup<PT> for HashMap<NodeId<PT>, SocketAddr> {
    fn lookup(&self, node_id: &NodeId<PT>) -> Option<SocketAddr> {
        self.get(node_id).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::{build_messages as build_new, *};
    use crate::udp::build_messages as build_old;
}
