use bytes::Bytes;
use monad_compress::{brotli::BrotliCompression, CompressionAlgo};
use monad_consensus::messages::consensus_message::ConsensusMessage;
use monad_consensus_types::{
    bls::BlsSignatureCollection,
    payload::{ExecutionArtifacts, TransactionHashList},
};
use monad_crypto::secp256k1::SecpSignature;
use monad_state::VerifiedMonadMessage;
use monad_testutil::{proposal::ProposalGen, validators::create_keys_w_validators};
use monad_types::Serializable;
use monad_validator::{
    leader_election::LeaderElection, simple_round_robin::SimpleRoundRobin,
    validator_set::ValidatorSetType,
};
use peak_alloc::PeakAlloc;
use rand_chacha::rand_core::{RngCore, SeedableRng};

#[global_allocator]
static PEAK_ALLOC: PeakAlloc = PeakAlloc;

fn main() {
    // created a serialized proposal message
    // transaction hashes follow a random distribution
    let mut transaction_hashes = [0x00_u8; 32 * 5000];
    let mut rng = rand_chacha::ChaCha8Rng::from_entropy();
    rng.fill_bytes(&mut transaction_hashes);

    let (keys, cert_keys, valset, valmap) = create_keys_w_validators::<BlsSignatureCollection>(10);
    let election = SimpleRoundRobin::new();
    let mut propgen: ProposalGen<SecpSignature, BlsSignatureCollection> = ProposalGen::new();

    let proposal = propgen
        .next_proposal(
            &keys,
            cert_keys.as_slice(),
            &valset,
            &election,
            &valmap,
            TransactionHashList::new(transaction_hashes.to_vec().into()),
            ExecutionArtifacts::zero(),
        )
        .destructure()
        .2;

    let leader_key = keys
        .iter()
        .find(|k| {
            k.pubkey()
                == election
                    .get_leader(proposal.block.round, valset.get_list())
                    .0
        })
        .expect("key in valset");

    let proposal: VerifiedMonadMessage<SecpSignature, BlsSignatureCollection> =
        ConsensusMessage::<BlsSignatureCollection>::Proposal(proposal)
            .sign(leader_key)
            .into();

    let proposal_bytes: Bytes = proposal.serialize();

    println!(
        "current mem usage before compression {:?} MB",
        PEAK_ALLOC.current_usage_as_mb()
    );
    println!(
        "peak mem usage before compression {:?} MB",
        PEAK_ALLOC.peak_usage_as_mb()
    );
    // compress proposal
    let algo = BrotliCompression::new(11, 22, Vec::new());
    let mut compressed = Vec::new();
    algo.compress(&proposal_bytes, &mut compressed)
        .expect("compression success");

    // report the peak memory usage
    println!(
        "current mem usage after compression {:?} MB",
        PEAK_ALLOC.current_usage_as_mb()
    );
    println!(
        "peak mem usage after compression {:?} MB",
        PEAK_ALLOC.peak_usage_as_mb()
    );

    println!("uncompressed size {:?}", proposal_bytes.len());
    println!("compressed size {:?}", compressed.len());
}
