use monad_compress::{brotli::BrotliCompression, CompressionAlgo};
use monad_consensus::messages::consensus_message::ConsensusMessage;
use monad_consensus_types::{
    block::BlockType,
    bls::BlsSignatureCollection,
    payload::{ExecutionArtifacts, TransactionHashList},
    quorum_certificate::{genesis_vote_info, QuorumCertificate},
    transaction_validator::MockValidator,
};
use monad_crypto::{hasher::HasherType, secp256k1::SecpSignature};
use monad_state::VerifiedMonadMessage;
use monad_testutil::{
    proposal::ProposalGen, signing::get_genesis_config, validators::create_keys_w_validators,
};
use monad_types::{NodeId, Serializable};
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
    let voting_keys = keys
        .iter()
        .map(|k| NodeId(k.pubkey()))
        .zip(cert_keys.iter())
        .collect::<Vec<_>>();
    let (genesis_block, genesis_sigs) = get_genesis_config::<
        HasherType,
        BlsSignatureCollection,
        MockValidator,
    >(voting_keys.iter(), &valmap, &MockValidator {});
    let genesis_qc = QuorumCertificate::genesis_qc::<HasherType>(
        genesis_vote_info(genesis_block.get_id()),
        genesis_sigs,
    );
    let election = SimpleRoundRobin::new();
    let mut propgen: ProposalGen<SecpSignature, BlsSignatureCollection> =
        ProposalGen::new(genesis_qc);

    let proposal = propgen
        .next_proposal(
            &keys,
            cert_keys.as_slice(),
            &valset,
            &election,
            &valmap,
            TransactionHashList::new(transaction_hashes.to_vec()),
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
            .sign::<HasherType, SecpSignature>(leader_key)
            .into();

    let proposal_bytes: Vec<u8> = proposal.serialize();

    println!(
        "current mem usage before compression {:?} MB",
        PEAK_ALLOC.current_usage_as_mb()
    );
    println!(
        "peak mem usage before compression {:?} MB",
        PEAK_ALLOC.peak_usage_as_mb()
    );
    // compress proposal
    let algo = BrotliCompression::new(11, 22);
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
