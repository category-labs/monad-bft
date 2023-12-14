use monad_consensus::{
    messages::{consensus_message::ConsensusMessage, message::VoteMessage},
    validation::signing::Unverified,
};
use monad_consensus_types::{
    block::BlockType,
    bls::BlsSignatureCollection,
    ledger::LedgerCommitInfo,
    multi_sig::MultiSig,
    payload::{ExecutionArtifacts, TransactionHashList},
    quorum_certificate::{genesis_vote_info, QuorumCertificate},
    transaction_validator::MockValidator,
    voting::{Vote, VoteInfo},
};
use monad_crypto::{
    hasher::{Hash, Hasher, HasherType},
    secp256k1::{KeyPair, SecpSignature},
};
use monad_executor_glue::{
    convert::interface::{deserialize_event, serialize_event},
    ConsensusEvent, MonadEvent,
};
use monad_testutil::{
    proposal::ProposalGen,
    signing::{get_certificate_key, get_genesis_config, get_key},
    validators::create_keys_w_validators,
};
use monad_types::{BlockId, NodeId, Round, SeqNum};
use monad_validator::{leader_election::LeaderElection, simple_round_robin::SimpleRoundRobin};

type SignatureCollectionType = MultiSig<SecpSignature>;

#[test]
fn test_consensus_timeout_event() {
    let event = MonadEvent::ConsensusEvent(
        ConsensusEvent::<SecpSignature, SignatureCollectionType>::Timeout(
            monad_types::TimeoutVariant::Pacemaker,
        ),
    );

    let buf = serialize_event(&event);
    let rx_event = deserialize_event(&buf);

    assert_eq!(event, rx_event.unwrap());
}

#[test]
fn test_consensus_message_event_vote_multisig() {
    let keypair: KeyPair = get_key(0);
    let certkeypair = get_certificate_key::<SignatureCollectionType>(7);
    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(2),
        seq_num: SeqNum(0),
    };
    let lci: LedgerCommitInfo = LedgerCommitInfo {
        commit_state_hash: None,
        vote_info_hash: Hash([42_u8; 32]),
    };
    let vote = Vote {
        vote_info: vi,
        ledger_commit_info: lci,
    };

    let votemsg: ConsensusMessage<SignatureCollectionType> =
        ConsensusMessage::Vote(VoteMessage::new(vote, &certkeypair));
    let votemsg_hash = HasherType::hash_object(&votemsg);
    let sig = keypair.sign(votemsg_hash.as_ref());

    let unverified_votemsg = Unverified::new(votemsg, sig);

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: keypair.pubkey(),
        unverified_message: unverified_votemsg,
    });

    let buf = serialize_event(&event);
    let rx_event = deserialize_event::<SecpSignature, SignatureCollectionType>(&buf);

    assert_eq!(event, rx_event.unwrap());
}

#[test]
fn test_consensus_message_event_proposal_bls() {
    let (keys, cert_keys, valset, valmap) = create_keys_w_validators::<BlsSignatureCollection>(10);
    let voting_keys = keys
        .iter()
        .map(|k| NodeId(k.pubkey()))
        .zip(cert_keys.iter())
        .collect::<Vec<_>>();
    let (genesis_block, genesis_sigs) = get_genesis_config::<BlsSignatureCollection, MockValidator>(
        voting_keys.iter(),
        &valmap,
        &MockValidator {},
    );
    let genesis_qc =
        QuorumCertificate::genesis_qc(genesis_vote_info(genesis_block.get_id()), genesis_sigs);
    let election = SimpleRoundRobin::new();
    let mut propgen: ProposalGen<SecpSignature, BlsSignatureCollection> =
        ProposalGen::new(genesis_qc);

    let proposal = propgen.next_proposal(
        &keys,
        cert_keys.as_slice(),
        &valset,
        &election,
        &valmap,
        TransactionHashList::empty(),
        ExecutionArtifacts::zero(),
    );

    let consensus_proposal_msg = ConsensusMessage::Proposal((*proposal).clone());

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: proposal.author().0,
        unverified_message: Unverified::new(consensus_proposal_msg, *proposal.author_signature()),
    });

    let buf = serialize_event(&event);
    let rx_event = deserialize_event::<SecpSignature, BlsSignatureCollection>(&buf);

    assert_eq!(event, rx_event.unwrap());
}
