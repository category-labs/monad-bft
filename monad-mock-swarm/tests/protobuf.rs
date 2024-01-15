use monad_consensus::{
    messages::{consensus_message::ConsensusMessage, message::VoteMessage},
    validation::signing::{Unvalidated, Unverified},
};
use monad_consensus_types::{
    bls::BlsSignatureCollection,
    ledger::CommitResult,
    multi_sig::MultiSig,
    payload::{ExecutionArtifacts, FullTransactionList},
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::{
    certificate_signature::{CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey},
    hasher::{Hash, Hasher, HasherType},
    NopSignature,
};
use monad_executor_glue::{
    convert::interface::{deserialize_event, serialize_event},
    ConsensusEvent, MonadEvent,
};
use monad_testutil::{
    proposal::ProposalGen,
    signing::{get_certificate_key, get_key},
    validators::create_keys_w_validators,
};
use monad_types::{BlockId, Epoch, Round, SeqNum};
use monad_validator::{
    epoch_manager::EpochManager,
    leader_election::LeaderElection,
    simple_round_robin::SimpleRoundRobin,
    validator_set::{ValidatorSet, ValidatorSetType},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

type SignatureType = NopSignature;
type SignatureCollectionType = MultiSig<SignatureType>;

#[test]
fn test_consensus_timeout_event() {
    let event = MonadEvent::ConsensusEvent(
        ConsensusEvent::<SignatureType, SignatureCollectionType>::Timeout(
            monad_types::TimeoutVariant::Pacemaker,
        ),
    );

    let buf = serialize_event(&event);
    let rx_event = deserialize_event(&buf);

    assert_eq!(event, rx_event.unwrap());
}

#[test]
fn test_consensus_message_event_vote_multisig() {
    let keypair = get_key::<SignatureType>(0);
    let certkeypair = get_certificate_key::<SignatureCollectionType>(7);
    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        round: Round(1),
        parent_id: BlockId(Hash([43_u8; 32])),
        parent_round: Round(2),
        seq_num: SeqNum(0),
    };
    let vote = Vote {
        vote_info: vi,
        ledger_commit_info: CommitResult::Commit,
    };

    let votemsg: ConsensusMessage<SignatureCollectionType> =
        ConsensusMessage::Vote(VoteMessage::new(vote, &certkeypair));
    let votemsg_hash = HasherType::hash_object(&votemsg);
    let sig = SignatureType::sign(votemsg_hash.as_ref(), &keypair);

    let unverified_votemsg = Unverified::new(Unvalidated::new(votemsg), sig);

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: keypair.pubkey(),
        unverified_message: unverified_votemsg,
    });

    let buf = serialize_event(&event);
    let rx_event = deserialize_event::<SignatureType, SignatureCollectionType>(&buf);

    assert_eq!(event, rx_event.unwrap());
}

#[test]
fn test_consensus_message_event_proposal_bls() {
    let (keys, cert_keys, valset, valmap) = create_keys_w_validators::<
        SignatureType,
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>,
    >(10);
    let mut val_epoch_map = ValidatorsEpochMapping::default();
    val_epoch_map.insert(
        Epoch(1),
        ValidatorSet::new(Vec::from_iter(valset.get_members().clone()))
            .expect("ValidatorData should not have duplicates or invalid entries"),
        ValidatorMapping::new(valmap),
    );
    let epoch_manager = EpochManager::new(SeqNum(2000), Round(50));
    let election = SimpleRoundRobin::new();
    let mut propgen: ProposalGen<
        SignatureType,
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>,
    > = ProposalGen::new();

    let proposal = propgen.next_proposal(
        &keys,
        cert_keys.as_slice(),
        &epoch_manager,
        &val_epoch_map,
        &election,
        FullTransactionList::empty(),
        ExecutionArtifacts::zero(),
    );

    let consensus_proposal_msg = ConsensusMessage::Proposal((*proposal).clone());

    let event = MonadEvent::ConsensusEvent(ConsensusEvent::Message {
        sender: proposal.author().pubkey(),
        unverified_message: Unverified::new(
            Unvalidated::new(consensus_proposal_msg),
            *proposal.author_signature(),
        ),
    });

    let buf = serialize_event(&event);
    let rx_event = deserialize_event::<
        SignatureType,
        BlsSignatureCollection<CertificateSignaturePubKey<SignatureType>>,
    >(&buf);

    assert_eq!(event, rx_event.unwrap());
}
