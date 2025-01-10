use monad_consensus_types::{
    block::{Block, BlockKind},
    ledger::CommitResult,
    payload::{ExecutionProtocol, Payload, RandaoReveal, TransactionPayload},
    quorum_certificate::{QcInfo, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::{ValidatorMapping, Vote, VoteInfo},
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable,
    },
    hasher::{Hash, Hasher, HasherType},
};
use monad_types::{BlockId, Epoch, NodeId, Round, SeqNum};

pub fn setup_block<ST, SCT>(
    author: NodeId<CertificateSignaturePubKey<ST>>,
    block_round: Round,
    qc_round: Round,
    parent_id: BlockId,
    txns: TransactionPayload,
    execution: ExecutionProtocol,
    certkeys: &[SignatureCollectionKeyPairType<SCT>],
    validator_mapping: &ValidatorMapping<
        CertificateSignaturePubKey<ST>,
        SignatureCollectionKeyPairType<SCT>,
    >,
) -> (Block<SCT>, Payload)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    let vi = VoteInfo {
        id: BlockId(Hash([42_u8; 32])),
        epoch: Epoch(1),
        round: qc_round,
        parent_id,
        parent_round: Round(0),
        seq_num: SeqNum(0),
        timestamp: 0,
    };
    let qcinfo = QcInfo {
        vote: Vote {
            vote_info: vi,
            ledger_commit_info: CommitResult::NoCommit,
        },
    };
    let qcinfo_hash = HasherType::hash_object(&qcinfo.vote);

    let mut sigs = Vec::new();
    for certkey in certkeys.iter() {
        let sig = <SCT::SignatureType as CertificateSignature>::sign(qcinfo_hash.as_ref(), certkey);

        for (node_id, pubkey) in validator_mapping.map.iter() {
            if *pubkey == certkey.pubkey() {
                sigs.push((*node_id, sig));
            }
        }
    }

    let sig_col = SCT::new(sigs, validator_mapping, qcinfo_hash.as_ref()).unwrap();
    let qc = QuorumCertificate::<SCT>::new(qcinfo, sig_col);

    let block_kind = match txns {
        TransactionPayload::List(_) => BlockKind::Executable,
        TransactionPayload::Null => BlockKind::Null,
    };
    let payload = Payload { txns };

    (
        Block::<SCT>::new(
            author,
            0,
            Epoch(1),
            block_round,
            &ExecutionProtocol {
                randao_reveal: RandaoReveal::new::<SCT::SignatureType>(block_round, &certkeys[0]),
                ..execution
            },
            payload.get_id(),
            block_kind,
            &qc,
        ),
        payload,
    )
}
