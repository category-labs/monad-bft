use std::marker::PhantomData;

use monad_consensus::validation::signing::Unverified;
use monad_consensus_types::{
    block::{Block, BlockType},
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    ledger::LedgerCommitInfo,
    payload::{ExecutionArtifacts, Payload, TransactionList},
    quorum_certificate::{genesis_vote_info, QuorumCertificate},
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    validation::{Hashable, Hasher, Sha256Hash},
    voting::ValidatorMapping,
};
use monad_crypto::secp256k1::{Error as SecpError, KeyPair, PubKey, SecpSignature};
use monad_types::{Hash, NodeId, Round};
use sha2::{Digest, Sha256};

#[derive(Clone, Default, Debug)]
pub struct MockSignatures {
    pubkey: Vec<PubKey>,
}

#[derive(Debug)]
struct MockSignatureError;

impl std::fmt::Display for MockSignatureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for MockSignatureError {}

impl MockSignatures {
    pub fn with_pubkeys(pubkeys: &[PubKey]) -> Self {
        Self {
            pubkey: pubkeys.to_vec(),
        }
    }
}

impl Hashable for MockSignatures {
    fn hash<H: Hasher>(&self, state: &mut H) {}
}

impl SignatureCollection for MockSignatures {
    type SignatureError = SecpError;
    type SignatureType = SecpSignature;

    fn new(
        sigs: Vec<(NodeId, Self::SignatureType)>,
        validator_mapping: &monad_consensus_types::voting::ValidatorMapping<
            <Self::SignatureType as monad_consensus_types::certificate_signature::CertificateSignature>::KeyPairType,
        >,
        msg: &[u8],
    ) -> Result<Self, Self::SignatureError> {
        Ok(Self { pubkey: Vec::new() })
    }

    fn get_hash<H: Hasher>(&self) -> Hash {
        Default::default()
    }

    fn verify(
        &self,
        validator_mapping: &monad_consensus_types::voting::ValidatorMapping<
            <Self::SignatureType as monad_consensus_types::certificate_signature::CertificateSignature>::KeyPairType,
        >,
        msg: &[u8],
    ) -> Result<Vec<NodeId>, Self::SignatureError> {
        Ok(self.pubkey.iter().map(|pubkey| NodeId(*pubkey)).collect())
    }

    fn num_signatures(&self) -> usize {
        self.pubkey.len()
    }
}

pub fn hash<T: SignatureCollection>(b: &Block<T>) -> Hash {
    // FIXME: we should replace this with the Sha256Hash
    let mut hasher = sha2::Sha256::new();
    hasher.update(b.author.0.bytes());
    hasher.update(b.round);
    hasher.update(&b.payload.txns.0);
    hasher.update(b.payload.header.parent_hash);
    hasher.update(b.payload.header.state_root);
    hasher.update(b.payload.header.transactions_root);
    hasher.update(b.payload.header.receipts_root);
    hasher.update(b.payload.header.logs_bloom);
    hasher.update(b.payload.header.gas_used);
    hasher.update(b.qc.info.vote.id.0);
    hasher.update(b.qc.signatures.get_hash::<Sha256Hash>());

    Hash(hasher.finalize().into())
}

pub fn node_id() -> NodeId {
    let mut privkey: [u8; 32] = [127; 32];
    let keypair = KeyPair::from_bytes(&mut privkey).unwrap();
    NodeId(keypair.pubkey())
}

pub fn create_keys(num_keys: u32) -> Vec<KeyPair> {
    let mut res = Vec::new();
    for i in 0..num_keys {
        let keypair = get_key(i.into());
        res.push(keypair);
    }

    res
}

pub fn create_certificate_keys<SCT: SignatureCollection>(
    num_keys: u32,
) -> Vec<SignatureCollectionKeyPairType<SCT>> {
    let mut res = Vec::new();
    for i in 0..num_keys {
        let keypair = get_certificate_key::<SCT>(i.into());
        res.push(keypair);
    }
    res
}

pub fn get_genesis_config<'k, H, SCT>(
    keys: impl Iterator<Item = &'k (NodeId, &'k SignatureCollectionKeyPairType<SCT>)>,
    validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
) -> (Block<SCT>, SCT)
where
    H: Hasher,
    SCT: SignatureCollection,
{
    let genesis_txn = TransactionList::default();
    let genesis_prime_qc = QuorumCertificate::<SCT>::genesis_prime_qc::<H>();
    let genesis_block = Block::<SCT>::new::<H>(
        // FIXME init from genesis config, don't use random key
        NodeId(KeyPair::from_bytes(&mut [0xBE_u8; 32]).unwrap().pubkey()),
        Round(0),
        &Payload {
            txns: genesis_txn,
            header: ExecutionArtifacts::zero(),
        },
        &genesis_prime_qc,
    );

    let genesis_lci = LedgerCommitInfo::new::<H>(None, &genesis_vote_info(genesis_block.get_id()));
    let msg = H::hash_object(&genesis_lci);

    let mut sigs = Vec::new();
    for (node_id, k) in keys {
        let sig = SCT::SignatureType::sign(msg.as_ref(), k);
        sigs.push((*node_id, sig))
    }

    let sigs = SCT::new(sigs, validator_mapping, msg.as_ref()).unwrap();
    (genesis_block, sigs)
}

pub struct TestSigner<S> {
    _p: PhantomData<S>,
}

impl TestSigner<SecpSignature> {
    pub fn sign_object<H: Hasher, T: Hashable>(
        o: T,
        key: &KeyPair,
    ) -> Unverified<SecpSignature, T> {
        let msg = H::hash_object(&o);
        let sig = key.sign(msg.as_ref());

        Unverified::new(o, sig)
    }
}

pub fn get_key(seed: u64) -> KeyPair {
    let mut hasher = Sha256::new();
    hasher.update(seed.to_le_bytes());
    let mut hash = hasher.finalize();
    KeyPair::from_bytes(&mut hash).unwrap()
}

pub fn get_certificate_key<SCT: SignatureCollection>(
    seed: u64,
) -> SignatureCollectionKeyPairType<SCT> {
    let mut hasher = Sha256::new();
    hasher.update(seed.to_le_bytes());
    let mut hash = hasher.finalize();
    <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::from_bytes(&mut hash).unwrap()
}
