use std::marker::PhantomData;

use monad_consensus::validation::signing::Unverified;
use monad_consensus_types::{
    block::Block,
    ledger::LedgerCommitInfo,
    payload::{ExecutionArtifacts, Payload, TransactionList},
    quorum_certificate::{genesis_vote_info, QuorumCertificate},
    signature::SignatureCollection,
    validation::Hasher,
};
use monad_crypto::{
    secp256k1::{Error, KeyPair, PubKey, SecpSignature},
    Signature,
};
use monad_types::{Hash, NodeId, Round};

#[derive(Clone, Default, Debug)]
pub struct MockSignatures {
    pubkey: Vec<PubKey>,
}

impl MockSignatures {
    pub fn with_pubkeys(pubkeys: &[PubKey]) -> Self {
        Self {
            pubkey: pubkeys.to_vec(),
        }
    }
}

impl SignatureCollection for MockSignatures {
    type SignatureType = SecpSignature;

    fn new() -> Self {
        MockSignatures { pubkey: Vec::new() }
    }

    fn get_hash(&self) -> Hash {
        Default::default()
    }

    fn add_signature(&mut self, _s: SecpSignature) {}

    fn verify_signatures(&self, _msg: &[u8]) -> Result<(), Error> {
        Ok(())
    }

    fn get_pubkeys(&self, _msg: &[u8]) -> Result<Vec<PubKey>, Error> {
        Ok(self.pubkey.clone())
    }

    fn num_signatures(&self) -> usize {
        0
    }
}

use sha2::{Digest, Sha256};

pub fn hash<T: SignatureCollection>(b: &Block<T>) -> Hash {
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
    hasher.update(b.qc.signatures.get_hash());

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

pub fn get_genesis_config<'k, H: Hasher, T: SignatureCollection>(
    keys: impl Iterator<Item = &'k KeyPair>,
) -> (Block<T>, T) {
    let genesis_txn = TransactionList::default();
    let genesis_prime_qc = QuorumCertificate::<T>::genesis_prime_qc::<H>();
    let genesis_block = Block::<T>::new::<H>(
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

    let mut sigs = T::new();
    let msg = H::hash_object(&genesis_lci);
    for k in keys {
        let s = T::SignatureType::sign(msg.as_ref(), k);
        sigs.add_signature(s);
    }

    (genesis_block, sigs)
}

pub struct TestSigner<S> {
    _p: PhantomData<S>,
}

impl TestSigner<SecpSignature> {
    pub fn sign_object<T>(o: T, msg: &[u8], key: &KeyPair) -> Unverified<SecpSignature, T> {
        let sig = key.sign(msg);

        Unverified::new(o, sig)
    }
}

pub fn get_key(seed: u64) -> KeyPair {
    let mut hasher = Sha256::new();
    hasher.update(seed.to_le_bytes());
    let mut hash = hasher.finalize();
    KeyPair::from_bytes(&mut hash).unwrap()
}
