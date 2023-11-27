use std::{collections::HashSet, marker::PhantomData};

use monad_consensus::validation::signing::Unverified;
use monad_consensus_types::{
    block::{Block, BlockType, FullBlock},
    certificate_signature::{CertificateKeyPair, CertificateSignature},
    ledger::LedgerCommitInfo,
    payload::{
        ExecutionArtifacts, FullTransactionList, Payload, RandaoReveal, TransactionHashList,
    },
    quorum_certificate::{genesis_vote_info, QuorumCertificate},
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    transaction_validator::TransactionValidator,
    voting::ValidatorMapping,
};
use monad_crypto::{
    hasher::{Hash, Hashable, Hasher, HasherType},
    secp256k1::{KeyPair, PubKey, SecpSignature},
};
use monad_eth_types::{EthAddress, EMPTY_RLP_TX_LIST};
use monad_types::{NodeId, Round, SeqNum};
use zerocopy::AsBytes;

#[derive(Clone, Default, Debug, PartialEq, Eq)]
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
    fn hash(&self, _state: &mut impl Hasher) {}
}

impl SignatureCollection for MockSignatures {
    type SignatureType = SecpSignature;

    fn new(
        _sigs: Vec<(NodeId, Self::SignatureType)>,
        _validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        _msg: &[u8],
    ) -> Result<Self, SignatureCollectionError<Self::SignatureType>> {
        Ok(Self { pubkey: Vec::new() })
    }

    fn get_hash<H: Hasher>(&self) -> Hash {
        Default::default()
    }

    fn verify(
        &self,
        _validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        _msg: &[u8],
    ) -> Result<Vec<NodeId>, SignatureCollectionError<Self::SignatureType>> {
        Ok(self.pubkey.iter().map(|pubkey| NodeId(*pubkey)).collect())
    }

    fn get_participants(
        &self,
        _validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<Self>>,
        _msg: &[u8],
    ) -> HashSet<NodeId> {
        HashSet::from_iter(self.pubkey.iter().map(|pubkey| NodeId(*pubkey)))
    }

    fn num_signatures(&self) -> usize {
        self.pubkey.len()
    }

    fn serialize(&self) -> Vec<u8> {
        unreachable!()
    }

    fn deserialize(_data: &[u8]) -> Result<Self, SignatureCollectionError<Self::SignatureType>> {
        unreachable!()
    }
}

pub fn hash<T: SignatureCollection>(b: &Block<T>) -> Hash {
    let block_id = {
        let mut hasher = HasherType::new();
        hasher.update(b.author.0.bytes());
        hasher.update(b.round);
        hasher.update(b.payload.txns.as_bytes());
        hasher.update(b.payload.header.parent_hash);
        hasher.update(b.payload.header.state_root);
        hasher.update(b.payload.header.transactions_root);
        hasher.update(b.payload.header.receipts_root);
        hasher.update(b.payload.header.logs_bloom);
        hasher.update(b.payload.header.gas_used);
        hasher.update(b.payload.seq_num.as_bytes());
        hasher.update(b.qc.info.vote.id.0);
        hasher.update(b.payload.beneficiary.0.as_bytes());
        hasher.update(b.payload.randao_reveal.0.as_bytes());
        hasher.update(b.qc.signatures.get_hash::<HasherType>());

        hasher.hash()
    };

    // Hash of a block is actually a hash of its cached BlockId
    let mut hasher = HasherType::new();
    hasher.update(block_id);
    hasher.hash()
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
        // (i+u32::MAX) makes sure that the MessageKeyPair != CertificateKeyPair
        // so we don't accidentally mis-sign stuff without test noticing
        let keypair = get_certificate_key::<SCT>(i as u64 + u32::MAX as u64);
        res.push(keypair);
    }
    res
}

pub fn create_seed_for_certificate_keys<SCT: SignatureCollection>(num_keys: u32) -> Vec<u64> {
    (0..num_keys).map(|i| i as u64 + u32::MAX as u64).collect()
}

pub fn get_genesis_config<'k, H, SCT, TVT>(
    keys: impl Iterator<Item = &'k (NodeId, &'k SignatureCollectionKeyPairType<SCT>)>,
    validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    tvt: &TVT,
) -> (FullBlock<SCT>, SCT)
where
    H: Hasher,
    SCT: SignatureCollection,
    TVT: TransactionValidator,
{
    let genesis_txn = TransactionHashList::default();
    let genesis_prime_qc = QuorumCertificate::<SCT>::genesis_prime_qc::<H>();
    let genesis_block = Block::<SCT>::new::<H>(
        // FIXME-4 init from genesis config, don't use random key
        NodeId(KeyPair::from_bytes(&mut [0xBE_u8; 32]).unwrap().pubkey()),
        Round(0),
        &Payload {
            txns: genesis_txn,
            header: ExecutionArtifacts::zero(),
            seq_num: SeqNum(0),
            beneficiary: EthAddress::default(),
            randao_reveal: RandaoReveal::default(),
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
    (
        FullBlock::from_block(
            genesis_block,
            FullTransactionList::new(vec![EMPTY_RLP_TX_LIST]),
            tvt,
        )
        .unwrap(),
        sigs,
    )
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
    let mut hasher = HasherType::new();
    hasher.update(seed.to_le_bytes());
    let mut hash = hasher.hash();
    KeyPair::from_bytes(&mut hash.0).unwrap()
}

pub fn get_certificate_key<SCT: SignatureCollection>(
    seed: u64,
) -> SignatureCollectionKeyPairType<SCT> {
    let mut hasher = HasherType::new();
    hasher.update(seed.to_le_bytes());
    let mut hash = hasher.hash();
    <SignatureCollectionKeyPairType<SCT> as CertificateKeyPair>::from_bytes(&mut hash.0).unwrap()
}
