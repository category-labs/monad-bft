use monad_consensus_types::{
    signature_collection::{SignatureCollection, SignatureCollectionKeyPairType},
    voting::ValidatorMapping,
};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair, CertificateSignaturePubKey, CertificateSignatureRecoverable,
    },
    NopKeyPair, NopPubKey, NopSignature,
};
use monad_types::{Epoch, NodeId, Round, SeqNum, Stake};
use monad_validator::{
    epoch_manager::EpochManager,
    validator_set::{ValidatorSetFactory, ValidatorSetType, ValidatorSetTypeFactory},
    validators_epoch_mapping::ValidatorsEpochMapping,
};

use crate::signing::{create_certificate_keys, create_keys};

type SignatureType = NopSignature;
type SignatureCollectionType = MockSignatures<SignatureType>;
use crate::signing::MockSignatures;

pub fn create_keys_w_validators<ST, SCT, VTF>(
    num_nodes: u32,
    validator_set_factory: VTF,
) -> (
    Vec<ST::KeyPairType>,
    Vec<SignatureCollectionKeyPairType<SCT>>,
    VTF::ValidatorSetType,
    ValidatorMapping<CertificateSignaturePubKey<ST>, SignatureCollectionKeyPairType<SCT>>,
)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    let keys = create_keys::<ST>(num_nodes);
    let certificate_keys = create_certificate_keys::<SCT>(num_nodes);
    let (validators, validator_mapping) =
        complete_keys_w_validators::<ST, SCT, VTF>(&keys, &certificate_keys, validator_set_factory);
    (keys, certificate_keys, validators, validator_mapping)
}

pub fn complete_keys_w_validators<ST, SCT, VTF>(
    keys: &[ST::KeyPairType],
    certificate_keys: &[SignatureCollectionKeyPairType<SCT>],
    validator_set_factory: VTF,
) -> (
    VTF::ValidatorSetType,
    ValidatorMapping<CertificateSignaturePubKey<ST>, SignatureCollectionKeyPairType<SCT>>,
)
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    VTF: ValidatorSetTypeFactory<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
{
    let staking_list = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(std::iter::repeat(Stake(1)))
        .collect::<Vec<_>>();

    let voting_identity = keys
        .iter()
        .map(|k| NodeId::new(k.pubkey()))
        .zip(certificate_keys.iter().map(|k| k.pubkey()))
        .collect::<Vec<_>>();

    let validators = validator_set_factory
        .create(staking_list)
        .expect("create validator set");
    let validator_mapping = ValidatorMapping::new(voting_identity);

    (validators, validator_mapping)
}

pub fn setup_val_state(
    known_epoch: Epoch,
    known_round: Round,
    val_epoch: Epoch,
    num_node: u32,
    val_set_update_interval: SeqNum,
    epoch_start_delay: Round,
) -> (
    Vec<NopKeyPair>,
    Vec<NopKeyPair>,
    EpochManager,
    ValidatorsEpochMapping<ValidatorSetFactory<NopPubKey>, SignatureCollectionType>,
) {
    let (keypairs, _certkeys, _, _) = create_keys_w_validators::<
        SignatureType,
        SignatureCollectionType,
        _,
    >(num_node, ValidatorSetFactory::default());

    let mut vlist = Vec::new();
    let mut vmap_vec = Vec::new();

    for keypair in &keypairs {
        let node_id = NodeId::new(keypair.pubkey());

        vlist.push((node_id, Stake(33)));
        vmap_vec.push((node_id, keypair.pubkey()));
    }

    let _vset = ValidatorSetFactory::default().create(vlist).unwrap();
    let _vmap = ValidatorMapping::new(vmap_vec);

    let epoch_manager = EpochManager::new(
        val_set_update_interval,
        epoch_start_delay,
        &[(known_epoch, known_round)],
    );
    let mut val_epoch_map: ValidatorsEpochMapping<ValidatorSetFactory<_>, SignatureCollectionType> =
        ValidatorsEpochMapping::new(ValidatorSetFactory::default());

    val_epoch_map.insert(
        val_epoch,
        _vset.get_members().iter().map(|(a, b)| (*a, *b)).collect(),
        _vmap,
    );

    (keypairs, _certkeys, epoch_manager, val_epoch_map)
}
