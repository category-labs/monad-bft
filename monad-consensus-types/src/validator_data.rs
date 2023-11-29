use monad_proto::{
    error::ProtoError,
    proto::validator_set::{ProtoValidatorSetData, ValidatorMapEntry},
};
use monad_types::{NodeId, Stake};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatorData(pub Vec<(NodeId, Stake)>);

impl From<&ValidatorData> for ProtoValidatorSetData {
    fn from(value: &ValidatorData) -> Self {
        let vlist = value
            .0
            .iter()
            .map(|(node, stake)| ValidatorMapEntry {
                key: Some(node.into()),
                value: Some(stake.into()),
            })
            .collect::<Vec<ValidatorMapEntry>>();
        Self { validators: vlist }
    }
}

impl TryFrom<ProtoValidatorSetData> for ValidatorData {
    type Error = ProtoError;
    fn try_from(value: ProtoValidatorSetData) -> std::result::Result<Self, Self::Error> {
        let mut vlist = ValidatorData(Vec::new());
        for v in value.validators {
            let a = v
                .key
                .ok_or(Self::Error::MissingRequiredField(
                    "ValildatorMapEntry.key".to_owned(),
                ))?
                .try_into()?;
            let b = v
                .value
                .ok_or(Self::Error::MissingRequiredField(
                    "ValildatorMapEntry.value".to_owned(),
                ))?
                .try_into()?;

            vlist.0.push((a, b));
        }

        Ok(vlist)
    }
}
