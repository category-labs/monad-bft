use alloy_rlp::{Decodable, Encodable};
use bytes::BytesMut;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable,
};
use monad_eth_types::EthAddress;
use monad_proto::{
    error::ProtoError,
    proto::{
        basic::{ProtoBlockBodyId, ProtoBloom, ProtoGas},
        block::*,
        blocksync::ProtoBlockRange,
        event::{
            proto_execution_result, ProtoExecutionResult, ProtoFinalizedExecutionResult,
            ProtoProposedExecutionResult,
        },
    },
};

use crate::{
    block::{
        BlockRange, ConsensusBlockHeader, ConsensusFullBlock, ExecutionProtocol, ExecutionResult,
        ProposedExecutionResult,
    },
    payload::{
        Bloom, ConsensusBlockBody, ConsensusBlockBodyId, ConsensusBlockBodyInner,
        FullTransactionList, Gas, RoundSignature,
    },
    signature_collection::SignatureCollection,
};

use super::signing::{certificate_signature_to_proto, proto_to_certificate_signature};

impl From<&BlockRange> for ProtoBlockRange {
    fn from(value: &BlockRange) -> Self {
        Self {
            last_block_id: Some((&value.last_block_id).into()),
            max_blocks: Some((&value.max_blocks).into()),
        }
    }
}

impl TryFrom<ProtoBlockRange> for BlockRange {
    type Error = ProtoError;

    fn try_from(value: ProtoBlockRange) -> Result<Self, Self::Error> {
        Ok(Self {
            last_block_id: value
                .last_block_id
                .ok_or(Self::Error::MissingRequiredField(
                    "BlockRange.last_block_id".to_owned(),
                ))?
                .try_into()?,
            max_blocks: value
                .max_blocks
                .ok_or(Self::Error::MissingRequiredField(
                    "BlockRange.max_blocks".to_owned(),
                ))?
                .try_into()?,
        })
    }
}

impl From<&ConsensusBlockBodyId> for ProtoBlockBodyId {
    fn from(value: &ConsensusBlockBodyId) -> Self {
        Self {
            pid: Some((&(value.0)).into()),
        }
    }
}

impl TryFrom<ProtoBlockBodyId> for ConsensusBlockBodyId {
    type Error = ProtoError;

    fn try_from(value: ProtoBlockBodyId) -> Result<Self, Self::Error> {
        Ok(Self(
            value
                .pid
                .ok_or(Self::Error::MissingRequiredField(
                    "ProtoBlockBodyId.pid".to_owned(),
                ))?
                .try_into()?,
        ))
    }
}

impl<ST, SCT, EPT> From<&ConsensusBlockHeader<ST, SCT, EPT>> for ProtoBlockHeader
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: &ConsensusBlockHeader<ST, SCT, EPT>) -> Self {
        Self {
            author: Some((&value.author).into()),
            epoch: Some((&value.epoch).into()),
            round: Some((&value.round).into()),
            delayed_execution_results: value
                .delayed_execution_results
                .iter()
                .map(|result| {
                    let mut buf = BytesMut::new();
                    result.encode(&mut buf);
                    buf.into()
                })
                .collect(),
            execution_inputs: {
                let mut buf = BytesMut::new();
                value.execution_inputs.encode(&mut buf);
                buf.into()
            },
            block_body_id: Some((&value.block_body_id).into()),
            qc: Some((&value.qc).into()),
            seq_num: Some((&value.seq_num).into()),
            timestamp: value.timestamp_ns as u64, // TODO: this is obv not correct but protobuf
            // is not used in protocol and definitions will be
            // removed
            round_signature: Some(certificate_signature_to_proto(&value.round_signature.0)),
        }
    }
}

impl<ST, SCT, EPT> TryFrom<ProtoBlockHeader> for ConsensusBlockHeader<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Error = ProtoError;

    fn try_from(value: ProtoBlockHeader) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value
                .author
                .ok_or(Self::Error::MissingRequiredField(
                    "BlockHeader.author".to_owned(),
                ))?
                .try_into()?,
            value
                .epoch
                .ok_or(Self::Error::MissingRequiredField(
                    "BlockHeader.epoch".to_owned(),
                ))?
                .try_into()?,
            value
                .round
                .ok_or(Self::Error::MissingRequiredField(
                    "BlockHeader.round".to_owned(),
                ))?
                .try_into()?,
            value
                .delayed_execution_results
                .into_iter()
                .map(|delayed_execution_result| {
                    EPT::FinalizedHeader::decode(&mut delayed_execution_result.as_ref()).map_err(
                        |_err| {
                            Self::Error::DeserializeError(
                                "BlockHeader.delayed_execution_results".to_owned(),
                            )
                        },
                    )
                })
                .collect::<Result<_, _>>()?,
            EPT::ProposedHeader::decode(&mut value.execution_inputs.as_ref()).map_err(|_err| {
                Self::Error::DeserializeError("BlockHeader.execution_inputs".to_owned())
            })?,
            value
                .block_body_id
                .ok_or(Self::Error::MissingRequiredField(
                    "BlockHeader.block_body_id".to_owned(),
                ))?
                .try_into()?,
            value
                .qc
                .ok_or(Self::Error::MissingRequiredField(
                    "BlockHeader.qc".to_owned(),
                ))?
                .try_into()?,
            value
                .seq_num
                .ok_or(Self::Error::MissingRequiredField(
                    "BlockHeader.seq_num".to_owned(),
                ))?
                .try_into()?,
            value.timestamp.into(),
            RoundSignature(proto_to_certificate_signature(
                value
                    .round_signature
                    .ok_or(Self::Error::MissingRequiredField(
                        "BlockHeader.round_signature".to_owned(),
                    ))?,
            )?),
        ))
    }
}

impl<ST, SCT, EPT> From<&ConsensusFullBlock<ST, SCT, EPT>> for ProtoFullBlock
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    fn from(value: &ConsensusFullBlock<ST, SCT, EPT>) -> Self {
        Self {
            header: Some(value.header().into()),
            body: Some(value.body().into()),
        }
    }
}

impl<ST, SCT, EPT> TryFrom<ProtoFullBlock> for ConsensusFullBlock<ST, SCT, EPT>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type Error = ProtoError;

    fn try_from(value: ProtoFullBlock) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value
                .header
                .ok_or(Self::Error::MissingRequiredField(
                    "FullBlock.header".to_owned(),
                ))?
                .try_into()?,
            value
                .body
                .ok_or(Self::Error::MissingRequiredField(
                    "FullBlock.body".to_owned(),
                ))?
                .try_into()?,
        )
        .map_err(|err| Self::Error::DeserializeError(format!("{:?}", err)))?)
    }
}

impl From<&Gas> for ProtoGas {
    fn from(value: &Gas) -> Self {
        Self { gas: value.0 }
    }
}

impl TryFrom<ProtoGas> for Gas {
    type Error = ProtoError;
    fn try_from(value: ProtoGas) -> Result<Self, Self::Error> {
        Ok(Self(value.gas))
    }
}

impl From<&Bloom> for ProtoBloom {
    fn from(value: &Bloom) -> Self {
        Self {
            bloom: value.0.to_vec().into(),
        }
    }
}

impl TryFrom<ProtoBloom> for Bloom {
    type Error = ProtoError;
    fn try_from(value: ProtoBloom) -> Result<Self, Self::Error> {
        Ok(Self(value.bloom.to_vec().try_into().map_err(
            |e: Vec<_>| Self::Error::WrongHashLen(format!("{}", e.len())),
        )?))
    }
}

impl<EPT> From<&ConsensusBlockBody<EPT>> for ProtoBlockBody
where
    EPT: ExecutionProtocol,
{
    fn from(value: &ConsensusBlockBody<EPT>) -> Self {
        ProtoBlockBody {
            execution_body: {
                let mut buf = BytesMut::new();
                value.execution_body.encode(&mut buf);
                buf.into()
            },
        }
    }
}

impl<EPT> TryFrom<ProtoBlockBody> for ConsensusBlockBody<EPT>
where
    EPT: ExecutionProtocol,
{
    type Error = ProtoError;
    fn try_from(value: ProtoBlockBody) -> Result<Self, Self::Error> {
        Ok(Self::new(ConsensusBlockBodyInner {
            execution_body: EPT::Body::decode(&mut value.execution_body.as_ref()).map_err(
                |_err| Self::Error::DeserializeError("BlockBody.execution_body".to_owned()),
            )?,
        }))
    }
}

impl<EPT> From<&ExecutionResult<EPT>> for ProtoExecutionResult
where
    EPT: ExecutionProtocol,
{
    fn from(value: &ExecutionResult<EPT>) -> Self {
        let event = match value {
            ExecutionResult::Proposed(proposed) => {
                proto_execution_result::Event::Proposed(proposed.into())
            }
            ExecutionResult::Finalized(seq_num, result) => {
                proto_execution_result::Event::Finalized(ProtoFinalizedExecutionResult {
                    seq_num: Some(seq_num.into()),
                    result: {
                        let mut buf = BytesMut::new();
                        result.encode(&mut buf);
                        buf.into()
                    },
                })
            }
        };
        Self { event: Some(event) }
    }
}

impl<EPT> TryFrom<ProtoExecutionResult> for ExecutionResult<EPT>
where
    EPT: ExecutionProtocol,
{
    type Error = ProtoError;

    fn try_from(value: ProtoExecutionResult) -> Result<Self, Self::Error> {
        let event = match value.event {
            Some(proto_execution_result::Event::Proposed(proposed)) => {
                Self::Proposed(proposed.try_into()?)
            }
            Some(proto_execution_result::Event::Finalized(finalized)) => Self::Finalized(
                finalized
                    .seq_num
                    .ok_or(ProtoError::MissingRequiredField(
                        "ExecutionResultEvent::Finalized.seq_num".to_owned(),
                    ))?
                    .try_into()?,
                EPT::FinalizedHeader::decode(&mut finalized.result.as_ref()).map_err(|_err| {
                    Self::Error::DeserializeError(
                        "ExecutionResultEvent::Finalized.result".to_owned(),
                    )
                })?,
            ),
            None => Err(ProtoError::MissingRequiredField(
                "ExecutionResultEvent.event".to_owned(),
            ))?,
        };

        Ok(event)
    }
}

impl<EPT> From<&ProposedExecutionResult<EPT>> for ProtoProposedExecutionResult
where
    EPT: ExecutionProtocol,
{
    fn from(event: &ProposedExecutionResult<EPT>) -> Self {
        Self {
            block_id: Some((&event.block_id).into()),
            seq_num: Some((&event.seq_num).into()),
            round: Some((&event.round).into()),
            result: {
                let mut buf = BytesMut::new();
                event.result.encode(&mut buf);
                buf.into()
            },
        }
    }
}

impl<EPT> TryFrom<ProtoProposedExecutionResult> for ProposedExecutionResult<EPT>
where
    EPT: ExecutionProtocol,
{
    type Error = ProtoError;

    fn try_from(event: ProtoProposedExecutionResult) -> Result<Self, Self::Error> {
        let block_id = event
            .block_id
            .ok_or(ProtoError::MissingRequiredField(
                "ExecutionResultEvent.block_id".to_owned(),
            ))?
            .try_into()?;
        let seq_num = event
            .seq_num
            .ok_or(ProtoError::MissingRequiredField(
                "ExecutionResultEvent.seq_num".to_owned(),
            ))?
            .try_into()?;
        let round = event
            .round
            .ok_or(ProtoError::MissingRequiredField(
                "ExecutionResultEvent.round".to_owned(),
            ))?
            .try_into()?;
        let result = EPT::FinalizedHeader::decode(&mut event.result.as_ref()).map_err(|_err| {
            Self::Error::DeserializeError("ExecutionResultEvent.result".to_owned())
        })?;
        Ok(Self {
            block_id,
            seq_num,
            round,
            result,
        })
    }
}
