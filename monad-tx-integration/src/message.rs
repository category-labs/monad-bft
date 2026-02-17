use monad_eth_types::EthExecutionProtocol;
use monad_executor_glue::MonadEvent;
use monad_secp::SecpSignature;
use monad_testutil::signing::MockSignatures;

pub type SignatureType = SecpSignature;
pub type SignatureCollectionType = MockSignatures<SignatureType>;
pub type ExecutionProtocolType = EthExecutionProtocol;

// In production, the network sends `VerifiedMonadMessage` (outbound) which is
// decoded as `MonadMessage` (inbound). Their RLP encoding is intentionally
// compatible.
pub type InboundMessage =
    monad_state::MonadMessage<SignatureType, SignatureCollectionType, ExecutionProtocolType>;
pub type OutboundMessage = monad_state::VerifiedMonadMessage<
    SignatureType,
    SignatureCollectionType,
    ExecutionProtocolType,
>;

pub type WireEvent = MonadEvent<SignatureType, SignatureCollectionType, ExecutionProtocolType>;
