use std::{convert::Infallible, sync::Arc};

use bytes::{Bytes, BytesMut};
use monad_crypto::{
    certificate_signature::{
        CertificateKeyPair as _, CertificateSignature, CertificateSignaturePubKey,
        CertificateSignatureRecoverable,
    },
    signing_domain,
};
use monad_types::NodeId;

use super::{AuthenticatedPayload, ReceiveError};
use crate::{
    util::{Collector, Recipient, TcpMessage},
    SIGNATURE_SIZE,
};

#[derive(Debug)]
pub enum TcpValidationError {
    MessageTooShort(usize),
    InvalidSignature,
}

impl TcpValidationError {
    pub fn log(&self, context: &str) {
        match self {
            TcpValidationError::MessageTooShort(len) => {
                tracing::debug!(context, len, "tcp message too short");
            }
            TcpValidationError::InvalidSignature => {
                tracing::debug!(context, "tcp invalid signature");
            }
        }
    }
}

// For tcp-based unicast, message not raptor-coded
pub struct TcpPointToPointState<ST>
where
    ST: CertificateSignatureRecoverable,
{
    keypair: Arc<ST::KeyPairType>,
    self_id: NodeId<CertificateSignaturePubKey<ST>>,
}

impl<ST> TcpPointToPointState<ST>
where
    ST: CertificateSignatureRecoverable,
{
    pub fn new(keypair: Arc<ST::KeyPairType>) -> Self {
        let self_id = NodeId::new(keypair.pubkey());
        Self { keypair, self_id }
    }

    pub fn publish(
        &mut self,
        message: bytes::Bytes,
        target: NodeId<CertificateSignaturePubKey<ST>>,
        completion: Option<futures::channel::oneshot::Sender<()>>, // notify on completion
        packet_collector: &mut impl Collector<TcpMessage<CertificateSignaturePubKey<ST>>>,
        data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), Infallible> {
        if target == self.self_id {
            // loopback message, deliver directly
            data_collector.push((self.self_id, message).into());
            if let Some(completion) = completion {
                let _ = completion.send(());
            }

            return Ok(());
        }

        let mut signed_message = BytesMut::zeroed(SIGNATURE_SIZE + message.len());

        let signature: ST =
            ST::sign::<signing_domain::RaptorcastAppMessage>(&message, &self.keypair);
        let signature = <ST as CertificateSignature>::serialize(&signature);

        assert_eq!(signature.len(), SIGNATURE_SIZE);
        signed_message[..SIGNATURE_SIZE].copy_from_slice(&signature);
        signed_message[SIGNATURE_SIZE..].copy_from_slice(&message);

        let signed_message = signed_message.freeze();
        packet_collector.push(TcpMessage {
            recipient: Recipient::new(target),
            payload: signed_message,
            completion,
        });

        Ok(())
    }

    // TODO: call this.dataplane_control.disconnect(src_addr); on error
    pub fn receive(
        &mut self,
        message: Bytes,
        data_collector: &mut impl Collector<AuthenticatedPayload<CertificateSignaturePubKey<ST>>>,
    ) -> Result<(), ReceiveError> {
        if message.len() < SIGNATURE_SIZE {
            let err = TcpValidationError::MessageTooShort(message.len());
            return Err(ReceiveError::TcpMessageValidation(err));
        }

        let signature_bytes = &message[..SIGNATURE_SIZE];
        let signature = <ST as CertificateSignature>::deserialize(signature_bytes)
            .map_err(|_| TcpValidationError::InvalidSignature)
            .map_err(ReceiveError::TcpMessageValidation)?;

        let payload = message.slice(SIGNATURE_SIZE..);

        let sender_pubkey = signature
            .recover_pubkey::<signing_domain::RaptorcastAppMessage>(payload.as_ref())
            .map_err(|_| TcpValidationError::InvalidSignature)
            .map_err(ReceiveError::TcpMessageValidation)?;
        let sender_nodeid = NodeId::new(sender_pubkey);

        data_collector.push((sender_nodeid, payload).into());

        Ok(())
    }
}
