// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{collections::BTreeMap, marker::PhantomData, ops::Deref, time::Duration};

use itertools::Itertools;
use monad_consensus::messages::consensus_message::ProtocolMessage;
use monad_crypto::certificate_signature::{
    CertificateSignaturePubKey, CertificateSignatureRecoverable, PubKey,
};
use monad_state::VerifiedMonadMessage;
use monad_transformer::{
    DropTransformer, LatencyTransformer, LinkMessage, PartitionTransformer, RandLatencyTransformer,
    Transformer, TransformerStream, XorLatencyTransformer, ID, UNIQUE_ID,
};
use monad_types::{ExecutionProtocol, NodeId, Round};
use monad_validator::signature_collection::SignatureCollection;

#[derive(Debug, Clone)]
pub struct FilterTransformer<PT: PubKey> {
    pub drop_proposal: bool,
    pub drop_vote: bool,
    pub drop_timeout: bool,
    pub drop_block_sync: bool,

    pub _phantom: PhantomData<PT>,
}

impl<PT: PubKey> Default for FilterTransformer<PT> {
    fn default() -> Self {
        Self {
            drop_proposal: false,
            drop_vote: false,
            drop_timeout: false,
            drop_block_sync: false,

            _phantom: PhantomData,
        }
    }
}

impl<ST, SCT, EPT> Transformer<VerifiedMonadMessage<ST, SCT, EPT>>
    for FilterTransformer<CertificateSignaturePubKey<ST>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    fn transform(
        &mut self,
        link_m: LinkMessage<CertificateSignaturePubKey<ST>, VerifiedMonadMessage<ST, SCT, EPT>>,
    ) -> TransformerStream<CertificateSignaturePubKey<ST>, VerifiedMonadMessage<ST, SCT, EPT>> {
        let should_drop = match &link_m.message {
            VerifiedMonadMessage::Consensus(consensus_msg) => {
                match consensus_msg.deref().deref().message {
                    ProtocolMessage::Proposal(_) => self.drop_proposal,
                    ProtocolMessage::Vote(_) => self.drop_vote,
                    ProtocolMessage::Timeout(_) => self.drop_timeout,
                    ProtocolMessage::RoundRecovery(_) => false,
                    ProtocolMessage::NoEndorsement(_) => false,
                    ProtocolMessage::AdvanceRound(_) => false,
                }
            }
            VerifiedMonadMessage::BlockSyncRequest(_)
            | VerifiedMonadMessage::BlockSyncResponse(_) => self.drop_block_sync,
            VerifiedMonadMessage::ForwardedTx(_) => false,
            VerifiedMonadMessage::StateSyncMessage(_) => false,
        };

        if should_drop {
            TransformerStream::Complete(vec![])
        } else {
            TransformerStream::Continue(vec![(Duration::ZERO, link_m)])
        }
    }
}
#[derive(Debug, Clone)]
pub struct TwinsTransformer<PT: PubKey> {
    // NodeId -> All Duplicate
    dups: BTreeMap<NodeId<PT>, Vec<usize>>,
    // Round -> Deliver target
    partition: BTreeMap<Round, Vec<ID<PT>>>,
    // when rounds cannot be found within partition
    default_part: Vec<ID<PT>>,
    // block sync and associated message is dropped by default
    ban_block_sync: bool,
}

impl<PT: PubKey> TwinsTransformer<PT> {
    pub fn new(
        dups: BTreeMap<NodeId<PT>, Vec<usize>>,
        partition: BTreeMap<Round, Vec<ID<PT>>>,
        default_part: Vec<ID<PT>>,
        ban_block_sync: bool,
    ) -> Self {
        Self {
            dups,
            partition,
            default_part,
            ban_block_sync,
        }
    }
}

enum TwinsCapture<PT: PubKey> {
    Spread(NodeId<PT>),         // spread to all target given NodeId
    Process(NodeId<PT>, Round), // spread to all target given NodeId and Round
    Drop,                       // Drop the message
}
impl<ST, SCT, EPT> Transformer<VerifiedMonadMessage<ST, SCT, EPT>>
    for TwinsTransformer<CertificateSignaturePubKey<ST>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;

    fn transform(
        &mut self,
        message: LinkMessage<Self::NodeIdPubKey, VerifiedMonadMessage<ST, SCT, EPT>>,
    ) -> TransformerStream<Self::NodeIdPubKey, VerifiedMonadMessage<ST, SCT, EPT>> {
        let LinkMessage {
            to,
            from,
            message,
            from_tick,
            nonce,
        } = message;
        let dup_identifier = *to.get_identifier();
        let pid = *to.get_peer_id();
        // only process messages that came in as default_id and spread to non-unique-ids
        assert_eq!(dup_identifier, UNIQUE_ID);

        let capture = match &message {
            VerifiedMonadMessage::Consensus(consensus_msg) => {
                match &consensus_msg.deref().deref().message {
                    ProtocolMessage::Proposal(p) => TwinsCapture::Process(pid, p.proposal_round),
                    ProtocolMessage::Vote(v) => TwinsCapture::Process(pid, v.vote.round),
                    // timeout naturally spread because liveness
                    ProtocolMessage::Timeout(_) => TwinsCapture::Spread(pid),
                    ProtocolMessage::RoundRecovery(m) => TwinsCapture::Process(pid, m.round),
                    ProtocolMessage::NoEndorsement(m) => TwinsCapture::Process(pid, m.msg.round),
                    ProtocolMessage::AdvanceRound(m) => {
                        TwinsCapture::Process(pid, m.last_round_certificate.round() + Round(1))
                    }
                }
            }
            VerifiedMonadMessage::BlockSyncRequest(_)
            | VerifiedMonadMessage::BlockSyncResponse(_) => {
                if self.ban_block_sync {
                    TwinsCapture::Drop
                } else {
                    TwinsCapture::Spread(pid)
                }
            }
            VerifiedMonadMessage::ForwardedTx(_) => {
                // is this correct?
                TwinsCapture::Drop
            }
            VerifiedMonadMessage::StateSyncMessage(_) => TwinsCapture::Spread(pid),
        };

        match capture {
            TwinsCapture::Drop => TransformerStream::Complete(vec![]),
            TwinsCapture::Spread(pid) => {
                let duplicate = self.dups.get(&pid).expect(
                    "NodeId to duplicate mapping provided to TwinTransformer is incomplete",
                );
                TransformerStream::Continue(
                    duplicate
                        .iter()
                        .map(|id| {
                            (
                                Duration::ZERO,
                                LinkMessage {
                                    from,
                                    to: ID::new(pid).as_non_unique(*id),
                                    message: message.clone(),
                                    from_tick,
                                    nonce,
                                },
                            )
                        })
                        .collect_vec(),
                )
            }
            TwinsCapture::Process(pid, round) => {
                // round unspecified is naturally treated as drop
                let round_dups = self.partition.get(&round).unwrap_or(&self.default_part);

                TransformerStream::Continue(
                    round_dups
                        .iter()
                        .filter_map(|id| {
                            if *id.get_peer_id() == pid {
                                let msg = LinkMessage {
                                    from,
                                    to: *id,
                                    message: message.clone(),
                                    from_tick,
                                    nonce,
                                };
                                Some((Duration::ZERO, msg))
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>(),
                )
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum MonadMessageTransformer<PT: PubKey> {
    Latency(LatencyTransformer<PT>),
    XorLatency(XorLatencyTransformer<PT>),
    RandLatency(RandLatencyTransformer<PT>),
    Twins(TwinsTransformer<PT>),
    Filter(FilterTransformer<PT>),
    Partition(PartitionTransformer<PT>),
    Drop(DropTransformer<PT>),
}

impl<ST, SCT, EPT> Transformer<VerifiedMonadMessage<ST, SCT, EPT>>
    for MonadMessageTransformer<CertificateSignaturePubKey<ST>>
where
    ST: CertificateSignatureRecoverable,
    SCT: SignatureCollection<NodeIdPubKey = CertificateSignaturePubKey<ST>>,
    EPT: ExecutionProtocol,
{
    type NodeIdPubKey = CertificateSignaturePubKey<ST>;
    fn transform(
        &mut self,
        message: LinkMessage<Self::NodeIdPubKey, VerifiedMonadMessage<ST, SCT, EPT>>,
    ) -> TransformerStream<Self::NodeIdPubKey, VerifiedMonadMessage<ST, SCT, EPT>> {
        match self {
            MonadMessageTransformer::Latency(t) => t.transform(message),
            MonadMessageTransformer::XorLatency(t) => t.transform(message),
            MonadMessageTransformer::RandLatency(t) => t.transform(message),
            MonadMessageTransformer::Twins(t) => t.transform(message),
            MonadMessageTransformer::Filter(t) => t.transform(message),
            MonadMessageTransformer::Partition(t) => t.transform(message),
            MonadMessageTransformer::Drop(t) => t.transform(message),
        }
    }

    fn min_external_delay(&self) -> Option<Duration> {
        match self {
            MonadMessageTransformer::Latency(t) => {
                <LatencyTransformer<Self::NodeIdPubKey> as Transformer<
                    VerifiedMonadMessage<ST, SCT, EPT>,
                >>::min_external_delay(t)
            }
            MonadMessageTransformer::XorLatency(t) => {
                <XorLatencyTransformer<Self::NodeIdPubKey> as Transformer<
                    VerifiedMonadMessage<ST, SCT, EPT>,
                >>::min_external_delay(t)
            }
            MonadMessageTransformer::RandLatency(t) => {
                <RandLatencyTransformer<Self::NodeIdPubKey> as Transformer<
                    VerifiedMonadMessage<ST, SCT, EPT>,
                >>::min_external_delay(t)
            }
            MonadMessageTransformer::Twins(t) => {
                <TwinsTransformer<Self::NodeIdPubKey> as Transformer<
                    VerifiedMonadMessage<ST, SCT, EPT>,
                >>::min_external_delay(t)
            }
            MonadMessageTransformer::Filter(t) => {
                <FilterTransformer<Self::NodeIdPubKey> as Transformer<
                    VerifiedMonadMessage<ST, SCT, EPT>,
                >>::min_external_delay(t)
            }
            MonadMessageTransformer::Drop(t) => {
                <DropTransformer<Self::NodeIdPubKey> as Transformer<
                    VerifiedMonadMessage<ST, SCT, EPT>,
                >>::min_external_delay(t)
            }
            MonadMessageTransformer::Partition(t) => {
                <PartitionTransformer<Self::NodeIdPubKey> as Transformer<
                    VerifiedMonadMessage<ST, SCT, EPT>,
                >>::min_external_delay(t)
            }
        }
    }
}

pub type MonadMessageTransformerPipeline<PT> = Vec<MonadMessageTransformer<PT>>;

#[cfg(test)]
mod test {
    use std::{collections::HashSet, time::Duration};

    use monad_crypto::{certificate_signature::CertificateKeyPair, NopSignature};
    use monad_testutil::signing::create_keys;
    use monad_transformer::{
        DropTransformer, GenericTransformer, LinkMessage, PartitionTransformer,
        PeriodicTransformer, Pipeline, RandLatencyTransformer, ReplayTransformer,
        TransformerReplayOrder, TransformerStream, XorLatencyTransformer,
    };
    use monad_types::NodeId;

    use super::{LatencyTransformer, Transformer, ID};
    use crate::utils::test_tool::*;

    #[test]
    fn test_latency_transformer() {
        let mut t = LatencyTransformer::new(Duration::from_secs(1));
        let m = get_mock_message();
        let TransformerStream::Continue(c) = t.transform(m.clone()) else {
            panic!("latency_transformer returned wrong type")
        };

        assert_eq!(c.len(), 1);
        assert!(c[0].0 == Duration::from_secs(1));
        assert!(c[0].1 == m);
    }

    #[test]
    fn test_xorlatency_transformer() {
        let mut t = XorLatencyTransformer::new(Duration::from_secs(1));
        let m = get_mock_message();
        let TransformerStream::Continue(c) = t.transform(m.clone()) else {
            panic!("xorlatency_transformer returned wrong type")
        };

        assert_eq!(c.len(), 1);
        // instead of verifying the random algorithm's correctness,
        // verifying the message is not touched
        // and feed through the right channel is more useful
        assert!(c[0].1 == m);
    }

    #[test]
    fn test_randlatency_transformer() {
        let mut t = RandLatencyTransformer::new(1, Duration::from_millis(30));
        let m = get_mock_message();
        let TransformerStream::Continue(c) = t.transform(m.clone()) else {
            panic!("randlatency_transformer returned wrong type")
        };

        assert_eq!(c.len(), 1);
        assert!(c[0].0 >= Duration::from_millis(1));
        assert!(c[0].0 <= Duration::from_millis(30));
        assert!(c[0].1 == m);
    }

    #[test]
    fn test_partition_transformer() {
        let keys = create_keys::<NopSignature>(2);
        let mut peers = HashSet::new();
        peers.insert(ID::new(NodeId::new(keys[0].pubkey())));
        let mut t = PartitionTransformer(peers.clone());
        let m = get_mock_message();
        let TransformerStream::Continue(c) = t.transform(m.clone()) else {
            panic!("partition_transformer returned wrong type")
        };

        assert_eq!(c.len(), 1);
        assert!(c[0].0 == Duration::ZERO);
        assert!(c[0].1 == m);

        let peers = HashSet::new();
        let mut t = PartitionTransformer(peers);
        let TransformerStream::Complete(c) = t.transform(m.clone()) else {
            panic!("partition_transformer returned wrong type")
        };

        assert_eq!(c.len(), 1);
        assert!(c[0].0 == Duration::ZERO);
        assert!(c[0].1 == m);
    }

    #[test]
    fn test_drop_transformer() {
        let mut t = DropTransformer::new();
        let m = get_mock_message();
        let TransformerStream::Complete(c) = t.transform(m) else {
            panic!("drop_transformer returned wrong type")
        };
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn test_periodic_transformer() {
        let mut t = PeriodicTransformer::new(Duration::from_millis(2), Duration::from_millis(7));
        for idx in 0..2 {
            let mut m = get_mock_message();
            m.from_tick = Duration::from_millis(idx);
            let TransformerStream::Complete(c) = t.transform(m.clone()) else {
                panic!("periodic_transformer returned wrong type")
            };
            assert_eq!(c.len(), 1);
            assert!(c[0].1 == m);
        }

        for idx in 2..7 {
            let mut m = get_mock_message();
            m.from_tick = Duration::from_millis(idx);
            let TransformerStream::Continue(c) = t.transform(m.clone()) else {
                panic!("periodic_transformer returned wrong type")
            };

            assert_eq!(c.len(), 1);
            assert!(c[0].1 == m);
        }
        for idx in 7..1000 {
            let mut m = get_mock_message();
            m.from_tick = Duration::from_millis(idx);
            let TransformerStream::Complete(c) = t.transform(m.clone()) else {
                panic!("periodic_transformer returned wrong type")
            };

            assert_eq!(c.len(), 1);
            assert!(c[0].1 == m);
        }
    }

    #[test]
    fn test_replay_transformer() {
        // we are mostly interested in the burst behaviur of replay
        let mut t =
            ReplayTransformer::new(Duration::from_millis(6), TransformerReplayOrder::Forward);
        for idx in 0..6 {
            let mut mock_message = get_mock_message();
            mock_message.from_tick = Duration::from_millis(idx);
            let TransformerStream::Continue(c) = t.transform(mock_message) else {
                panic!("replay_transformer returned wrong type")
            };
            assert_eq!(c.len(), 0);
        }

        let mut mock_message = get_mock_message();
        mock_message.from_tick = Duration::from_millis(6);
        let TransformerStream::Continue(c) = t.transform(mock_message) else {
            panic!("replay_transformer returned wrong type")
        };

        assert_eq!(c.len(), 7);
        for (idx, (_, m)) in c.iter().enumerate().take(7) {
            assert_eq!(m.from_tick, Duration::from_millis(idx as u64))
        }

        for idx in 7..1000 {
            let mut mock_message = get_mock_message();
            mock_message.from_tick = Duration::from_millis(idx);
            let TransformerStream::Continue(c) = t.transform(mock_message) else {
                panic!("replay_transformer returned wrong type")
            };
            assert_eq!(c.len(), 1);
        }
    }

    #[test]
    fn test_twins_transformer() {
        use std::collections::BTreeMap;

        use itertools::Itertools;
        use monad_types::Round;

        use crate::transformer::TwinsTransformer;

        let keys = create_keys::<NopSignature>(2);

        let pid = NodeId::new(keys[0].pubkey());
        let dups = (1..4)
            .map(|i| ID::new(pid).as_non_unique(i))
            .collect::<Vec<_>>();
        let default_id = ID::new(NodeId::new(keys[0].pubkey()));
        let mut pid_to_dups = BTreeMap::new();
        pid_to_dups.insert(pid, (1..4).collect_vec());
        let mut filter = BTreeMap::new();
        filter.insert(Round(1), dups.iter().take(2).copied().collect());
        filter.insert(Round(2), dups.iter().skip(1).take(2).copied().collect());

        let mut t = TwinsTransformer::new(pid_to_dups.clone(), filter.clone(), vec![], true);

        for i in 3..30 {
            // messages that is not part of the specified round result in default
            for msg in [
                fake_proposal_message(&keys[0], Round(i)),
                fake_vote_message(&keys[0], Round(i)),
            ] {
                let TransformerStream::Continue(c) = t.transform(LinkMessage {
                    from: default_id,
                    to: default_id,
                    message: msg,
                    from_tick: Duration::ZERO,
                    nonce: rand::random(),
                }) else {
                    panic!("twins_transformer returned wrong type")
                };
                assert_eq!(c.len(), 0);
            }
        }

        // timeout message gets spread regardless of rounds
        let TransformerStream::Continue(c) = t.transform(LinkMessage {
            from: default_id,
            to: default_id,
            message: fake_timeout_message(&keys[0]),
            from_tick: Duration::ZERO,
            nonce: rand::random(),
        }) else {
            panic!("twins_transformer returned wrong type")
        };
        assert_eq!(c.len(), 3);
        for (
            i,
            (
                t,
                LinkMessage {
                    from,
                    to,
                    message,
                    from_tick,
                    nonce: _,
                },
            ),
        ) in c.into_iter().enumerate()
        {
            assert_eq!(t, Duration::ZERO);
            assert_eq!(from, default_id);
            assert_eq!(to, dups[i]);
            assert_eq!(message, fake_timeout_message(&keys[0]));
            assert_eq!(from_tick, Duration::ZERO)
        }

        // on round 1, message sent to correct pid split into 2 new messages
        for msg in [
            fake_proposal_message(&keys[0], Round(1)),
            fake_vote_message(&keys[0], Round(1)),
        ] {
            let TransformerStream::Continue(c) = t.transform(LinkMessage {
                from: default_id,
                to: default_id,
                message: msg.clone(),
                from_tick: Duration::ZERO,
                nonce: rand::random(),
            }) else {
                panic!("twins_transformer returned wrong type")
            };
            assert_eq!(c.len(), 2);

            for (
                i,
                (
                    t,
                    LinkMessage {
                        from,
                        to,
                        message,
                        from_tick,
                        nonce: _,
                    },
                ),
            ) in c.into_iter().enumerate()
            {
                assert_eq!(t, Duration::ZERO);
                assert_eq!(from, default_id);
                assert_eq!(to, dups[i]);
                assert_eq!(message, msg);
                assert_eq!(from_tick, Duration::ZERO)
            }
        }

        // on round 2, message sent to correct pid split into 2 new messages (later 2 dups)
        for msg in [
            fake_proposal_message(&keys[0], Round(2)),
            fake_vote_message(&keys[0], Round(2)),
        ] {
            let TransformerStream::Continue(c) = t.transform(LinkMessage {
                from: default_id,
                to: default_id,
                message: msg.clone(),
                from_tick: Duration::ZERO,
                nonce: rand::random(),
            }) else {
                panic!("twins_transformer returned wrong type")
            };
            assert_eq!(c.len(), 2);

            for (
                i,
                (
                    t,
                    LinkMessage {
                        from,
                        to,
                        message,
                        from_tick,
                        nonce: _,
                    },
                ),
            ) in c.into_iter().enumerate()
            {
                assert_eq!(t, Duration::ZERO);
                assert_eq!(from, default_id);
                assert_eq!(to, dups[i + 1]);
                assert_eq!(message, msg);
                assert_eq!(from_tick, Duration::ZERO)
            }
        }

        // Sending Message to any round Key that is not logged will have no impact
        let wrong_id: ID<_> = ID::new(NodeId::new(keys[1].pubkey()));

        for r in 0..30 {
            for msg in [
                fake_proposal_message(&keys[1], Round(r)),
                fake_vote_message(&keys[1], Round(r)),
            ] {
                let stream = t.transform(LinkMessage {
                    from: wrong_id,
                    to: wrong_id,
                    message: msg.clone(),
                    from_tick: Duration::ZERO,
                    nonce: rand::random(),
                });
                // no matter stage is drop triggered, it should be empty always
                match stream {
                    TransformerStream::Complete(c) => assert_eq!(c.len(), 0),
                    TransformerStream::Continue(c) => assert_eq!(c.len(), 0),
                };
            }
        }

        // throwing it block sync message should get rejected
        for msg in [
            fake_request_block_sync(),
            fake_block_sync(),
            fake_request_block_sync(),
            fake_block_sync(),
        ] {
            let TransformerStream::Complete(c) = t.transform(LinkMessage {
                from: default_id,
                to: default_id,
                message: msg,
                from_tick: Duration::ZERO,
                nonce: rand::random(),
            }) else {
                panic!("twins_transformer returned wrong type")
            };
            assert_eq!(c.len(), 0);
        }

        // however if we enable block_sync then it should be broadcasted
        t = TwinsTransformer::new(pid_to_dups, filter, vec![], false);
        for msg in [
            fake_request_block_sync(),
            fake_block_sync(),
            fake_request_block_sync(),
            fake_block_sync(),
        ] {
            let TransformerStream::Continue(c) = t.transform(LinkMessage {
                from: default_id,
                to: default_id,
                message: msg,
                from_tick: Duration::ZERO,
                nonce: rand::random(),
            }) else {
                panic!("twins_transformer returned wrong type")
            };
            assert_eq!(c.len(), 3);
        }
    }

    #[test]
    fn test_pipeline_basic_flow() {
        let mut pipe = vec![LatencyTransformer::new(Duration::from_millis(30))];

        let mock_message = get_mock_message();
        // try to feed some message through, only some basic latency should be added to everything
        let result = pipe.process(mock_message.clone());
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, Duration::from_millis(30));
        assert!(result[0].1 == mock_message);
    }

    #[test]
    fn test_pipeline_complex_flow() {
        let keys = create_keys::<NopSignature>(5);
        let mut peers = HashSet::new();
        peers.insert(ID::new(NodeId::new(keys[0].pubkey())));

        let mut pipe = vec![
            GenericTransformer::Latency(LatencyTransformer::new(Duration::from_millis(30))),
            GenericTransformer::Partition(PartitionTransformer(peers)),
            GenericTransformer::Periodic(PeriodicTransformer::new(
                Duration::from_millis(2),
                Duration::from_millis(7),
            )),
            GenericTransformer::Latency(LatencyTransformer::new(Duration::from_millis(30))),
        ];
        for idx in 0..1000 {
            let mut mock_message = get_mock_message();
            mock_message.from = ID::new(NodeId::new(keys[3].pubkey()));
            mock_message.from_tick = Duration::from_millis(idx);
            let result = pipe.process(mock_message.clone());
            assert_eq!(result.len(), 1);
            // since its not part of the id, it doesn't get selected and filter
            assert_eq!(result[0].0, Duration::from_millis(30));
            assert!(result[0].1 == mock_message);
        }

        // first 2 message should not trigger extra mili
        for idx in 0..2 {
            let mut mock_message = get_mock_message();
            mock_message.from_tick = Duration::from_millis(idx);
            let result = pipe.process(mock_message.clone());
            assert_eq!(result.len(), 1);
            // since its part of the id, it get selected and filter, thus 30 extra mili
            assert_eq!(result[0].0, Duration::from_millis(30));
            assert!(result[0].1 == mock_message);
        }
        // follow by 5 message that get extra delay
        for idx in 2..7 {
            let mut mock_message = get_mock_message();
            mock_message.from_tick = Duration::from_millis(idx);
            let result = pipe.process(mock_message.clone());
            assert_eq!(result.len(), 1);
            // since its part of the id, it get selected and filter, thus 30 extra mili
            assert_eq!(result[0].0, Duration::from_millis(60));
            assert!(result[0].1 == mock_message);
        }
        for idx in 7..1000 {
            let mut mock_message = get_mock_message();
            mock_message.from_tick = Duration::from_millis(idx);
            let result = pipe.process(mock_message.clone());
            assert_eq!(result.len(), 1);
            // since its part of the id, it get selected and filter, thus 30 extra mili
            assert_eq!(result[0].0, Duration::from_millis(30));
            assert!(result[0].1 == mock_message);
        }
    }
}
