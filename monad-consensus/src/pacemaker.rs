use std::{collections::HashMap, time::Duration};

use monad_consensus_types::{
    quorum_certificate::QuorumCertificate,
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    timeout::{Timeout, TimeoutCertificate},
    voting::ValidatorMapping,
};
use monad_crypto::hasher::Hasher;
use monad_types::{NodeId, Round};
use monad_validator::validator_set::ValidatorSetType;

use crate::{
    messages::message::TimeoutMessage,
    validation::{message::well_formed, safety::Safety},
};

#[derive(PartialEq, Eq, Debug)]
pub struct Pacemaker<SCT: SignatureCollection> {
    delta: Duration,

    current_round: Round,
    last_round_tc: Option<TimeoutCertificate<SCT>>,

    // only need to store for current round
    pending_timeouts: HashMap<NodeId, TimeoutMessage<SCT>>,

    // used to not duplicate broadcast/tc
    phase: PhaseHonest,
}

#[derive(Debug, PartialEq, Eq)]
enum PhaseHonest {
    Zero,
    One,
    Supermajority,
}

#[derive(Debug)]
pub enum PacemakerCommand<SCT: SignatureCollection> {
    PrepareTimeout(Timeout<SCT>),
    Broadcast(TimeoutMessage<SCT>),
    Schedule { duration: Duration },
    ScheduleReset,
}

impl<SCT: SignatureCollection> Pacemaker<SCT> {
    pub fn new(
        delta: Duration,
        current_round: Round,
        last_round_tc: Option<TimeoutCertificate<SCT>>,
    ) -> Self {
        Self {
            delta,
            current_round,
            last_round_tc,
            pending_timeouts: HashMap::new(),

            phase: PhaseHonest::Zero,
        }
    }

    pub fn get_current_round(&self) -> Round {
        self.current_round
    }

    fn get_round_timer(&self) -> Duration {
        self.delta * 4
    }

    #[must_use]
    fn start_timer(&mut self, new_round: Round) -> PacemakerCommand<SCT> {
        assert!(new_round > self.current_round);

        self.phase = PhaseHonest::Zero;
        self.pending_timeouts.clear();

        self.current_round = new_round;

        PacemakerCommand::Schedule {
            duration: self.get_round_timer(),
        }
    }

    #[must_use]
    fn local_timeout_round(
        &self,
        safety: &mut Safety,
        high_qc: &QuorumCertificate<SCT>,
    ) -> Vec<PacemakerCommand<SCT>> {
        let mut cmds = vec![PacemakerCommand::ScheduleReset];
        let maybe_broadcast = safety
            .make_timeout(self.current_round, high_qc.clone(), &self.last_round_tc)
            .map(|timeout_info| {
                well_formed(
                    timeout_info.round,
                    timeout_info.high_qc.info.vote.round,
                    &self.last_round_tc,
                )
                .expect("invalid timeout");

                PacemakerCommand::PrepareTimeout(Timeout {
                    tminfo: timeout_info,
                    last_round_tc: self.last_round_tc.clone(),
                })
            });
        cmds.extend(maybe_broadcast);
        cmds.push(PacemakerCommand::Schedule {
            duration: self.get_round_timer(),
        });
        cmds
    }

    #[must_use]
    pub fn handle_event(
        &mut self,
        safety: &mut Safety,
        high_qc: &QuorumCertificate<SCT>,
    ) -> Vec<PacemakerCommand<SCT>> {
        self.phase = PhaseHonest::One;
        self.local_timeout_round(safety, high_qc)
    }

    #[must_use]
    pub fn process_remote_timeout<H: Hasher, VST: ValidatorSetType>(
        &mut self,
        validators: &VST,
        validator_mapping: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
        safety: &mut Safety,
        high_qc: &QuorumCertificate<SCT>,
        author: NodeId,
        tmo: TimeoutMessage<SCT>,
    ) -> (Option<TimeoutCertificate<SCT>>, Vec<PacemakerCommand<SCT>>) {
        let mut ret_commands = Vec::new();

        let tm_info = &tmo.timeout.tminfo;
        if tm_info.round < self.current_round {
            return (None, ret_commands);
        }
        assert_eq!(tm_info.round, self.current_round);

        // it's fine to overwrite if already exists
        self.pending_timeouts.insert(author, tmo.clone());

        let mut timeouts: Vec<NodeId> = self.pending_timeouts.keys().copied().collect();

        if self.phase == PhaseHonest::Zero && validators.has_honest_vote(&timeouts) {
            // self.local_timeout_round emits PacemakerCommand::ScheduleReset
            ret_commands.extend(self.local_timeout_round(safety, high_qc));
            self.phase = PhaseHonest::One;
        }

        let mut ret_tc: Option<TimeoutCertificate<SCT>> = None;
        while self.phase == PhaseHonest::One && validators.has_super_majority_votes(&timeouts) {
            match TimeoutCertificate::new::<H>(
                tm_info.round,
                self.pending_timeouts
                    .iter()
                    .map(|(node_id, tmo_msg)| {
                        (*node_id, tmo_msg.timeout.tminfo.clone(), tmo_msg.sig)
                    })
                    .collect::<Vec<_>>()
                    .as_slice(),
                validator_mapping,
            ) {
                Ok(tc) => {
                    self.phase = PhaseHonest::Supermajority;
                    ret_tc = Some(tc);
                }
                Err(err) => match err {
                    SignatureCollectionError::InvalidSignaturesCreate(invalid_sigs) => {
                        ret_commands.extend(self.handle_invalid_timeout(invalid_sigs));
                        timeouts = self.pending_timeouts.keys().copied().collect();
                    }
                    _ => {
                        unreachable!("unexpected error {}", err)
                    }
                },
            };
        }

        (ret_tc, ret_commands)
    }

    #[must_use]
    fn handle_invalid_timeout(
        &mut self,
        invalid_timeouts: Vec<(NodeId, SCT::SignatureType)>,
    ) -> Vec<PacemakerCommand<SCT>> {
        for (node_id, sig) in invalid_timeouts {
            let removed = self.pending_timeouts.remove(&node_id);
            // TODO: debug assert
            assert_eq!(removed.expect("Timeout removed").sig, sig);
        }
        // TODO: evidence collection
        vec![]
    }

    #[must_use]
    pub fn advance_round_tc(
        &mut self,
        tc: &TimeoutCertificate<SCT>,
    ) -> Option<PacemakerCommand<SCT>> {
        if tc.round < self.current_round {
            return None;
        }
        let round = tc.round;
        self.last_round_tc = Some(tc.clone());
        Some(self.start_timer(round + Round(1)))
    }

    #[must_use]
    pub fn advance_round_qc(
        &mut self,
        qc: &QuorumCertificate<SCT>,
    ) -> Option<PacemakerCommand<SCT>> {
        if qc.info.vote.round < self.current_round {
            return None;
        }
        self.last_round_tc = None;
        Some(self.start_timer(qc.info.vote.round + Round(1)))
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use monad_consensus_types::{
        certificate_signature::CertificateSignature,
        ledger::LedgerCommitInfo,
        multi_sig::MultiSig,
        quorum_certificate::QcInfo,
        timeout::TimeoutInfo,
        voting::{Vote, VoteInfo},
    };
    use monad_crypto::{
        hasher::{Hash, HasherType},
        secp256k1::{KeyPair, SecpSignature},
    };
    use monad_testutil::{
        signing::{create_certificate_keys, create_keys},
        validators::create_keys_w_validators,
    };
    use monad_types::{BlockId, SeqNum, Stake};
    use monad_validator::validator_set::ValidatorSet;
    use zerocopy::AsBytes;

    use super::*;

    type SignatureCollectionType = MultiSig<SecpSignature>;

    fn get_high_qc<SCT: SignatureCollection>(
        qc_round: Round,
        keys: &[KeyPair],
        certkeys: &[SignatureCollectionKeyPairType<SCT>],
        valmap: &ValidatorMapping<SignatureCollectionKeyPairType<SCT>>,
    ) -> QuorumCertificate<SCT> {
        let vote_info = VoteInfo {
            id: BlockId(Hash([0x00_u8; 32])),
            round: qc_round,
            parent_id: BlockId(Hash([0x00_u8; 32])),
            parent_round: Round(0),
            seq_num: SeqNum(0),
        };

        let ledger_commit_info = LedgerCommitInfo::new::<HasherType>(None, &vote_info);

        let qc_info = QcInfo {
            vote: vote_info,
            ledger_commit: ledger_commit_info,
        };

        let vote = Vote {
            vote_info,
            ledger_commit_info,
        };

        let vote_hash = HasherType::hash_object(&vote);

        let mut sigs = Vec::new();

        for (key, certkey) in keys.iter().zip(certkeys.iter()) {
            let node_id = NodeId(key.pubkey());
            let sig =
                <SCT::SignatureType as CertificateSignature>::sign(vote_hash.as_ref(), certkey);
            sigs.push((node_id, sig));
        }

        let sigcol = SCT::new(sigs, valmap, vote_hash.as_ref()).expect("success");

        QuorumCertificate::<SCT>::new::<HasherType>(qc_info, sigcol)
    }

    fn create_timeout_message<SCT: SignatureCollection>(
        certkeypair: &SignatureCollectionKeyPairType<SCT>,
        timeout_round: Round,
        high_qc: QuorumCertificate<SCT>,
        valid: bool,
    ) -> TimeoutMessage<SCT> {
        let timeout = Timeout {
            tminfo: TimeoutInfo {
                round: timeout_round,
                high_qc,
            },
            last_round_tc: None,
        };

        let invalid_msg = b"invalid";

        let mut tmo_msg = TimeoutMessage::<SCT>::new::<HasherType>(timeout, certkeypair);
        if !valid {
            tmo_msg.sig =
                <SCT::SignatureType as CertificateSignature>::sign(invalid_msg, certkeypair);
        }
        tmo_msg
    }

    #[test]
    fn all_honest() {
        let mut pacemaker =
            Pacemaker::<SignatureCollectionType>::new(Duration::from_secs(1), Round(1), None);
        let mut safety = Safety::default();

        let (keys, certkeys, valset, vmap) = create_keys_w_validators::<SignatureCollectionType>(4);
        let timeout_round = Round(1);
        let high_qc = get_high_qc(Round(0), keys.as_slice(), certkeys.as_slice(), &vmap);

        let tm0 = create_timeout_message(&certkeys[0], timeout_round, high_qc.clone(), true);
        let tm1 = create_timeout_message(&certkeys[1], timeout_round, high_qc.clone(), true);
        let tm2 = create_timeout_message(&certkeys[2], timeout_round, high_qc.clone(), true);
        let tm3 = create_timeout_message(&certkeys[3], timeout_round, high_qc.clone(), true);

        let (tc, cmds) = pacemaker.process_remote_timeout::<HasherType, _>(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId(keys[0].pubkey()),
            tm0,
        );
        assert!(tc.is_none());
        assert!(cmds.is_empty());

        // enter PhaseHonest::One, timeout itself
        let (tc, cmds) = pacemaker.process_remote_timeout::<HasherType, _>(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId(keys[1].pubkey()),
            tm1,
        );
        assert_eq!(pacemaker.phase, PhaseHonest::One);
        assert!(tc.is_none());
        assert_eq!(cmds.len(), 3);
        assert!(matches!(cmds[0], PacemakerCommand::ScheduleReset));
        assert!(matches!(cmds[1], PacemakerCommand::PrepareTimeout(_)));
        assert!(matches!(cmds[2], PacemakerCommand::Schedule { .. }));

        // enter PhaseHonest::SuperMajority, qc is created
        let (tc, cmds) = pacemaker.process_remote_timeout::<HasherType, _>(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId(keys[2].pubkey()),
            tm2,
        );
        assert_eq!(pacemaker.phase, PhaseHonest::Supermajority);
        assert!(tc.is_some());
        assert!(cmds.is_empty());

        // in PhaseHonest::SuperMajority, pacemaker doesn't create TC with new timeouts
        let (tc, cmds) = pacemaker.process_remote_timeout::<HasherType, _>(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId(keys[3].pubkey()),
            tm3,
        );
        assert!(tc.is_none());
        assert!(cmds.is_empty());
    }

    #[test]
    fn phase_supermajority_invalid_no_progress() {
        let mut pacemaker =
            Pacemaker::<SignatureCollectionType>::new(Duration::from_secs(1), Round(1), None);
        let mut safety = Safety::default();

        let (keys, certkeys, valset, vmap) = create_keys_w_validators::<SignatureCollectionType>(4);
        let timeout_round = Round(1);
        let high_qc = get_high_qc(Round(0), keys.as_slice(), certkeys.as_slice(), &vmap);

        let tm0_valid = create_timeout_message(&certkeys[0], timeout_round, high_qc.clone(), true);
        let tm1_valid = create_timeout_message(&certkeys[1], timeout_round, high_qc.clone(), true);
        let tm2_invalid =
            create_timeout_message(&certkeys[2], timeout_round, high_qc.clone(), false);

        let _ = pacemaker.process_remote_timeout::<HasherType, _>(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId(keys[0].pubkey()),
            tm0_valid,
        );

        let _ = pacemaker.process_remote_timeout::<HasherType, _>(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId(keys[1].pubkey()),
            tm1_valid,
        );

        let (tc, cmds) = pacemaker.process_remote_timeout::<HasherType, _>(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId(keys[2].pubkey()),
            tm2_invalid,
        );
        assert!(tc.is_none());
        assert!(cmds.is_empty());
        assert_eq!(pacemaker.phase, PhaseHonest::One);
        assert_eq!(pacemaker.pending_timeouts.len(), 2);
    }

    #[test]
    fn phase_supermajority_invalid_progress() {
        let mut pacemaker =
            Pacemaker::<SignatureCollectionType>::new(Duration::from_secs(1), Round(1), None);
        let mut safety = Safety::default();

        let keys = create_keys(4);
        let certkeys = create_certificate_keys::<SignatureCollectionType>(4);

        let mut staking_list = keys
            .iter()
            .map(|k| NodeId(k.pubkey()))
            .zip(std::iter::repeat(Stake(1)))
            .collect::<Vec<_>>();

        // total stake = 7, f = 2
        // node1 holds f+1 stake
        // node1 + node2 has 2f+1 stake
        staking_list[0].1 = Stake(1);
        staking_list[1].1 = Stake(3);
        staking_list[2].1 = Stake(2);
        staking_list[3].1 = Stake(1);

        let voting_identity = keys
            .iter()
            .map(|k| NodeId(k.pubkey()))
            .zip(certkeys.iter().map(|k| k.pubkey()))
            .collect::<Vec<_>>();

        let valset = ValidatorSet::new(staking_list).expect("create validator set");
        let vmap = ValidatorMapping::new(voting_identity);

        let timeout_round = Round(1);
        let high_qc = get_high_qc(Round(0), keys.as_slice(), certkeys.as_slice(), &vmap);

        let tm0_invalid =
            create_timeout_message(&certkeys[0], timeout_round, high_qc.clone(), false);
        let tm1_valid = create_timeout_message(&certkeys[1], timeout_round, high_qc.clone(), true);
        let tm2_valid = create_timeout_message(&certkeys[2], timeout_round, high_qc.clone(), true);

        let _ = pacemaker.process_remote_timeout::<HasherType, _>(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId(keys[1].pubkey()),
            tm1_valid,
        );

        let (tc, _cmds) = pacemaker.process_remote_timeout::<HasherType, _>(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId(keys[0].pubkey()),
            tm0_invalid,
        );
        assert!(tc.is_none());
        assert_eq!(pacemaker.pending_timeouts.len(), 2);

        // invalid timeout is removed
        // the remaining two timeouts has 5/7 stake, so TC is created
        let (tc, cmds) = pacemaker.process_remote_timeout::<HasherType, _>(
            &valset,
            &vmap,
            &mut safety,
            &high_qc,
            NodeId(keys[2].pubkey()),
            tm2_valid,
        );
        assert_eq!(pacemaker.phase, PhaseHonest::Supermajority);
        assert!(tc.is_some());

        // assert the TC is created over the two valid timeouts
        let mut hasher = HasherType::new();
        hasher.update(Round(1).as_bytes());
        hasher.update(Round(0).as_bytes());
        let timeout_hash = hasher.hash();
        let tc = tc.unwrap();
        assert_eq!(tc.high_qc_rounds.len(), 1);
        let sc = tc.high_qc_rounds.first().unwrap().sigs.clone();
        assert_eq!(
            sc.verify(&vmap, timeout_hash.as_ref())
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>(),
            vec![NodeId(keys[1].pubkey()), NodeId(keys[2].pubkey())]
                .into_iter()
                .collect::<HashSet<_>>()
        );

        assert!(cmds.is_empty());
        assert_eq!(pacemaker.pending_timeouts.len(), 2);
    }
}
