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

use std::collections::{BTreeMap, HashMap, HashSet};

use monad_consensus_types::{
    no_endorsement::NoEndorsementCertificate,
    signature_collection::{
        SignatureCollection, SignatureCollectionError, SignatureCollectionKeyPairType,
    },
    voting::ValidatorMapping,
};
use monad_crypto::signing_domain;
use monad_types::{NodeId, Round};
use monad_validator::validator_set::ValidatorSetType;
use tracing::{debug, error, info, warn};

use crate::messages::message::NoEndorsementMessage;

#[derive(Debug, PartialEq, Eq)]
pub struct NoEndorsementState<SCT: SignatureCollection> {
    pending_no_endorsements: BTreeMap<Round, RoundNoEndorsementState<SCT>>,
    /// The earliest round that we'll accept no-endorsements for
    /// We use this to not build the same NEC twice, and to know which no-endorsements are stale
    earliest_round: Round,
}

#[derive(Debug, PartialEq, Eq)]
struct RoundNoEndorsementState<SCT: SignatureCollection> {
    qc_round_no_endorsements:
        HashMap<Round, BTreeMap<NodeId<SCT::NodeIdPubKey>, SCT::SignatureType>>,
    node_no_endorsements: HashMap<NodeId<SCT::NodeIdPubKey>, HashSet<SCT::SignatureType>>,
    certificate: Option<NoEndorsementCertificate<SCT>>,
}

impl<SCT: SignatureCollection> Default for RoundNoEndorsementState<SCT> {
    fn default() -> Self {
        Self {
            qc_round_no_endorsements: HashMap::new(),
            node_no_endorsements: HashMap::new(),
            certificate: None,
        }
    }
}

#[derive(Debug)]
pub enum NoEndorsementStateCommand {
    // TODO: evidence collection command
}

impl<SCT> NoEndorsementState<SCT>
where
    SCT: SignatureCollection,
{
    pub fn new(round: Round) -> Self {
        Self {
            earliest_round: round,
            pending_no_endorsements: Default::default(),
        }
    }

    #[must_use]
    pub fn process_no_endorsement<VT>(
        &mut self,
        author: &NodeId<SCT::NodeIdPubKey>,
        no_endorsement_msg: &NoEndorsementMessage<SCT>,
        validators: &VT,
        validator_mapping: &ValidatorMapping<
            SCT::NodeIdPubKey,
            SignatureCollectionKeyPairType<SCT>,
        >,
    ) -> (
        Option<NoEndorsementCertificate<SCT>>,
        Vec<NoEndorsementStateCommand>,
    )
    where
        VT: ValidatorSetType<NodeIdPubKey = SCT::NodeIdPubKey>,
    {
        let no_endorsement = no_endorsement_msg.msg.clone();
        let round = no_endorsement.round;

        let mut ret_commands = Vec::new();

        if round < self.earliest_round {
            error!(
                ?round,
                earliest_round = ?self.earliest_round,
                "process_no_endorsement called on round < self.earliest_round",
            );
            return (None, ret_commands);
        }

        // pending no_endorsements for a given round + tip
        let round_state = self.pending_no_endorsements.entry(round).or_default();
        let node_votes = round_state.node_no_endorsements.entry(*author).or_default();
        node_votes.insert(no_endorsement_msg.signature);
        if node_votes.len() > 1 {
            // TODO: collect double vote as evidence
        }

        // pending no-endorsements for a given round + tip
        let round_pending_no_endorsements = round_state
            .qc_round_no_endorsements
            .entry(no_endorsement.tip_qc_round)
            .or_default();
        round_pending_no_endorsements.insert(*author, no_endorsement_msg.signature);

        debug!(
            ?no_endorsement,
            current_stake = ?validators.calculate_current_stake(&round_pending_no_endorsements.keys().copied().collect::<Vec<_>>()),
            total_stake = ?validators.get_total_stake(),
            "collecting no-endorsement"
        );

        while validators
            .has_super_majority_votes(
                &round_pending_no_endorsements
                    .keys()
                    .copied()
                    .collect::<Vec<_>>(),
            )
            .expect("has_super_majority_votes succeeds since addresses are unique")
        {
            assert!(round >= self.earliest_round);
            let no_endorsement_enc = alloy_rlp::encode(&no_endorsement);
            match SCT::new::<signing_domain::NoEndorsement>(
                round_pending_no_endorsements
                    .iter()
                    .map(|(node, signature)| (*node, *signature)),
                validator_mapping,
                no_endorsement_enc.as_ref(),
            ) {
                Ok(sigcol) => {
                    let nec = NoEndorsementCertificate {
                        msg: no_endorsement.clone(),
                        signatures: sigcol,
                    };
                    // we update self.earliest round so that we no longer will build an NEC for
                    // current round
                    self.earliest_round = round + Round(1);

                    info!(?no_endorsement, "Created new NEC");
                    assert!(round_state.certificate.is_none());
                    round_state.certificate = Some(nec.clone());
                    return (Some(nec), ret_commands);
                }
                Err(SignatureCollectionError::InvalidSignaturesCreate(invalid_sigs)) => {
                    // remove invalid signatures from round_pending_no_endorsements, and populate commands
                    let cmds = Self::handle_invalid_no_endorsement(
                        round_pending_no_endorsements,
                        invalid_sigs,
                    );

                    warn!(?no_endorsement, "Invalid signatures when creating new NEC");
                    ret_commands.extend(cmds);
                }
                Err(
                    SignatureCollectionError::NodeIdNotInMapping(_)
                    | SignatureCollectionError::ConflictingSignatures(_)
                    | SignatureCollectionError::InvalidSignaturesVerify
                    | SignatureCollectionError::DeserializeError(_),
                ) => {
                    unreachable!("InvalidSignaturesCreate is only expected error from creating SC");
                }
            }
        }

        (None, ret_commands)
    }

    #[must_use]
    fn handle_invalid_no_endorsement(
        pending_entry: &mut BTreeMap<NodeId<SCT::NodeIdPubKey>, SCT::SignatureType>,
        invalid_no_endorsements: Vec<(NodeId<SCT::NodeIdPubKey>, SCT::SignatureType)>,
    ) -> Vec<NoEndorsementStateCommand> {
        let invalid_no_endorsement_set = invalid_no_endorsements
            .into_iter()
            .map(|(a, _)| a)
            .collect::<HashSet<_>>();
        pending_entry.retain(|node_id, _| !invalid_no_endorsement_set.contains(node_id));
        // TODO: evidence
        vec![]
    }

    pub fn start_new_round(&mut self, new_round: Round) {
        self.earliest_round = new_round;
        self.pending_no_endorsements.retain(|k, _| *k >= new_round);
    }

    pub fn get_nec(&self, round: &Round) -> Option<&NoEndorsementCertificate<SCT>> {
        let state = self.pending_no_endorsements.get(round)?;
        state.certificate.as_ref()
    }
}
