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

//! Commands from Quantum Chorus to the data-availability layer
//! (`monad-pq-da`). Events stay [`ChorusDAEvent`].

pub use monad_mcp_chorus::stub::slot::chorus::{ChorusDAEvent, MissingHeaders, ProposalDAEvent};
use monad_mcp_chorus::stub::{
    slot::chorus::ChorusDACommand,
    types::{NodeId, ProposalId, ProposalIndex, SignedProposalHeader},
};

/// A superset of [`ChorusDACommand`]: the upstream variants with unchanged
/// semantics, plus [`PqDACommand::PullRoot`].
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum PqDACommand {
    // release the slot's chunks to peers: rebroadcast our owned
    // chunks, held and arriving, and serve chunk recovery requests.
    ReleaseChunks,

    // nodes that decoded the proposal under j, so they can serve our own chunks
    AddDecodedHolders {
        j: ProposalIndex,
        pid: ProposalId,
        node_ids: Vec<NodeId>,
    },

    // the finalized entry under j. None when it needs no data
    PinFinal {
        j: ProposalIndex,
        pid: Option<ProposalId>,
    },

    // headers named by votes but not seen yet, with the voters that hold them
    PullHeader {
        j: ProposalIndex,
        missing: MissingHeaders,
    },

    // an unseen header from a peer
    IngestProposalHeader {
        sender: NodeId,
        header: SignedProposalHeader,
    },

    // track and pull the root `pid` under j from `voters` (its positive
    // voters, at least f+1) until it is decoded or fails to decode.
    // Re-emitted with a grown voter set as further votes arrive, and on
    // every retransmission tick until resolved.
    PullRoot {
        j: ProposalIndex,
        pid: ProposalId,
        voters: Vec<NodeId>,
    },
}

impl From<ChorusDACommand> for PqDACommand {
    fn from(command: ChorusDACommand) -> Self {
        match command {
            ChorusDACommand::ReleaseChunks => Self::ReleaseChunks,
            ChorusDACommand::AddDecodedHolders { j, pid, node_ids } => {
                Self::AddDecodedHolders { j, pid, node_ids }
            }
            ChorusDACommand::PinFinal { j, pid } => Self::PinFinal { j, pid },
            ChorusDACommand::PullHeader { j, missing } => Self::PullHeader { j, missing },
            ChorusDACommand::IngestProposalHeader { sender, header } => {
                Self::IngestProposalHeader { sender, header }
            }
        }
    }
}
