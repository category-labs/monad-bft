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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// Message is signed by an author not in the validator set
    InvalidAuthor,
    /// Message does not contain the proper QC or TC values
    NotWellFormed,
    /// Bad signature
    InvalidSignature,
    /// There are high qc rounds larger than the TC round
    InvalidTcRound,
    /// There are duplicate (tip_round, high_qc_round) in the TC
    DuplicateTcTipRound,
    /// Empty signers for a (tip_round, high_qc_round) in the TC
    EmptySignersTcTipRound,
    /// Too many (tip_round, high_qc_round) in the TC
    TooManyTcTipRound,
    /// The SignatureCollection doesn't have supermajority of the stake signed
    InsufficientStake,
    /// Required validator set (or cert pubkeys) not in validators epoch mapping
    ValidatorSetDataUnavailable,
    /// Signatures contain duplicate node id
    SignaturesDuplicateNode,
    /// Vote does not contain a valid commit condition
    InvalidVote,
    /// Consensus Message version must match
    InvalidVersion,
    /// Epoch number in message doesn't match local records
    InvalidEpoch,
}
