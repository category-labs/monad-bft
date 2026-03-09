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

monad_executor::metric_consts! {
    pub COUNTER_PEER_SCORE_RECORD_CONTRIBUTION_TOTAL {
        name: "monad.peer_score.record_contribution.total",
        help: "Total non-zero contribution records processed by peer-score",
    }
    pub COUNTER_PEER_SCORE_NEWCOMER_ADMITTED {
        name: "monad.peer_score.newcomer.admitted",
        help: "Total newcomer identities admitted into the bounded newcomer pool",
    }
    pub COUNTER_PEER_SCORE_NEWCOMER_REJECTED {
        name: "monad.peer_score.newcomer.rejected",
        help: "Total newcomer identities rejected from the bounded newcomer pool",
    }
    pub COUNTER_PEER_SCORE_PROMOTION_SUCCEEDED {
        name: "monad.peer_score.promotion.succeeded",
        help: "Total newcomer identities promoted into the promoted pool",
    }
    pub COUNTER_PEER_SCORE_PROMOTION_REJECTED {
        name: "monad.peer_score.promotion.rejected",
        help: "Total promotion attempts rejected by the promoted pool admission policy",
    }
    pub COUNTER_PEER_SCORE_DEMOTION {
        name: "monad.peer_score.demotion",
        help: "Total promoted identities demoted back into the newcomer pool",
    }
    pub GAUGE_PEER_SCORE_PROMOTED_SIZE {
        name: "monad.peer_score.pool.promoted_size",
        help: "Current number of identities in the promoted pool",
    }
    pub GAUGE_PEER_SCORE_NEWCOMER_SIZE {
        name: "monad.peer_score.pool.newcomer_size",
        help: "Current number of identities in the newcomer pool",
    }
    pub GAUGE_PEER_SCORE_TOTAL_SIZE {
        name: "monad.peer_score.pool.total_size",
        help: "Current total number of identities tracked by peer-score",
    }
}
