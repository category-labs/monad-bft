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

use monad_crypto::certificate_signature::PubKey;
use monad_node_config::FullNodeRaptorCastConfig;
use thiserror::Error;

use crate::raptorcast_secondary::group_message::MAX_PEERS_IN_GROUP;

#[derive(Debug, Error)]
pub enum FullNodeRaptorCastConfigError {
    #[error(
        "fullnode_raptorcast.max_group_size = {value} exceeds MAX_PEERS_IN_GROUP = {max}; \
         secondary raptorcast `name_records` is a `LimitedVec<_, {max}>` and receivers drop \
         oversize ConfirmGroup messages at decode time"
    )]
    MaxGroupSizeTooLarge { value: usize, max: usize },
}

pub fn validate_fullnode_raptorcast_config<P: PubKey>(
    cfg: &FullNodeRaptorCastConfig<P>,
) -> Result<(), FullNodeRaptorCastConfigError> {
    if cfg.max_group_size > MAX_PEERS_IN_GROUP {
        return Err(FullNodeRaptorCastConfigError::MaxGroupSizeTooLarge {
            value: cfg.max_group_size,
            max: MAX_PEERS_IN_GROUP,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use monad_crypto::NopPubKey;
    use monad_node_config::FullNodeConfig;
    use monad_types::Round;

    use super::*;

    fn make_cfg(max_group_size: usize) -> FullNodeRaptorCastConfig<NopPubKey> {
        FullNodeRaptorCastConfig {
            enable_publisher: false,
            enable_client: false,
            full_nodes_prioritized: FullNodeConfig {
                identities: Vec::new(),
            },
            raptor10_fullnode_redundancy_factor: 2.0,
            round_span: Round(120),
            invite_lookahead: Round(1200),
            max_invite_wait: Round(2),
            deadline_round_dist: Round(10),
            init_empty_round_span: Round(18),
            max_group_size,
            max_num_group: 3,
            invite_future_dist_min: Round(1),
            invite_future_dist_max: Round(600),
            invite_accept_heartbeat_ms: 10_000,
        }
    }

    #[test]
    fn accepts_max_group_size_at_limit() {
        let cfg = make_cfg(MAX_PEERS_IN_GROUP);
        assert!(validate_fullnode_raptorcast_config(&cfg).is_ok());
    }

    #[test]
    fn accepts_typical_max_group_size() {
        let cfg = make_cfg(50);
        assert!(validate_fullnode_raptorcast_config(&cfg).is_ok());
    }

    #[test]
    fn rejects_max_group_size_above_limit() {
        let cfg = make_cfg(MAX_PEERS_IN_GROUP + 1);
        let err = validate_fullnode_raptorcast_config(&cfg).unwrap_err();
        match err {
            FullNodeRaptorCastConfigError::MaxGroupSizeTooLarge { value, max } => {
                assert_eq!(value, MAX_PEERS_IN_GROUP + 1);
                assert_eq!(max, MAX_PEERS_IN_GROUP);
            }
        }
    }
}
