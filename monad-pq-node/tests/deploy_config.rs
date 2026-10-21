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

//! The tracked mcp_test configs that deploy/gen-config.sh renders: each is a
//! valid pq config with the §8.3 values and, unchanged, a monad-mcp-node
//! `NodeConfig` with an `[rpc]` table the unchanged rpc accepts (§5.1).

use std::path::{Path, PathBuf};

use monad_mcp_node::config::{NodeConfig, SourceKind};
use monad_mcp_rpc::config::RpcSection;
use monad_pq_node::{chorus::types::TimestampDelta, config::PqNodeConfig};
use serde::Deserialize;

fn deploy_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy")
}

// hosts.txt order is node_id order
fn hosts() -> Vec<String> {
    let text = std::fs::read_to_string(deploy_dir().join("hosts.txt")).unwrap();
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect()
}

fn config_text(host: &str) -> String {
    let path = deploy_dir().join("config").join(host).join("node.toml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn ms(millis: u64) -> TimestampDelta {
    TimestampDelta::from_millis(millis)
}

#[test]
fn every_host_config_is_a_valid_pq_config_with_the_mcp_test_values() {
    let hosts = hosts();
    assert_eq!(hosts.len(), 8);
    for (id, host) in hosts.iter().enumerate() {
        let config = PqNodeConfig::parse(&config_text(host)).unwrap();
        config.validate().unwrap();
        config.epoch().unwrap();

        let base = &config.base;
        assert_eq!(u64::from(base.node_id), id as u64, "{host}");
        assert_eq!(config.proposal_key, id as u64, "{host}");
        assert_eq!(base.validators.len(), hosts.len());
        assert_eq!(base.network.port, 8002);
        assert_eq!(base.cadence.delta, ms(150));
        assert_eq!(base.cadence.slot_interval, ms(100));
        assert_eq!(base.cadence.lag_threshold.get(), 20);
        assert_eq!(base.proposal.source, SourceKind::Mempool);
        assert_eq!(base.proposal.num_proposals, 5);
        assert_eq!(base.proposal.propose_before_deadline, ms(150));
        assert_eq!(base.proposal.withhold_before_deadline, ms(140));
        let repeater = base.repeater.unwrap();
        assert_eq!(repeater.interval, ms(500));
        assert_eq!(repeater.certificate_retention, 50);
        assert_eq!(
            base.ledger.dir,
            PathBuf::from("/home/monad/monad-pq/ledger")
        );

        let quantum = config.quantum_config();
        assert_eq!(quantum.retransmit_interval, ms(150));
        assert_eq!(quantum.decode_grace, ms(75));
        assert_eq!(quantum.coda_timeout_deltas, 5);
        assert!(!quantum.early_vote);
        assert!(config.has_rpc);
    }
}

// the rpc parses the node file as NodeConfig and its strict [rpc] table
// separately; the latency matrix it names must fit the validator set
#[test]
fn every_host_config_is_an_rpc_node_config() {
    #[derive(Deserialize)]
    struct LatencyFile {
        rtt_ms: Vec<Vec<f64>>,
    }

    for host in hosts() {
        let text = config_text(&host);
        let node: NodeConfig = toml::from_str(&text).unwrap();
        node.validate().unwrap();
        node.epoch_handle().unwrap();

        let rpc = RpcSection::parse(&text).unwrap();
        assert_eq!(rpc.resend_after_ms, Some(500));
        let latency = rpc.latency.expect("an [rpc] latency matrix");
        // shipped as config/latency.toml next to node.toml
        let path = deploy_dir().join("config").join(latency);
        let matrix: LatencyFile = toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let n = node.validators.len();
        assert_eq!(matrix.rtt_ms.len(), n);
        assert!(matrix.rtt_ms.iter().all(|row| row.len() == n));
    }
}

#[test]
fn every_host_config_has_the_same_network() {
    let shared = |host: &str| {
        let mut table: toml::Table = toml::from_str(&config_text(host)).unwrap();
        for key in ["node_id", "proposal_key_pair", "cadence_key_pair"] {
            table.remove(key).unwrap();
        }
        table
    };
    let hosts = hosts();
    let first = shared(&hosts[0]);
    for host in &hosts[1..] {
        assert_eq!(shared(host), first, "{host} differs from {}", hosts[0]);
    }
}
