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

use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use monad_mcp_node::{
    chorus::types::{NodeId, TimestampDelta},
    config::NodeConfig,
};
use serde::Deserialize;

use crate::{pending::PendingConfig, schedule::Planner};

pub const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:8080";

// the rpc beside one validator, whose config is its view of the validator
// set, the schedule and the clock
#[derive(Clone, Debug)]
pub struct RpcConfig {
    pub http_addr: String,
    // the colocated validator's id, the sender of every frame
    pub node_id: NodeId,
    pub peers: HashMap<NodeId, SocketAddr>,
    pub planner: Arc<Planner>,
    // the node's `ledger.dir`; only `<ledger_dir>/blocks` is read
    pub ledger_dir: PathBuf,
    pub resend_after: Duration,
    pub max_attempts: u32,
    pub max_pending: usize,
    // committed and failed txs kept for `GET /tx/{hash}`
    pub retain_finished: usize,
    pub poll_interval: Duration,
}

impl RpcConfig {
    // the defaults, beside `node`
    pub fn colocated(
        node: &NodeConfig,
        ledger_dir: impl Into<PathBuf>,
    ) -> Result<Self, ConfigError> {
        RpcSection::default().resolve(node, ledger_dir.into())
    }

    pub fn pending(&self) -> PendingConfig {
        PendingConfig {
            resend_after: self.resend_after,
            max_attempts: self.max_attempts,
            max_pending: self.max_pending,
            retain_finished: self.retain_finished,
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let check = |ok: bool, what: &str| {
            if ok {
                Ok(())
            } else {
                Err(ConfigError::Invalid(what.to_owned()))
            }
        };
        check(
            !self.resend_after.is_zero(),
            "resend_after_ms must be positive",
        )?;
        check(self.max_attempts > 0, "max_attempts must be positive")?;
        check(self.max_pending > 0, "max_pending must be positive")?;
        // with none kept, a finished tx would vanish from `GET /tx/{hash}` at once
        check(self.retain_finished > 0, "retain_finished must be positive")?;
        check(
            !self.poll_interval.is_zero(),
            "poll_interval_ms must be positive",
        )?;
        // one socket sends to every validator
        check(
            self.peers.values().all(SocketAddr::is_ipv4)
                || self.peers.values().all(SocketAddr::is_ipv6),
            "validator addresses must all be ipv4 or all ipv6",
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("parse {path}: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("missing rpc.{0}")]
    Missing(&'static str),
    #[error("{0}")]
    Invalid(String),
}

// the `[rpc]` table of a config file, which may be the node's own; every key
// is optional so flags can fill it
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcSection {
    pub http_addr: Option<String>,
    // the colocated validator's node toml
    pub node_config: Option<PathBuf>,
    // defaults to the node's `ledger.dir`
    pub ledger_dir: Option<PathBuf>,
    // added to the node's proposing lead and delta [default: one slot]
    pub lead_margin_ms: Option<u64>,
    pub resend_after_ms: Option<u64>,
    pub max_attempts: Option<u32>,
    pub max_pending: Option<usize>,
    pub retain_finished: Option<usize>,
    pub poll_interval_ms: Option<u64>,
}

#[derive(Deserialize)]
struct ConfigFile {
    #[serde(default)]
    rpc: RpcSection,
}

fn read(path: &PathBuf) -> Result<String, ConfigError> {
    std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.clone(),
        source,
    })
}

impl RpcSection {
    pub fn parse(text: &str) -> Result<Self, toml::de::Error> {
        Ok(toml::from_str::<ConfigFile>(text)?.rpc)
    }

    pub fn load(path: impl Into<PathBuf>) -> Result<Self, ConfigError> {
        let path = path.into();
        Self::parse(&read(&path)?).map_err(|source| ConfigError::Parse { path, source })
    }

    // fields set in `over` win
    pub fn merge(self, over: Self) -> Self {
        Self {
            http_addr: over.http_addr.or(self.http_addr),
            node_config: over.node_config.or(self.node_config),
            ledger_dir: over.ledger_dir.or(self.ledger_dir),
            lead_margin_ms: over.lead_margin_ms.or(self.lead_margin_ms),
            resend_after_ms: over.resend_after_ms.or(self.resend_after_ms),
            max_attempts: over.max_attempts.or(self.max_attempts),
            max_pending: over.max_pending.or(self.max_pending),
            retain_finished: over.retain_finished.or(self.retain_finished),
            poll_interval_ms: over.poll_interval_ms.or(self.poll_interval_ms),
        }
    }

    pub fn build(self) -> Result<RpcConfig, ConfigError> {
        let path = self
            .node_config
            .clone()
            .ok_or(ConfigError::Missing("node_config"))?;
        let node: NodeConfig =
            toml::from_str(&read(&path)?).map_err(|source| ConfigError::Parse { path, source })?;
        let ledger_dir = self
            .ledger_dir
            .clone()
            .or_else(|| node.ledger.as_ref().map(|ledger| ledger.dir.clone()))
            .ok_or(ConfigError::Missing("ledger_dir"))?;
        self.resolve(&node, ledger_dir)
    }

    fn resolve(self, node: &NodeConfig, ledger_dir: PathBuf) -> Result<RpcConfig, ConfigError> {
        let lead_margin = self.lead_margin_ms.map(TimestampDelta::from_millis);
        let planner = Planner::new(node, lead_margin)
            .map_err(|error| ConfigError::Invalid(format!("proposer schedule: {error:?}")))?;
        let config = RpcConfig {
            http_addr: self
                .http_addr
                .unwrap_or_else(|| DEFAULT_HTTP_ADDR.to_owned()),
            node_id: node.node_id,
            peers: node.peers(),
            planner: Arc::new(planner),
            ledger_dir,
            resend_after: Duration::from_millis(self.resend_after_ms.unwrap_or(3000)),
            max_attempts: self.max_attempts.unwrap_or(5),
            max_pending: self.max_pending.unwrap_or(10_000),
            retain_finished: self.retain_finished.unwrap_or(10_000),
            poll_interval: Duration::from_millis(self.poll_interval_ms.unwrap_or(200)),
        };
        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use monad_mcp_node::{
        chorus::types::{NodeId, Timestamp, TimestampDelta},
        config::NodeConfig,
    };

    use super::*;

    // a lone validator with a ledger and, below it, the rpc's own table
    const NODE: &str = r#"
node_id = 0
proposal_key_pair = 0
cadence_key_pair = 0
genesis_deadline = 1789412000000

[[validators]]
node_id = 0
stake = 1
chorus_pubkey = 0
address = "127.0.0.1:9000"

[[validators]]
node_id = 1
stake = 1
chorus_pubkey = 1
address = "127.0.0.1:9001"

[network]
port = 9000

[ledger]
dir = "/var/mcp/ledger"
"#;

    fn node_file(dir: &std::path::Path, text: &str) -> PathBuf {
        let path = dir.join("node.toml");
        std::fs::write(&path, text).unwrap();
        path
    }

    #[test]
    fn defaults_follow_the_plan() {
        let node = NodeConfig::single_node(9000, Timestamp::from_millis(0));
        let config = RpcConfig::colocated(&node, "/var/ledger").unwrap();
        assert_eq!(config.http_addr, DEFAULT_HTTP_ADDR);
        assert_eq!(config.ledger_dir, PathBuf::from("/var/ledger"));
        assert_eq!(config.resend_after, Duration::from_secs(3));
        assert_eq!(config.max_attempts, 5);
        assert_eq!(config.max_pending, 10_000);
        assert_eq!(config.poll_interval, Duration::from_millis(200));
        assert_eq!(config.node_id, NodeId::dummy(0));
        assert_eq!(config.peers, node.peers());
        // demo: 200 ms proposing lead, 50 ms delta, one 100 ms slot of margin
        assert_eq!(config.planner.lead(), TimestampDelta::from_millis(350));
    }

    // one toml holds the node's config and the rpc's [rpc] table
    #[test]
    fn the_rpc_reads_its_table_and_its_validator_from_the_node_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = node_file(
            dir.path(),
            &format!("{NODE}\n[rpc]\nhttp_addr = \"0.0.0.0:9100\"\nlead_margin_ms = 40\n"),
        );
        let node: NodeConfig = toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(node.peers().len(), 2);

        let file = RpcSection::load(&path).unwrap();
        assert_eq!(file.http_addr.as_deref(), Some("0.0.0.0:9100"));
        let flags = RpcSection {
            node_config: Some(path),
            ..RpcSection::default()
        };
        let config = file.merge(flags).build().unwrap();
        assert_eq!(config.http_addr, "0.0.0.0:9100");
        assert_eq!(config.node_id, NodeId::dummy(0));
        assert_eq!(config.peers, node.peers());
        // the node's own ledger.dir unless the rpc overrides it
        assert_eq!(config.ledger_dir, PathBuf::from("/var/mcp/ledger"));
        // default cadence (delta 150) and proposal lead (500), 40 ms margin
        assert_eq!(config.planner.lead(), TimestampDelta::from_millis(690));
    }

    #[test]
    fn a_file_section_is_overridden_by_flags() {
        let dir = tempfile::tempdir().unwrap();
        let path = node_file(dir.path(), NODE);
        let file = RpcSection::parse(
            r#"
            [rpc]
            http_addr = "0.0.0.0:9000"
            node_config = "/elsewhere.toml"
            ledger_dir = "/ledger"
            resend_after_ms = 500
            max_attempts = 2
            "#,
        )
        .unwrap();
        let flags = RpcSection {
            node_config: Some(path),
            max_attempts: Some(9),
            lead_margin_ms: Some(0),
            ..RpcSection::default()
        };
        let config = file.merge(flags).build().unwrap();
        assert_eq!(config.http_addr, "0.0.0.0:9000");
        assert_eq!(config.ledger_dir, PathBuf::from("/ledger"));
        assert_eq!(config.resend_after, Duration::from_millis(500));
        assert_eq!(config.max_attempts, 9);
        assert_eq!(config.planner.lead(), TimestampDelta::from_millis(650));
    }

    #[test]
    fn a_missing_node_config_zero_bounds_and_unknown_keys_are_errors() {
        assert!(matches!(
            RpcSection::default().build(),
            Err(ConfigError::Missing("node_config"))
        ));
        let dir = tempfile::tempdir().unwrap();
        let unreadable = RpcSection {
            node_config: Some(dir.path().join("absent.toml")),
            ..RpcSection::default()
        };
        assert!(matches!(unreadable.build(), Err(ConfigError::Read { .. })));
        let garbage = RpcSection {
            node_config: Some(node_file(dir.path(), "node_id = \"zero\"")),
            ..RpcSection::default()
        };
        assert!(matches!(garbage.build(), Err(ConfigError::Parse { .. })));

        // a node config without [ledger] needs the rpc's ledger_dir
        let (without_ledger, _) = NODE.split_once("[ledger]").unwrap();
        let section = RpcSection {
            node_config: Some(node_file(dir.path(), without_ledger)),
            ..RpcSection::default()
        };
        assert!(matches!(
            section.clone().build(),
            Err(ConfigError::Missing("ledger_dir"))
        ));
        let zero = RpcSection {
            ledger_dir: Some("/l".into()),
            max_attempts: Some(0),
            ..section
        };
        assert!(matches!(zero.clone().build(), Err(ConfigError::Invalid(_))));
        let no_retention = RpcSection {
            max_attempts: None,
            retain_finished: Some(0),
            ..zero
        };
        assert!(matches!(
            no_retention.build(),
            Err(ConfigError::Invalid(what)) if what.contains("retain_finished")
        ));
        // one socket reaches either family, not both
        let mixed = RpcSection {
            node_config: Some(node_file(
                dir.path(),
                &NODE.replace("127.0.0.1:9001", "[::1]:9001"),
            )),
            ..RpcSection::default()
        };
        assert!(matches!(
            mixed.build(),
            Err(ConfigError::Invalid(what)) if what.contains("ipv4")
        ));
        // the socket is gone
        assert!(RpcSection::parse("[rpc]\nnode_socket = \"/a\"").is_err());
        // a file of only other tables is an empty section
        assert!(
            RpcSection::parse("[node]\nx = 1")
                .unwrap()
                .node_config
                .is_none()
        );
    }
}
