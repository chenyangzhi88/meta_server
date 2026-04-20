use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct MetaConfigFile {
    pub node: NodeConfig,
    pub cluster: ClusterConfig,
    pub reconciler: Option<ReconcilerConfig>,
    pub observability: Option<ObservabilityConfig>,
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct NodeConfig {
    pub id: u64,
    pub listen_addr: String,
    pub advertise_addr: Option<String>,
    pub storage_dir: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterConfig {
    pub group_id: Option<String>,
    pub voters: Vec<u64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PeerConfig {
    pub id: u64,
    pub addr: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ReconcilerConfig {
    pub interval_ms: Option<u64>,
    pub heartbeat_timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ObservabilityConfig {
    pub http_listen_addr: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub node_id: u64,
    pub listen_addr: String,
    pub advertised_leader: Option<String>,
    pub storage_dir: String,
    pub voter_ids: Vec<u64>,
    pub peers: HashMap<u64, String>,
    pub group_id: Option<String>,
    pub config_path: Option<PathBuf>,
    pub reconcile_interval_ms: u64,
    pub heartbeat_timeout_secs: u64,
    pub http_listen_addr: Option<String>,
}

impl RuntimeConfig {
    pub fn load() -> Result<Self> {
        let config_path = std::env::var("META_CONFIG").ok().map(PathBuf::from);
        let file = match config_path.as_ref() {
            Some(path) => Some(load_file(path)?),
            None => load_default_file_if_exists()?,
        };

        let mut config = if let Some(file) = file {
            Self::from_file(file, config_path.clone())?
        } else {
            Self::from_env_only()?
        };
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }

    fn from_file(file: MetaConfigFile, config_path: Option<PathBuf>) -> Result<Self> {
        let peers = file
            .peers
            .into_iter()
            .map(|peer| (peer.id, peer.addr))
            .collect::<HashMap<_, _>>();
        Ok(Self {
            node_id: file.node.id,
            listen_addr: file.node.listen_addr.clone(),
            advertised_leader: Some(
                file.node
                    .advertise_addr
                    .unwrap_or_else(|| file.node.listen_addr.clone()),
            ),
            storage_dir: file.node.storage_dir,
            voter_ids: file.cluster.voters,
            peers,
            group_id: file.cluster.group_id,
            config_path,
            reconcile_interval_ms: file
                .reconciler
                .as_ref()
                .and_then(|cfg| cfg.interval_ms)
                .unwrap_or(1000),
            heartbeat_timeout_secs: file
                .reconciler
                .as_ref()
                .and_then(|cfg| cfg.heartbeat_timeout_secs)
                .unwrap_or(15),
            http_listen_addr: file
                .observability
                .as_ref()
                .and_then(|cfg| cfg.http_listen_addr.clone()),
        })
    }

    fn from_env_only() -> Result<Self> {
        Ok(Self {
            node_id: env_required_parse("META_NODE_ID")?,
            listen_addr: env_required("META_LISTEN_ADDR")?,
            advertised_leader: std::env::var("META_LEADER_HINT").ok(),
            storage_dir: env_required("META_STORAGE_DIR")?,
            voter_ids: parse_u64_csv(&env_required("META_VOTERS")?),
            peers: parse_peers(&std::env::var("META_PEERS").unwrap_or_default()),
            group_id: std::env::var("META_GROUP_ID").ok(),
            config_path: None,
            reconcile_interval_ms: 1000,
            heartbeat_timeout_secs: 15,
            http_listen_addr: None,
        })
    }

    fn apply_env_overrides(&mut self) -> Result<()> {
        if let Ok(value) = std::env::var("META_NODE_ID") {
            self.node_id = value
                .parse()
                .with_context(|| format!("parse META_NODE_ID from {value}"))?;
        }
        if let Ok(value) = std::env::var("META_LISTEN_ADDR") {
            self.listen_addr = value;
        }
        if let Ok(value) = std::env::var("META_STORAGE_DIR") {
            self.storage_dir = value;
        }
        if let Ok(value) = std::env::var("META_LEADER_HINT") {
            self.advertised_leader = Some(value);
        } else if self.advertised_leader.is_none() {
            self.advertised_leader = Some(self.listen_addr.clone());
        }
        if let Ok(value) = std::env::var("META_VOTERS") {
            self.voter_ids = parse_u64_csv(&value);
        }
        if let Ok(value) = std::env::var("META_PEERS") {
            self.peers = parse_peers(&value);
        }
        if let Ok(value) = std::env::var("META_GROUP_ID") {
            self.group_id = Some(value);
        }
        if let Ok(value) = std::env::var("META_RECONCILE_INTERVAL_MS") {
            self.reconcile_interval_ms = value
                .parse()
                .with_context(|| format!("parse META_RECONCILE_INTERVAL_MS from {value}"))?;
        }
        if let Ok(value) = std::env::var("META_HEARTBEAT_TIMEOUT_SECS") {
            self.heartbeat_timeout_secs = value
                .parse()
                .with_context(|| format!("parse META_HEARTBEAT_TIMEOUT_SECS from {value}"))?;
        }
        if let Ok(value) = std::env::var("META_HTTP_LISTEN_ADDR") {
            self.http_listen_addr = Some(value);
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        if self.voter_ids.is_empty() {
            return Err(anyhow!("meta cluster voters must not be empty"));
        }
        if self.reconcile_interval_ms == 0 {
            return Err(anyhow!("reconcile interval must be greater than zero"));
        }
        if self.heartbeat_timeout_secs == 0 {
            return Err(anyhow!("heartbeat timeout must be greater than zero"));
        }
        if !self.voter_ids.contains(&self.node_id) {
            return Err(anyhow!(
                "node {} is not present in configured voters {:?}",
                self.node_id,
                self.voter_ids
            ));
        }
        for voter_id in &self.voter_ids {
            if *voter_id == self.node_id {
                continue;
            }
            if !self.peers.contains_key(voter_id) {
                return Err(anyhow!(
                    "missing peer address for voter {} in config",
                    voter_id
                ));
            }
        }
        Ok(())
    }
}

fn load_default_file_if_exists() -> Result<Option<MetaConfigFile>> {
    let path = Path::new("meta.toml");
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(load_file(path)?))
}

fn load_file(path: &Path) -> Result<MetaConfigFile> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("read meta config file {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parse meta config file {}", path.display()))
}

fn env_required(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("missing required env var {name}"))
}

fn env_required_parse<T>(name: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = env_required(name)?;
    value
        .parse()
        .map_err(|err| anyhow!("parse {name} from {value}: {err}"))
}

fn parse_u64_csv(input: &str) -> Vec<u64> {
    input
        .split(',')
        .filter_map(|item| item.trim().parse::<u64>().ok())
        .collect()
}

fn parse_peers(input: &str) -> HashMap<u64, String> {
    input
        .split(',')
        .filter_map(|item| {
            let (id, addr) = item.split_once('=')?;
            let id = id.trim().parse::<u64>().ok()?;
            Some((id, addr.trim().to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::MetaConfigFile;

    #[test]
    fn parse_toml_config_file() {
        let input = r#"
[node]
id = 1
listen_addr = "127.0.0.1:7001"
advertise_addr = "10.0.0.1:7001"
storage_dir = "./meta_data_1"

[cluster]
group_id = "meta-cluster-1"
voters = [1, 2, 3]

[[peers]]
id = 2
addr = "10.0.0.2:7001"

[[peers]]
id = 3
addr = "10.0.0.3:7001"

[reconciler]
interval_ms = 1000
heartbeat_timeout_secs = 15
"#;

        let config: MetaConfigFile = toml::from_str(input).unwrap();
        assert_eq!(config.node.id, 1);
        assert_eq!(config.node.listen_addr, "127.0.0.1:7001");
        assert_eq!(config.cluster.group_id.as_deref(), Some("meta-cluster-1"));
        assert_eq!(config.cluster.voters, vec![1, 2, 3]);
        assert_eq!(config.peers.len(), 2);
        assert_eq!(config.peers[0].id, 2);
        assert_eq!(config.peers[1].addr, "10.0.0.3:7001");
        assert_eq!(
            config.reconciler.as_ref().and_then(|cfg| cfg.interval_ms),
            Some(1000)
        );
    }
}
