use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServerRole {
    CacheServer,
    WalServer,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeState {
    Up,
    Down,
    Tombstone,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerNode {
    pub node_id: u64,
    pub address: String,
    pub role: ServerRole,
    pub state: NodeState,
    pub last_heartbeat_ts: u64,
    pub cpu_usage: f64,
    pub memory_usage: f64,
    pub disk_free_gb: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TabletRoute {
    pub tablet_id: u64,
    pub leader_cache_node_id: u64,
    pub wal_replica_node_ids: Vec<u64>,
    pub epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TabletOperatorAlert {
    pub tablet_id: u64,
    pub reason: String,
    pub created_at_ts: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchedulerCommandKind {
    DropTablet,
    LoadTablet,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchedulerCommandStatus {
    Pending,
    Acked,
    Completed,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchedulerCommandSpec {
    pub kind: SchedulerCommandKind,
    pub tablet_id: u64,
    pub epoch: u64,
    pub wal_replica_node_ids: Vec<u64>,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchedulerCommandRecord {
    pub command_id: u64,
    pub target_node_id: u64,
    pub status: SchedulerCommandStatus,
    pub created_at_ts: u64,
    pub acked_at_ts: Option<u64>,
    pub completed_at_ts: Option<u64>,
    pub failed_at_ts: Option<u64>,
    pub failure_reason: Option<String>,
    pub command: SchedulerCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchedulerCommand {
    pub command_id: u64,
    pub kind: SchedulerCommandKind,
    pub tablet_id: u64,
    pub epoch: u64,
    pub wal_replica_node_ids: Vec<u64>,
    pub detail: String,
}
