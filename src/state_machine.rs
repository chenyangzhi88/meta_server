use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use parking_lot::RwLock;

use crate::command::MetaCommand;
use crate::model::{
    NodeState, SchedulerCommand, SchedulerCommandRecord, SchedulerCommandStatus, ServerNode,
    StreamAssignment, StreamAssignmentState, TabletOperatorAlert, TabletRoute, WalRaftGroup,
    WalRaftGroupState, WalRaftReplica,
};

struct StateMachineCore {
    nodes: HashMap<u64, ServerNode>,
    routes: HashMap<u64, TabletRoute>,
    wal_raft_groups: HashMap<(String, u64), WalRaftGroup>,
    stream_assignments: HashMap<(String, u64), StreamAssignment>,
    scheduler_commands: HashMap<u64, SchedulerCommandRecord>,
    tablet_alerts: HashMap<u64, TabletOperatorAlert>,
    next_scheduler_command_id: u64,
    next_wal_raft_group_id: u64,
    next_stream_id: u64,
}

impl Default for StateMachineCore {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            routes: HashMap::new(),
            wal_raft_groups: HashMap::new(),
            stream_assignments: HashMap::new(),
            scheduler_commands: HashMap::new(),
            tablet_alerts: HashMap::new(),
            next_scheduler_command_id: 1,
            next_wal_raft_group_id: 1,
            next_stream_id: 1,
        }
    }
}

#[derive(Clone, Default)]
pub struct MetaStateMachine {
    inner: Arc<RwLock<StateMachineCore>>,
}

impl MetaStateMachine {
    pub fn apply(&self, command: MetaCommand) -> Result<()> {
        let mut inner = self.inner.write();
        match command {
            MetaCommand::UpsertNode(node) => {
                inner.nodes.insert(node.node_id, node);
            }
            MetaCommand::UpdateNodeHeartbeat {
                node_id,
                last_heartbeat_ts,
            } => {
                let node = inner
                    .nodes
                    .get_mut(&node_id)
                    .with_context(|| format!("node {node_id} not found"))?;
                node.last_heartbeat_ts = last_heartbeat_ts;
                if node.state != NodeState::Tombstone {
                    node.state = NodeState::Up;
                }
            }
            MetaCommand::MarkNodeState { node_id, state } => {
                let node = inner
                    .nodes
                    .get_mut(&node_id)
                    .with_context(|| format!("node {node_id} not found"))?;
                node.state = state;
            }
            MetaCommand::UpsertTabletRoute(route) => {
                Self::validate_route_epoch(inner.routes.get(&route.tablet_id), &route)?;
                inner.routes.insert(route.tablet_id, route);
            }
            MetaCommand::BumpTabletEpoch {
                tablet_id,
                next_epoch,
                next_leader_cache_node_id,
            } => {
                let route = inner
                    .routes
                    .get_mut(&tablet_id)
                    .with_context(|| format!("tablet route {tablet_id} not found"))?;
                if next_epoch <= route.epoch {
                    return Err(anyhow!(
                        "epoch regression for tablet {}: {} -> {}",
                        tablet_id,
                        route.epoch,
                        next_epoch
                    ));
                }
                route.epoch = next_epoch;
                route.leader_cache_node_id = next_leader_cache_node_id.unwrap_or_default();
                if route.leader_cache_node_id != 0 {
                    inner.tablet_alerts.remove(&tablet_id);
                }
            }
            MetaCommand::CreateSchedulerCommand {
                target_node_id,
                spec,
                created_at_ts,
            } => {
                let command_id = inner.next_scheduler_command_id;
                inner.next_scheduler_command_id += 1;
                inner.scheduler_commands.insert(
                    command_id,
                    SchedulerCommandRecord {
                        command_id,
                        target_node_id,
                        status: SchedulerCommandStatus::Pending,
                        created_at_ts,
                        acked_at_ts: None,
                        completed_at_ts: None,
                        failed_at_ts: None,
                        failure_reason: None,
                        command: SchedulerCommand {
                            command_id,
                            kind: spec.kind,
                            tablet_id: spec.tablet_id,
                            epoch: spec.epoch,
                            wal_replica_node_ids: spec.wal_replica_node_ids,
                            detail: spec.detail,
                        },
                    },
                );
            }
            MetaCommand::AckSchedulerCommand {
                command_id,
                acked_at_ts,
            } => {
                let record = inner
                    .scheduler_commands
                    .get_mut(&command_id)
                    .with_context(|| format!("scheduler command {command_id} not found for ack"))?;
                if matches!(
                    record.status,
                    SchedulerCommandStatus::Completed | SchedulerCommandStatus::Failed
                ) {
                    return Ok(());
                }
                record.status = SchedulerCommandStatus::Acked;
                record.acked_at_ts = Some(acked_at_ts);
            }
            MetaCommand::CompleteSchedulerCommand {
                command_id,
                completed_at_ts,
            } => {
                let record = inner
                    .scheduler_commands
                    .get_mut(&command_id)
                    .with_context(|| {
                        format!("scheduler command {command_id} not found for completion")
                    })?;
                record.status = SchedulerCommandStatus::Completed;
                if record.acked_at_ts.is_none() {
                    record.acked_at_ts = Some(completed_at_ts);
                }
                record.completed_at_ts = Some(completed_at_ts);
                record.failed_at_ts = None;
                record.failure_reason = None;
            }
            MetaCommand::FailSchedulerCommand {
                command_id,
                failed_at_ts,
                reason,
            } => {
                let record = inner
                    .scheduler_commands
                    .get_mut(&command_id)
                    .with_context(|| {
                        format!("scheduler command {command_id} not found for failure")
                    })?;
                if record.status == SchedulerCommandStatus::Completed {
                    return Err(anyhow!(
                        "cannot fail completed scheduler command {command_id}"
                    ));
                }
                record.status = SchedulerCommandStatus::Failed;
                if record.acked_at_ts.is_none() {
                    record.acked_at_ts = Some(failed_at_ts);
                }
                record.failed_at_ts = Some(failed_at_ts);
                record.failure_reason = Some(reason);
            }
            MetaCommand::UpsertTabletOperatorAlert {
                tablet_id,
                reason,
                created_at_ts,
            } => {
                inner.tablet_alerts.insert(
                    tablet_id,
                    TabletOperatorAlert {
                        tablet_id,
                        reason,
                        created_at_ts,
                    },
                );
            }
            MetaCommand::ClearTabletOperatorAlert { tablet_id } => {
                inner.tablet_alerts.remove(&tablet_id);
            }
            MetaCommand::CreateWalRaftGroup {
                cluster_name,
                raft_group_id,
                epoch,
                replicas,
                leader_node_id,
                state,
            } => {
                let cluster_name = normalize_cluster_name(cluster_name)?;
                let raft_group_id =
                    Self::allocate_id(&mut inner.next_wal_raft_group_id, raft_group_id);
                let replicas = Self::validate_wal_replicas(&inner.nodes, replicas)?;
                let key = (cluster_name.clone(), raft_group_id);
                if inner.wal_raft_groups.contains_key(&key) {
                    return Err(anyhow!(
                        "wal raft group {} already exists in cluster {}",
                        raft_group_id,
                        cluster_name
                    ));
                }
                Self::validate_group_state_transition(None, state)?;
                if leader_node_id != 0
                    && !replicas
                        .iter()
                        .any(|replica| replica.node_id == leader_node_id)
                {
                    return Err(anyhow!(
                        "wal raft group {} leader {} is not in replicas",
                        raft_group_id,
                        leader_node_id
                    ));
                }
                inner.wal_raft_groups.insert(
                    key,
                    WalRaftGroup {
                        cluster_name,
                        raft_group_id,
                        epoch,
                        replicas,
                        leader_node_id,
                        state,
                    },
                );
            }
            MetaCommand::UpdateWalRaftGroupState {
                cluster_name,
                raft_group_id,
                epoch,
                leader_node_id,
                state,
            } => {
                let cluster_name = normalize_cluster_name(cluster_name)?;
                let group = inner
                    .wal_raft_groups
                    .get_mut(&(cluster_name.clone(), raft_group_id))
                    .with_context(|| {
                        format!(
                            "wal raft group {} not found in cluster {}",
                            raft_group_id, cluster_name
                        )
                    })?;
                if epoch <= group.epoch {
                    return Err(anyhow!(
                        "epoch regression for wal raft group {} in cluster {}: {} -> {}",
                        raft_group_id,
                        cluster_name,
                        group.epoch,
                        epoch
                    ));
                }
                Self::validate_group_state_transition(Some(group.state), state)?;
                if let Some(leader_node_id) = leader_node_id {
                    if leader_node_id != 0
                        && !group
                            .replicas
                            .iter()
                            .any(|replica| replica.node_id == leader_node_id)
                    {
                        return Err(anyhow!(
                            "wal raft group {} leader {} is not in replicas",
                            raft_group_id,
                            leader_node_id
                        ));
                    }
                    group.leader_node_id = leader_node_id;
                }
                group.epoch = epoch;
                group.state = state;
            }
            MetaCommand::DeleteWalRaftGroup {
                cluster_name,
                raft_group_id,
                epoch,
            } => {
                let cluster_name = normalize_cluster_name(cluster_name)?;
                let key = (cluster_name.clone(), raft_group_id);
                let group = inner.wal_raft_groups.get(&key).with_context(|| {
                    format!(
                        "wal raft group {} not found in cluster {}",
                        raft_group_id, cluster_name
                    )
                })?;
                if epoch <= group.epoch {
                    return Err(anyhow!(
                        "epoch regression for wal raft group {} in cluster {}: {} -> {}",
                        raft_group_id,
                        cluster_name,
                        group.epoch,
                        epoch
                    ));
                }
                if inner.stream_assignments.values().any(|assignment| {
                    assignment.cluster_name == cluster_name
                        && assignment.raft_group_id == raft_group_id
                        && assignment.state != StreamAssignmentState::Unassigned
                }) {
                    return Err(anyhow!(
                        "wal raft group {} in cluster {} still has assigned streams",
                        raft_group_id,
                        cluster_name
                    ));
                }
                inner.wal_raft_groups.remove(&key);
            }
            MetaCommand::AssignStream {
                cluster_name,
                stream_id,
                raft_group_id,
                epoch,
                state,
            } => {
                let cluster_name = normalize_cluster_name(cluster_name)?;
                let stream_id = Self::allocate_id(&mut inner.next_stream_id, stream_id);
                let group = inner
                    .wal_raft_groups
                    .get(&(cluster_name.clone(), raft_group_id))
                    .with_context(|| {
                        format!(
                            "wal raft group {} not found in cluster {}",
                            raft_group_id, cluster_name
                        )
                    })?;
                if group.state == WalRaftGroupState::Deleting {
                    return Err(anyhow!(
                        "wal raft group {} in cluster {} is deleting",
                        raft_group_id,
                        cluster_name
                    ));
                }
                let key = (cluster_name.clone(), stream_id);
                match inner.stream_assignments.get_mut(&key) {
                    Some(assignment) => {
                        if epoch <= assignment.epoch {
                            return Err(anyhow!(
                                "epoch regression for stream {} in cluster {}: {} -> {}",
                                stream_id,
                                cluster_name,
                                assignment.epoch,
                                epoch
                            ));
                        }
                        assignment.raft_group_id = raft_group_id;
                        assignment.epoch = epoch;
                        assignment.state = state;
                    }
                    None => {
                        inner.stream_assignments.insert(
                            key,
                            StreamAssignment {
                                cluster_name,
                                stream_id,
                                raft_group_id,
                                epoch,
                                state,
                            },
                        );
                    }
                }
            }
            MetaCommand::UnassignStream {
                cluster_name,
                stream_id,
                epoch,
            } => {
                let cluster_name = normalize_cluster_name(cluster_name)?;
                let assignment = inner
                    .stream_assignments
                    .get_mut(&(cluster_name.clone(), stream_id))
                    .with_context(|| {
                        format!(
                            "stream assignment {} not found in cluster {}",
                            stream_id, cluster_name
                        )
                    })?;
                if epoch <= assignment.epoch {
                    return Err(anyhow!(
                        "epoch regression for stream {} in cluster {}: {} -> {}",
                        stream_id,
                        cluster_name,
                        assignment.epoch,
                        epoch
                    ));
                }
                assignment.epoch = epoch;
                assignment.raft_group_id = 0;
                assignment.state = StreamAssignmentState::Unassigned;
            }
        }
        Ok(())
    }

    pub fn route(&self, tablet_id: u64) -> Option<TabletRoute> {
        self.inner.read().routes.get(&tablet_id).cloned()
    }

    pub fn node(&self, node_id: u64) -> Option<ServerNode> {
        self.inner.read().nodes.get(&node_id).cloned()
    }

    pub fn nodes(&self) -> Vec<ServerNode> {
        self.inner.read().nodes.values().cloned().collect()
    }

    pub fn routes(&self) -> Vec<TabletRoute> {
        self.inner.read().routes.values().cloned().collect()
    }

    pub fn wal_raft_group(&self, cluster_name: &str, raft_group_id: u64) -> Option<WalRaftGroup> {
        self.inner
            .read()
            .wal_raft_groups
            .get(&(cluster_name.to_string(), raft_group_id))
            .cloned()
    }

    pub fn wal_raft_groups(&self, cluster_name: &str) -> Vec<WalRaftGroup> {
        let mut groups = self
            .inner
            .read()
            .wal_raft_groups
            .values()
            .filter(|group| group.cluster_name == cluster_name)
            .cloned()
            .collect::<Vec<_>>();
        groups.sort_by_key(|group| group.raft_group_id);
        groups
    }

    pub fn next_wal_raft_group_id(&self) -> u64 {
        self.inner.read().next_wal_raft_group_id
    }

    pub fn stream_assignment(
        &self,
        cluster_name: &str,
        stream_id: u64,
    ) -> Option<StreamAssignment> {
        self.inner
            .read()
            .stream_assignments
            .get(&(cluster_name.to_string(), stream_id))
            .cloned()
    }

    pub fn stream_assignments(
        &self,
        cluster_name: &str,
        raft_group_id: Option<u64>,
    ) -> Vec<StreamAssignment> {
        let mut assignments = self
            .inner
            .read()
            .stream_assignments
            .values()
            .filter(|assignment| {
                assignment.cluster_name == cluster_name
                    && raft_group_id.is_none_or(|group_id| assignment.raft_group_id == group_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        assignments.sort_by_key(|assignment| assignment.stream_id);
        assignments
    }

    pub fn next_stream_id(&self) -> u64 {
        self.inner.read().next_stream_id
    }

    pub fn pending_scheduler_commands(&self, node_id: u64) -> Vec<SchedulerCommand> {
        let mut commands = self
            .inner
            .read()
            .scheduler_commands
            .values()
            .filter(|record| {
                record.target_node_id == node_id && record.status == SchedulerCommandStatus::Pending
            })
            .map(|record| record.command.clone())
            .collect::<Vec<_>>();
        commands.sort_by_key(|command| command.command_id);
        commands
    }

    pub fn scheduler_command(&self, command_id: u64) -> Option<SchedulerCommandRecord> {
        self.inner
            .read()
            .scheduler_commands
            .get(&command_id)
            .cloned()
    }

    pub fn scheduler_commands(&self) -> Vec<SchedulerCommandRecord> {
        let mut commands = self
            .inner
            .read()
            .scheduler_commands
            .values()
            .cloned()
            .collect::<Vec<_>>();
        commands.sort_by_key(|record| record.command_id);
        commands
    }

    pub fn next_scheduler_command_id(&self) -> u64 {
        self.inner.read().next_scheduler_command_id
    }

    pub fn tablet_alert(&self, tablet_id: u64) -> Option<TabletOperatorAlert> {
        self.inner.read().tablet_alerts.get(&tablet_id).cloned()
    }

    pub fn tablet_alerts(&self) -> Vec<TabletOperatorAlert> {
        let mut alerts = self
            .inner
            .read()
            .tablet_alerts
            .values()
            .cloned()
            .collect::<Vec<_>>();
        alerts.sort_by_key(|alert| alert.tablet_id);
        alerts
    }

    pub fn failed_scheduler_commands(&self) -> Vec<SchedulerCommandRecord> {
        let mut commands = self
            .inner
            .read()
            .scheduler_commands
            .values()
            .filter(|record| record.status == SchedulerCommandStatus::Failed)
            .cloned()
            .collect::<Vec<_>>();
        commands.sort_by_key(|record| record.command_id);
        commands
    }

    pub fn restore_from_snapshot_bytes(&self, bytes: &[u8]) -> Result<()> {
        #[derive(serde::Deserialize)]
        struct Snapshot {
            nodes: HashMap<u64, ServerNode>,
            routes: HashMap<u64, TabletRoute>,
            #[serde(default)]
            wal_raft_groups: HashMap<(String, u64), WalRaftGroup>,
            #[serde(default)]
            stream_assignments: HashMap<(String, u64), StreamAssignment>,
            #[serde(default)]
            scheduler_commands: HashMap<u64, SchedulerCommandRecord>,
            #[serde(default)]
            tablet_alerts: HashMap<u64, TabletOperatorAlert>,
            #[serde(default = "default_next_scheduler_command_id")]
            next_scheduler_command_id: u64,
            #[serde(default = "default_next_wal_raft_group_id")]
            next_wal_raft_group_id: u64,
            #[serde(default = "default_next_stream_id")]
            next_stream_id: u64,
        }

        let snapshot: Snapshot =
            serde_json::from_slice(bytes).context("deserialize state machine snapshot")?;
        let mut inner = self.inner.write();
        inner.nodes = snapshot.nodes;
        inner.routes = snapshot.routes;
        inner.wal_raft_groups = snapshot.wal_raft_groups;
        inner.stream_assignments = snapshot.stream_assignments;
        inner.scheduler_commands = snapshot.scheduler_commands;
        inner.tablet_alerts = snapshot.tablet_alerts;
        inner.next_scheduler_command_id = snapshot.next_scheduler_command_id.max(
            inner
                .scheduler_commands
                .keys()
                .max()
                .copied()
                .map(|id| id + 1)
                .unwrap_or(1),
        );
        inner.next_wal_raft_group_id = snapshot.next_wal_raft_group_id.max(
            inner
                .wal_raft_groups
                .keys()
                .map(|(_, id)| *id)
                .max()
                .map(|id| id + 1)
                .unwrap_or(1),
        );
        inner.next_stream_id = snapshot.next_stream_id.max(
            inner
                .stream_assignments
                .keys()
                .map(|(_, id)| *id)
                .max()
                .map(|id| id + 1)
                .unwrap_or(1),
        );
        Ok(())
    }

    pub fn snapshot_bytes(&self) -> Result<Vec<u8>> {
        #[derive(serde::Serialize)]
        struct Snapshot<'a> {
            nodes: &'a HashMap<u64, ServerNode>,
            routes: &'a HashMap<u64, TabletRoute>,
            wal_raft_groups: &'a HashMap<(String, u64), WalRaftGroup>,
            stream_assignments: &'a HashMap<(String, u64), StreamAssignment>,
            scheduler_commands: &'a HashMap<u64, SchedulerCommandRecord>,
            tablet_alerts: &'a HashMap<u64, TabletOperatorAlert>,
            next_scheduler_command_id: u64,
            next_wal_raft_group_id: u64,
            next_stream_id: u64,
        }

        let inner = self.inner.read();
        serde_json::to_vec(&Snapshot {
            nodes: &inner.nodes,
            routes: &inner.routes,
            wal_raft_groups: &inner.wal_raft_groups,
            stream_assignments: &inner.stream_assignments,
            scheduler_commands: &inner.scheduler_commands,
            tablet_alerts: &inner.tablet_alerts,
            next_scheduler_command_id: inner.next_scheduler_command_id,
            next_wal_raft_group_id: inner.next_wal_raft_group_id,
            next_stream_id: inner.next_stream_id,
        })
        .context("serialize state machine snapshot")
    }

    fn validate_route_epoch(previous: Option<&TabletRoute>, next: &TabletRoute) -> Result<()> {
        if let Some(previous) = previous
            && next.epoch <= previous.epoch
        {
            return Err(anyhow!(
                "epoch regression for tablet {}: {} -> {}",
                next.tablet_id,
                previous.epoch,
                next.epoch
            ));
        }
        Ok(())
    }

    fn allocate_id(next_id: &mut u64, requested_id: u64) -> u64 {
        if requested_id == 0 {
            let allocated_id = *next_id;
            *next_id += 1;
            allocated_id
        } else {
            *next_id = (*next_id).max(requested_id + 1);
            requested_id
        }
    }

    fn validate_wal_replicas(
        nodes: &HashMap<u64, ServerNode>,
        replicas: Vec<(u64, String)>,
    ) -> Result<Vec<WalRaftReplica>> {
        if replicas.is_empty() {
            return Err(anyhow!("wal raft group replicas cannot be empty"));
        }
        let mut validated = Vec::with_capacity(replicas.len());
        for (node_id, address) in replicas {
            let node = nodes
                .get(&node_id)
                .with_context(|| format!("wal replica node {} not found", node_id))?;
            if node.role != crate::model::ServerRole::WalServer {
                return Err(anyhow!("node {} is not a wal server", node_id));
            }
            validated.push(WalRaftReplica { node_id, address });
        }
        Ok(validated)
    }

    fn validate_group_state_transition(
        previous: Option<WalRaftGroupState>,
        next: WalRaftGroupState,
    ) -> Result<()> {
        if matches!(previous, Some(WalRaftGroupState::Deleting)) {
            return Err(anyhow!("cannot transition wal raft group after deleting"));
        }
        if previous == Some(next) {
            return Err(anyhow!("wal raft group state transition is a no-op"));
        }
        Ok(())
    }
}

fn default_next_scheduler_command_id() -> u64 {
    1
}

fn default_next_wal_raft_group_id() -> u64 {
    1
}

fn default_next_stream_id() -> u64 {
    1
}

fn normalize_cluster_name(cluster_name: String) -> Result<String> {
    let normalized = cluster_name.trim();
    if normalized.is_empty() {
        return Err(anyhow!("cluster name cannot be empty"));
    }
    Ok(normalized.to_string())
}

#[cfg(test)]
mod tests {
    use super::MetaStateMachine;
    use crate::command::MetaCommand;
    use crate::model::{
        NodeState, ServerNode, ServerRole, StreamAssignmentState, WalRaftGroupState,
    };

    fn wal_node(node_id: u64, address: &str) -> ServerNode {
        ServerNode {
            node_id,
            address: address.to_string(),
            role: ServerRole::WalServer,
            state: NodeState::Up,
            last_heartbeat_ts: 1,
            cpu_usage: 0.1,
            memory_usage: 0.2,
            disk_free_gb: 100.0,
            wal_server_info: None,
        }
    }

    #[test]
    fn wal_group_and_stream_assignment_lifecycle_works() {
        let state_machine = MetaStateMachine::default();
        for node in [
            wal_node(101, "127.0.0.1:9101"),
            wal_node(102, "127.0.0.1:9102"),
            wal_node(103, "127.0.0.1:9103"),
            wal_node(104, "127.0.0.1:9104"),
        ] {
            state_machine.apply(MetaCommand::UpsertNode(node)).unwrap();
        }

        state_machine
            .apply(MetaCommand::CreateWalRaftGroup {
                cluster_name: "wal-a".to_string(),
                raft_group_id: 0,
                epoch: 1,
                replicas: vec![
                    (101, "127.0.0.1:9101".to_string()),
                    (102, "127.0.0.1:9102".to_string()),
                    (103, "127.0.0.1:9103".to_string()),
                ],
                leader_node_id: 101,
                state: WalRaftGroupState::Creating,
            })
            .unwrap();
        state_machine
            .apply(MetaCommand::UpdateWalRaftGroupState {
                cluster_name: "wal-a".to_string(),
                raft_group_id: 1,
                epoch: 2,
                leader_node_id: Some(101),
                state: WalRaftGroupState::Active,
            })
            .unwrap();
        state_machine
            .apply(MetaCommand::AssignStream {
                cluster_name: "wal-a".to_string(),
                stream_id: 0,
                raft_group_id: 1,
                epoch: 1,
                state: StreamAssignmentState::Assigned,
            })
            .unwrap();
        state_machine
            .apply(MetaCommand::AssignStream {
                cluster_name: "wal-a".to_string(),
                stream_id: 1,
                raft_group_id: 1,
                epoch: 2,
                state: StreamAssignmentState::Moving,
            })
            .unwrap();
        state_machine
            .apply(MetaCommand::UnassignStream {
                cluster_name: "wal-a".to_string(),
                stream_id: 1,
                epoch: 3,
            })
            .unwrap();

        let group = state_machine.wal_raft_group("wal-a", 1).unwrap();
        assert_eq!(group.state, WalRaftGroupState::Active);
        assert_eq!(group.replicas.len(), 3);
        assert_eq!(state_machine.next_wal_raft_group_id(), 2);

        let assignment = state_machine.stream_assignment("wal-a", 1).unwrap();
        assert_eq!(assignment.stream_id, 1);
        assert_eq!(assignment.raft_group_id, 0);
        assert_eq!(assignment.state, StreamAssignmentState::Unassigned);
        assert_eq!(state_machine.next_stream_id(), 2);
    }

    #[test]
    fn deleting_group_with_live_streams_is_rejected() {
        let state_machine = MetaStateMachine::default();
        for node in [
            wal_node(201, "127.0.0.1:9201"),
            wal_node(202, "127.0.0.1:9202"),
            wal_node(203, "127.0.0.1:9203"),
        ] {
            state_machine.apply(MetaCommand::UpsertNode(node)).unwrap();
        }

        state_machine
            .apply(MetaCommand::CreateWalRaftGroup {
                cluster_name: "wal-b".to_string(),
                raft_group_id: 9,
                epoch: 10,
                replicas: vec![
                    (201, "127.0.0.1:9201".to_string()),
                    (202, "127.0.0.1:9202".to_string()),
                    (203, "127.0.0.1:9203".to_string()),
                ],
                leader_node_id: 201,
                state: WalRaftGroupState::Active,
            })
            .unwrap();
        state_machine
            .apply(MetaCommand::AssignStream {
                cluster_name: "wal-b".to_string(),
                stream_id: 7,
                raft_group_id: 9,
                epoch: 1,
                state: StreamAssignmentState::Assigned,
            })
            .unwrap();

        let error = state_machine
            .apply(MetaCommand::DeleteWalRaftGroup {
                cluster_name: "wal-b".to_string(),
                raft_group_id: 9,
                epoch: 11,
            })
            .unwrap_err();
        assert!(error.to_string().contains("still has assigned streams"));
    }
}
