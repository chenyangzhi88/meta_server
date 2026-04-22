use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};

use crate::command::MetaCommand;
use crate::model::{
    NodeState, SchedulerCommandKind, SchedulerCommandSpec, ServerRole, StreamAssignmentState,
    WalRaftGroup, WalRaftGroupState, WalRaftReplica,
};
use crate::state_machine::MetaStateMachine;

#[derive(Clone)]
pub struct TabletScheduler {
    state_machine: MetaStateMachine,
}

impl TabletScheduler {
    pub fn new(state_machine: MetaStateMachine) -> Self {
        Self { state_machine }
    }

    pub fn begin_migration(
        &self,
        tablet_id: u64,
        source_node_id: u64,
        target_node_id: u64,
    ) -> Result<Vec<MetaCommand>> {
        let route = self
            .state_machine
            .route(tablet_id)
            .ok_or_else(|| anyhow!("tablet route {tablet_id} not found"))?;
        if route.leader_cache_node_id != source_node_id {
            return Err(anyhow!(
                "tablet {} leader mismatch: expected {}, actual {}",
                tablet_id,
                source_node_id,
                route.leader_cache_node_id
            ));
        }

        let drain_epoch = route.epoch + 1;
        Ok(vec![
            self.create_scheduler_command(
                source_node_id,
                SchedulerCommandKind::DropTablet,
                tablet_id,
                route.epoch,
                route.wal_replica_node_ids.clone(),
                "stop serving requests on source cache node".to_string(),
            ),
            self.create_scheduler_command(
                target_node_id,
                SchedulerCommandKind::LoadTablet,
                tablet_id,
                drain_epoch,
                route.wal_replica_node_ids.clone(),
                "load tablet from wal replicas before takeover".to_string(),
            ),
            MetaCommand::BumpTabletEpoch {
                tablet_id,
                next_epoch: drain_epoch,
                next_leader_cache_node_id: None,
            },
        ])
    }

    pub fn complete_migration(&self, tablet_id: u64, target_node_id: u64) -> Result<MetaCommand> {
        let route = self
            .state_machine
            .route(tablet_id)
            .ok_or_else(|| anyhow!("tablet route {tablet_id} not found"))?;
        Ok(MetaCommand::BumpTabletEpoch {
            tablet_id,
            next_epoch: route.epoch + 1,
            next_leader_cache_node_id: Some(target_node_id),
        })
    }

    pub fn failover_from_down_node(
        &self,
        tablet_id: u64,
        source_node_id: u64,
        target_node_id: u64,
    ) -> Result<Vec<MetaCommand>> {
        let route = self
            .state_machine
            .route(tablet_id)
            .ok_or_else(|| anyhow!("tablet route {tablet_id} not found"))?;
        if route.leader_cache_node_id != source_node_id {
            return Err(anyhow!(
                "tablet {} leader mismatch: expected {}, actual {}",
                tablet_id,
                source_node_id,
                route.leader_cache_node_id
            ));
        }

        let next_epoch = route.epoch + 1;
        Ok(vec![
            self.create_scheduler_command(
                target_node_id,
                SchedulerCommandKind::LoadTablet,
                tablet_id,
                next_epoch,
                route.wal_replica_node_ids.clone(),
                format!(
                    "take over tablet after source cache node {} timed out",
                    source_node_id
                ),
            ),
            MetaCommand::BumpTabletEpoch {
                tablet_id,
                next_epoch,
                next_leader_cache_node_id: Some(target_node_id),
            },
        ])
    }

    fn create_scheduler_command(
        &self,
        target_node_id: u64,
        kind: SchedulerCommandKind,
        tablet_id: u64,
        epoch: u64,
        wal_replica_node_ids: Vec<u64>,
        detail: String,
    ) -> MetaCommand {
        MetaCommand::CreateSchedulerCommand {
            target_node_id,
            spec: SchedulerCommandSpec {
                kind,
                tablet_id,
                epoch,
                wal_replica_node_ids,
                detail,
            },
            created_at_ts: now_ts(),
        }
    }
}

#[derive(Clone)]
pub struct WalGroupScheduler {
    state_machine: MetaStateMachine,
}

impl WalGroupScheduler {
    pub fn new(state_machine: MetaStateMachine) -> Self {
        Self { state_machine }
    }

    pub fn plan_create_group(
        &self,
        cluster_name: String,
        raft_group_id: u64,
        epoch: u64,
        replicas: Vec<WalRaftReplica>,
    ) -> Result<MetaCommand> {
        let replicas = if replicas.is_empty() {
            self.pick_wal_replicas(3)?
        } else {
            replicas
        };
        let leader_node_id = replicas
            .first()
            .map(|replica| replica.node_id)
            .ok_or_else(|| anyhow!("wal raft group replicas cannot be empty"))?;
        Ok(MetaCommand::CreateWalRaftGroup {
            cluster_name,
            raft_group_id,
            epoch,
            replicas: replicas
                .into_iter()
                .map(|replica| (replica.node_id, replica.address))
                .collect(),
            leader_node_id,
            state: WalRaftGroupState::Creating,
        })
    }

    pub fn plan_activate_group(
        &self,
        cluster_name: String,
        raft_group_id: u64,
        epoch: u64,
    ) -> Result<MetaCommand> {
        let group = self
            .state_machine
            .wal_raft_group(&cluster_name, raft_group_id)
            .ok_or_else(|| anyhow!("wal raft group {raft_group_id} not found"))?;
        Ok(MetaCommand::UpdateWalRaftGroupState {
            cluster_name,
            raft_group_id,
            epoch,
            leader_node_id: Some(group.leader_node_id),
            state: WalRaftGroupState::Active,
        })
    }

    pub fn pick_group_for_stream(&self, cluster_name: &str) -> Result<WalRaftGroup> {
        self.state_machine
            .wal_raft_groups(cluster_name)
            .into_iter()
            .filter(|group| {
                matches!(
                    group.state,
                    WalRaftGroupState::Creating | WalRaftGroupState::Active
                )
            })
            .min_by_key(|group| {
                (
                    self.state_machine
                        .stream_assignments(cluster_name, Some(group.raft_group_id))
                        .into_iter()
                        .filter(|assignment| assignment.state != StreamAssignmentState::Unassigned)
                        .count(),
                    group.raft_group_id,
                )
            })
            .ok_or_else(|| anyhow!("no wal raft group available in cluster {}", cluster_name))
    }

    pub fn pick_reassignment_target(
        &self,
        cluster_name: &str,
        source_raft_group_id: u64,
    ) -> Result<WalRaftGroup> {
        self.state_machine
            .wal_raft_groups(cluster_name)
            .into_iter()
            .filter(|group| {
                group.raft_group_id != source_raft_group_id
                    && matches!(
                        group.state,
                        WalRaftGroupState::Creating | WalRaftGroupState::Active
                    )
            })
            .min_by_key(|group| {
                (
                    self.state_machine
                        .stream_assignments(cluster_name, Some(group.raft_group_id))
                        .into_iter()
                        .filter(|assignment| assignment.state != StreamAssignmentState::Unassigned)
                        .count(),
                    group.raft_group_id,
                )
            })
            .ok_or_else(|| {
                anyhow!(
                    "no wal raft group available for reassignment in cluster {}",
                    cluster_name
                )
            })
    }

    fn pick_wal_replicas(&self, desired: usize) -> Result<Vec<WalRaftReplica>> {
        let mut replicas = self
            .state_machine
            .nodes()
            .into_iter()
            .filter(|node| node.role == ServerRole::WalServer && node.state == NodeState::Up)
            .map(|node| WalRaftReplica {
                node_id: node.node_id,
                address: node.address,
            })
            .collect::<Vec<_>>();
        replicas.sort_by_key(|replica| replica.node_id);
        if replicas.len() < desired {
            return Err(anyhow!(
                "not enough wal servers to place raft group: need {}, have {}",
                desired,
                replicas.len()
            ));
        }
        replicas.truncate(desired);
        Ok(replicas)
    }
}

fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::WalGroupScheduler;
    use crate::command::MetaCommand;
    use crate::model::{
        NodeState, ServerNode, ServerRole, StreamAssignmentState, WalRaftGroupState,
    };
    use crate::state_machine::MetaStateMachine;

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
    fn wal_scheduler_picks_least_loaded_group() {
        let state_machine = MetaStateMachine::default();
        for node in [
            wal_node(1, "127.0.0.1:9001"),
            wal_node(2, "127.0.0.1:9002"),
            wal_node(3, "127.0.0.1:9003"),
            wal_node(4, "127.0.0.1:9004"),
            wal_node(5, "127.0.0.1:9005"),
            wal_node(6, "127.0.0.1:9006"),
        ] {
            state_machine.apply(MetaCommand::UpsertNode(node)).unwrap();
        }
        for command in [
            MetaCommand::CreateWalRaftGroup {
                cluster_name: "wal-c".to_string(),
                raft_group_id: 1,
                epoch: 1,
                replicas: vec![
                    (1, "127.0.0.1:9001".to_string()),
                    (2, "127.0.0.1:9002".to_string()),
                    (3, "127.0.0.1:9003".to_string()),
                ],
                leader_node_id: 1,
                state: WalRaftGroupState::Active,
            },
            MetaCommand::CreateWalRaftGroup {
                cluster_name: "wal-c".to_string(),
                raft_group_id: 2,
                epoch: 1,
                replicas: vec![
                    (4, "127.0.0.1:9004".to_string()),
                    (5, "127.0.0.1:9005".to_string()),
                    (6, "127.0.0.1:9006".to_string()),
                ],
                leader_node_id: 4,
                state: WalRaftGroupState::Active,
            },
            MetaCommand::AssignStream {
                cluster_name: "wal-c".to_string(),
                stream_id: 11,
                raft_group_id: 1,
                epoch: 1,
                state: StreamAssignmentState::Assigned,
            },
        ] {
            state_machine.apply(command).unwrap();
        }

        let scheduler = WalGroupScheduler::new(state_machine.clone());
        assert_eq!(
            scheduler
                .pick_group_for_stream("wal-c")
                .unwrap()
                .raft_group_id,
            2
        );
        assert_eq!(
            scheduler
                .pick_reassignment_target("wal-c", 1)
                .unwrap()
                .raft_group_id,
            2
        );
    }
}
