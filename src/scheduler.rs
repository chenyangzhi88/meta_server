use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};

use crate::command::MetaCommand;
use crate::model::{SchedulerCommandKind, SchedulerCommandSpec};
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

fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
