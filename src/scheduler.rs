use anyhow::{Result, anyhow};

use crate::command::MetaCommand;
use crate::model::{SchedulerCommand, SchedulerCommandKind};
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
        self.state_machine.enqueue_scheduler_command(
            source_node_id,
            SchedulerCommand {
                kind: SchedulerCommandKind::DropTablet,
                tablet_id,
                epoch: route.epoch,
                wal_replica_node_ids: route.wal_replica_node_ids.clone(),
                detail: "stop serving requests on source cache node".to_string(),
            },
        );
        self.state_machine.enqueue_scheduler_command(
            target_node_id,
            SchedulerCommand {
                kind: SchedulerCommandKind::LoadTablet,
                tablet_id,
                epoch: drain_epoch,
                wal_replica_node_ids: route.wal_replica_node_ids.clone(),
                detail: "load tablet from wal replicas before takeover".to_string(),
            },
        );

        Ok(vec![MetaCommand::BumpTabletEpoch {
            tablet_id,
            next_epoch: drain_epoch,
            next_leader_cache_node_id: None,
        }])
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
}
