use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use parking_lot::RwLock;

use crate::command::MetaCommand;
use crate::model::{
    NodeState, SchedulerCommand, SchedulerCommandRecord, SchedulerCommandStatus, ServerNode,
    TabletOperatorAlert, TabletRoute,
};

struct StateMachineCore {
    nodes: HashMap<u64, ServerNode>,
    routes: HashMap<u64, TabletRoute>,
    scheduler_commands: HashMap<u64, SchedulerCommandRecord>,
    tablet_alerts: HashMap<u64, TabletOperatorAlert>,
    next_scheduler_command_id: u64,
}

impl Default for StateMachineCore {
    fn default() -> Self {
        Self {
            nodes: HashMap::new(),
            routes: HashMap::new(),
            scheduler_commands: HashMap::new(),
            tablet_alerts: HashMap::new(),
            next_scheduler_command_id: 1,
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
            scheduler_commands: HashMap<u64, SchedulerCommandRecord>,
            #[serde(default)]
            tablet_alerts: HashMap<u64, TabletOperatorAlert>,
            #[serde(default = "default_next_scheduler_command_id")]
            next_scheduler_command_id: u64,
        }

        let snapshot: Snapshot =
            serde_json::from_slice(bytes).context("deserialize state machine snapshot")?;
        let mut inner = self.inner.write();
        inner.nodes = snapshot.nodes;
        inner.routes = snapshot.routes;
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
        Ok(())
    }

    pub fn snapshot_bytes(&self) -> Result<Vec<u8>> {
        #[derive(serde::Serialize)]
        struct Snapshot<'a> {
            nodes: &'a HashMap<u64, ServerNode>,
            routes: &'a HashMap<u64, TabletRoute>,
            scheduler_commands: &'a HashMap<u64, SchedulerCommandRecord>,
            tablet_alerts: &'a HashMap<u64, TabletOperatorAlert>,
            next_scheduler_command_id: u64,
        }

        let inner = self.inner.read();
        serde_json::to_vec(&Snapshot {
            nodes: &inner.nodes,
            routes: &inner.routes,
            scheduler_commands: &inner.scheduler_commands,
            tablet_alerts: &inner.tablet_alerts,
            next_scheduler_command_id: inner.next_scheduler_command_id,
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
}

fn default_next_scheduler_command_id() -> u64 {
    1
}
