use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use parking_lot::RwLock;

use crate::command::MetaCommand;
use crate::model::{NodeState, SchedulerCommand, ServerNode, TabletRoute};

#[derive(Default)]
struct StateMachineCore {
    nodes: HashMap<u64, ServerNode>,
    routes: HashMap<u64, TabletRoute>,
    command_queues: HashMap<u64, VecDeque<SchedulerCommand>>,
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

    pub fn enqueue_scheduler_command(&self, node_id: u64, command: SchedulerCommand) {
        let mut inner = self.inner.write();
        inner
            .command_queues
            .entry(node_id)
            .or_default()
            .push_back(command);
    }

    pub fn drain_scheduler_commands(&self, node_id: u64) -> Vec<SchedulerCommand> {
        let mut inner = self.inner.write();
        inner
            .command_queues
            .entry(node_id)
            .or_default()
            .drain(..)
            .collect()
    }

    pub fn snapshot_bytes(&self) -> Result<Vec<u8>> {
        #[derive(serde::Serialize)]
        struct Snapshot<'a> {
            nodes: &'a HashMap<u64, ServerNode>,
            routes: &'a HashMap<u64, TabletRoute>,
        }

        let inner = self.inner.read();
        serde_json::to_vec(&Snapshot {
            nodes: &inner.nodes,
            routes: &inner.routes,
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
