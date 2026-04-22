use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use raft::StateRole;

use crate::command::MetaCommand;
use crate::model::{NodeState, SchedulerCommandKind, ServerRole};
use crate::raft_node::RaftNodeFacade;
use crate::scheduler::TabletScheduler;
use crate::state_machine::MetaStateMachine;

#[derive(Clone)]
pub struct MetaReconciler {
    raft: Arc<RaftNodeFacade>,
    state_machine: MetaStateMachine,
    scheduler: TabletScheduler,
    heartbeat_timeout: Duration,
}

impl MetaReconciler {
    pub fn new(
        raft: Arc<RaftNodeFacade>,
        state_machine: MetaStateMachine,
        heartbeat_timeout: Duration,
    ) -> Self {
        Self {
            raft,
            scheduler: TabletScheduler::new(state_machine.clone()),
            state_machine,
            heartbeat_timeout,
        }
    }

    pub fn spawn(self, interval: Duration) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if let Err(error) = self.reconcile_once().await {
                    tracing::warn!(?error, "meta reconcile failed");
                }
            }
        });
    }

    pub async fn reconcile_once(&self) -> Result<()> {
        if self.raft.state_role().await != StateRole::Leader && self.raft.voter_count().await > 1 {
            return Ok(());
        }

        let now = now_ts();
        let timeout_secs = self.heartbeat_timeout.as_secs();
        for node in self.state_machine.nodes() {
            if node.role != ServerRole::CacheServer || node.state != NodeState::Up {
                continue;
            }
            if now.saturating_sub(node.last_heartbeat_ts) <= timeout_secs {
                continue;
            }

            tracing::info!(node_id = node.node_id, "cache node heartbeat timed out");
            self.propose_command(MetaCommand::MarkNodeState {
                node_id: node.node_id,
                state: NodeState::Down,
            })
            .await?;

            let target_node_id = self
                .pick_failover_target(node.node_id)
                .with_context(|| format!("pick failover target for node {}", node.node_id))?;

            for route in self.state_machine.routes() {
                if route.leader_cache_node_id != node.node_id {
                    continue;
                }
                let commands = self
                    .scheduler
                    .failover_from_down_node(route.tablet_id, node.node_id, target_node_id)
                    .with_context(|| {
                        format!(
                            "plan failover for tablet {} from node {} to {}",
                            route.tablet_id, node.node_id, target_node_id
                        )
                    })?;
                for command in commands {
                    self.propose_command(command).await?;
                }
            }
        }

        for failed in self.state_machine.failed_scheduler_commands() {
            if failed.command.kind != SchedulerCommandKind::LoadTablet {
                continue;
            }
            let Some(route) = self.state_machine.route(failed.command.tablet_id) else {
                continue;
            };
            if route.leader_cache_node_id != 0 {
                continue;
            }
            if self
                .state_machine
                .tablet_alert(failed.command.tablet_id)
                .is_some()
            {
                continue;
            }

            let reason = failed.failure_reason.clone().unwrap_or_else(|| {
                format!(
                    "load command {} for tablet {} failed",
                    failed.command_id, failed.command.tablet_id
                )
            });
            self.propose_command(MetaCommand::UpsertTabletOperatorAlert {
                tablet_id: failed.command.tablet_id,
                reason: format!(
                    "manual intervention required after failed load on node {}: {}",
                    failed.target_node_id, reason
                ),
                created_at_ts: now_ts(),
            })
            .await?;
        }
        Ok(())
    }

    async fn propose_command(&self, command: MetaCommand) -> Result<()> {
        if self.raft.voter_count().await == 1 {
            return self.state_machine.apply(command);
        }
        let payload = serde_json::to_vec(&command).context("encode reconciler meta command")?;
        self.raft.propose(payload).await
    }

    fn pick_failover_target(&self, source_node_id: u64) -> Result<u64> {
        self.state_machine
            .nodes()
            .into_iter()
            .find(|node| {
                node.node_id != source_node_id
                    && node.role == ServerRole::CacheServer
                    && node.state == NodeState::Up
            })
            .map(|node| node.node_id)
            .context("no available cache failover target")
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
    use std::sync::Arc;
    use std::time::Duration;

    use crate::command::MetaCommand;
    use crate::model::{
        NodeState, SchedulerCommandKind, SchedulerCommandSpec, ServerNode, ServerRole, TabletRoute,
    };
    use crate::raft_node::RaftNodeFacade;
    use crate::state_machine::MetaStateMachine;
    use crate::storage::MetaStorage;

    use super::MetaReconciler;

    #[tokio::test]
    async fn reconcile_marks_stale_cache_node_down_and_fails_over_routes() {
        let temp = tempfile::tempdir().unwrap();
        let storage = MetaStorage::open(temp.path()).unwrap();
        let state_machine = MetaStateMachine::default();
        let raft = Arc::new(
            RaftNodeFacade::bootstrap_single_node(1, storage, state_machine.clone(), None)
                .await
                .unwrap(),
        );
        raft.spawn_tick_task(Duration::from_millis(100));
        raft.campaign().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        for command in [
            MetaCommand::UpsertNode(ServerNode {
                node_id: 11,
                address: "127.0.0.1:8101".to_string(),
                role: ServerRole::CacheServer,
                state: NodeState::Up,
                last_heartbeat_ts: 1,
                cpu_usage: 0.1,
                memory_usage: 0.2,
                disk_free_gb: 10.0,
                wal_server_info: None,
            }),
            MetaCommand::UpsertNode(ServerNode {
                node_id: 12,
                address: "127.0.0.1:8102".to_string(),
                role: ServerRole::CacheServer,
                state: NodeState::Up,
                last_heartbeat_ts: super::now_ts(),
                cpu_usage: 0.1,
                memory_usage: 0.2,
                disk_free_gb: 10.0,
                wal_server_info: None,
            }),
            MetaCommand::UpsertTabletRoute(TabletRoute {
                tablet_id: 100,
                leader_cache_node_id: 11,
                wal_replica_node_ids: vec![201, 202, 203],
                epoch: 7,
            }),
        ] {
            state_machine.apply(command).unwrap();
        }

        let reconciler = MetaReconciler::new(raft, state_machine.clone(), Duration::from_secs(5));
        reconciler.reconcile_once().await.unwrap();

        let down_node = state_machine.node(11).unwrap();
        assert_eq!(down_node.state, NodeState::Down);

        let route = state_machine.route(100).unwrap();
        assert_eq!(route.leader_cache_node_id, 12);
        assert_eq!(route.epoch, 8);

        let commands = state_machine.pending_scheduler_commands(12);
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].tablet_id, 100);
        assert_eq!(commands[0].epoch, 8);
    }

    #[tokio::test]
    async fn reconcile_marks_failed_load_for_manual_intervention() {
        let temp = tempfile::tempdir().unwrap();
        let storage = MetaStorage::open(temp.path()).unwrap();
        let state_machine = MetaStateMachine::default();
        let raft = Arc::new(
            RaftNodeFacade::bootstrap_single_node(1, storage, state_machine.clone(), None)
                .await
                .unwrap(),
        );
        raft.spawn_tick_task(Duration::from_millis(100));
        raft.campaign().await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        for command in [
            MetaCommand::UpsertTabletRoute(TabletRoute {
                tablet_id: 200,
                leader_cache_node_id: 0,
                wal_replica_node_ids: vec![301, 302, 303],
                epoch: 4,
            }),
            MetaCommand::CreateSchedulerCommand {
                target_node_id: 12,
                spec: SchedulerCommandSpec {
                    kind: SchedulerCommandKind::LoadTablet,
                    tablet_id: 200,
                    epoch: 4,
                    wal_replica_node_ids: vec![301, 302, 303],
                    detail: "load failed tablet".to_string(),
                },
                created_at_ts: super::now_ts(),
            },
            MetaCommand::FailSchedulerCommand {
                command_id: 1,
                failed_at_ts: super::now_ts(),
                reason: "wal replica unavailable".to_string(),
            },
        ] {
            state_machine.apply(command).unwrap();
        }

        let reconciler = MetaReconciler::new(raft, state_machine.clone(), Duration::from_secs(5));
        reconciler.reconcile_once().await.unwrap();

        let alert = state_machine.tablet_alert(200).unwrap();
        assert!(alert.reason.contains("manual intervention required"));
        assert!(alert.reason.contains("wal replica unavailable"));
    }
}
