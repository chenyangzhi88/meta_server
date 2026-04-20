use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use raft::StateRole;
use tonic::{Request, Response, Status};

use crate::command::MetaCommand;
use crate::model::{
    NodeState, SchedulerCommand as DomainSchedulerCommand, SchedulerCommandKind, ServerNode,
    ServerRole,
};
use crate::pb::meta_service_server::MetaService;
use crate::pb::raft_transport_server::RaftTransport;
use crate::pb::{
    BeginTabletMigrationRequest, CompleteTabletMigrationRequest, GetMetaClusterStatusRequest,
    GetMetaClusterStatusResponse, GetNodeTabletsRequest, GetNodeTabletsResponse,
    GetTabletRouteRequest, GetTabletRouteResponse, HeartbeatRequest, HeartbeatResponse,
    ListNodesRequest, ListNodesResponse, ListSchedulerCommandsRequest,
    ListSchedulerCommandsResponse, ListTabletAlertsRequest, ListTabletAlertsResponse,
    ListTabletRoutesRequest, ListTabletRoutesResponse, MetaNodeStatus, MutationResponse, NodeInfo,
    NodeTabletInfo, RaftMessage, RaftMessageAck, SchedulerCommand,
    SchedulerCommandRecord as PbSchedulerCommandRecord, TabletAlert, TabletRoute as PbTabletRoute,
};
use crate::raft_node::RaftNodeFacade;
use crate::scheduler::TabletScheduler;
use crate::state_machine::MetaStateMachine;

#[derive(Clone)]
pub struct MetaServiceState {
    pub raft: Arc<RaftNodeFacade>,
    pub state_machine: MetaStateMachine,
    pub advertised_leader: Option<String>,
    pub meta_node_id: u64,
    pub meta_voters: Vec<u64>,
    pub meta_peers: HashMap<u64, String>,
}

#[derive(Clone)]
pub struct MetaGrpcService {
    state: MetaServiceState,
}

impl MetaGrpcService {
    pub fn new(state: MetaServiceState) -> Self {
        Self { state }
    }

    pub fn state(&self) -> &MetaServiceState {
        &self.state
    }

    async fn propose_command(&self, command: MetaCommand) -> Result<()> {
        let bytes = serde_json::to_vec(&command).context("encode meta command")?;
        self.state.raft.propose(bytes).await
    }

    fn state_machine(&self) -> &MetaStateMachine {
        &self.state.state_machine
    }

    fn scheduler(&self) -> TabletScheduler {
        TabletScheduler::new(self.state.state_machine.clone())
    }

    async fn ensure_leader(&self) -> Result<(), Status> {
        if self.state.raft.state_role().await == StateRole::Leader {
            return Ok(());
        }
        Err(Status::failed_precondition(format!(
            "request must be sent to current leader{}",
            self.state
                .advertised_leader
                .as_deref()
                .map(|leader| format!(" at {leader}"))
                .unwrap_or_default()
        )))
    }

    fn mutation_response(&self) -> Response<MutationResponse> {
        Response::new(MutationResponse {
            accepted: true,
            leader_hint: self.state.advertised_leader.clone().unwrap_or_default(),
        })
    }

    fn map_node_info(node: NodeInfo) -> Result<ServerNode> {
        Ok(ServerNode {
            node_id: node.node_id,
            address: node.address,
            role: match node.role.as_str() {
                "CacheServer" => ServerRole::CacheServer,
                "WalServer" => ServerRole::WalServer,
                other => anyhow::bail!("unsupported server role {other}"),
            },
            state: match node.state.as_str() {
                "Up" => NodeState::Up,
                "Down" => NodeState::Down,
                "Tombstone" => NodeState::Tombstone,
                other => anyhow::bail!("unsupported node state {other}"),
            },
            last_heartbeat_ts: node.last_heartbeat_ts,
            cpu_usage: node.cpu_usage,
            memory_usage: node.memory_usage,
            disk_free_gb: node.disk_free_gb,
        })
    }

    fn map_scheduler_command(command: DomainSchedulerCommand) -> SchedulerCommand {
        SchedulerCommand {
            command_id: command.command_id,
            kind: match command.kind {
                SchedulerCommandKind::DropTablet => "DropTablet".to_string(),
                SchedulerCommandKind::LoadTablet => "LoadTablet".to_string(),
            },
            tablet_id: command.tablet_id,
            epoch: command.epoch,
            wal_replica_node_ids: command.wal_replica_node_ids,
            detail: command.detail,
        }
    }

    fn map_scheduler_command_record(
        record: crate::model::SchedulerCommandRecord,
    ) -> PbSchedulerCommandRecord {
        PbSchedulerCommandRecord {
            command_id: record.command_id,
            target_node_id: record.target_node_id,
            status: match record.status {
                crate::model::SchedulerCommandStatus::Pending => "Pending".to_string(),
                crate::model::SchedulerCommandStatus::Acked => "Acked".to_string(),
                crate::model::SchedulerCommandStatus::Completed => "Completed".to_string(),
                crate::model::SchedulerCommandStatus::Failed => "Failed".to_string(),
            },
            created_at_ts: record.created_at_ts,
            acked_at_ts: record.acked_at_ts.unwrap_or_default(),
            completed_at_ts: record.completed_at_ts.unwrap_or_default(),
            failed_at_ts: record.failed_at_ts.unwrap_or_default(),
            failure_reason: record.failure_reason.unwrap_or_default(),
            command: Some(Self::map_scheduler_command(record.command)),
        }
    }

    fn leader_hint(&self) -> String {
        self.state.advertised_leader.clone().unwrap_or_default()
    }

    async fn local_meta_node_status(&self) -> MetaNodeStatus {
        let node_id = self.state.meta_node_id;
        let address = self
            .state
            .meta_peers
            .get(&node_id)
            .cloned()
            .or_else(|| self.state.advertised_leader.clone())
            .unwrap_or_default();
        let leader_id = self.state.raft.leader_id().await;
        let raft_role = match self.state.raft.state_role().await {
            StateRole::Leader => "Leader",
            StateRole::Follower => "Follower",
            StateRole::Candidate => "Candidate",
            StateRole::PreCandidate => "PreCandidate",
        }
        .to_string();

        MetaNodeStatus {
            node_id,
            address,
            raft_role,
            leader_id,
            observed_at_ts: now_ts(),
            is_voter: self.state.meta_voters.contains(&node_id),
        }
    }
}

#[tonic::async_trait]
impl MetaService for MetaGrpcService {
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let request = request.into_inner();
        let node = request
            .node
            .ok_or_else(|| Status::invalid_argument("missing node info"))?;
        let server_node = Self::map_node_info(node).map_err(internal_status)?;
        let node_id = server_node.node_id;

        for command_id in request.acked_command_ids {
            self.apply_scheduler_ack(node_id, command_id)
                .await
                .map_err(internal_status)?;
        }
        for command_id in request.completed_command_ids {
            self.apply_scheduler_completion(node_id, command_id)
                .await
                .map_err(internal_status)?;
        }
        for failed in request.failed_commands {
            self.apply_scheduler_failure(node_id, failed.command_id, failed.reason)
                .await
                .map_err(internal_status)?;
        }

        let command = if self.state_machine().node(node_id).is_some() {
            MetaCommand::UpdateNodeHeartbeat {
                node_id,
                last_heartbeat_ts: now_ts(),
            }
        } else {
            MetaCommand::UpsertNode(ServerNode {
                last_heartbeat_ts: now_ts(),
                ..server_node.clone()
            })
        };
        self.propose_command(command)
            .await
            .map_err(internal_status)?;

        let commands = self
            .state_machine()
            .pending_scheduler_commands(node_id)
            .into_iter()
            .map(Self::map_scheduler_command)
            .collect();
        Ok(Response::new(HeartbeatResponse {
            accepted: true,
            leader_hint: self.state.advertised_leader.clone().unwrap_or_default(),
            commands,
        }))
    }

    async fn get_tablet_route(
        &self,
        request: Request<GetTabletRouteRequest>,
    ) -> Result<Response<GetTabletRouteResponse>, Status> {
        self.state
            .raft
            .read_index()
            .await
            .map_err(internal_status)?;

        let tablet_id = request.into_inner().tablet_id;
        if let Some(route) = self.state_machine().route(tablet_id) {
            Ok(Response::new(GetTabletRouteResponse {
                found: true,
                route: Some(PbTabletRoute {
                    tablet_id: route.tablet_id,
                    leader_cache_node_id: route.leader_cache_node_id,
                    wal_replica_node_ids: route.wal_replica_node_ids,
                    epoch: route.epoch,
                }),
                leader_hint: self.leader_hint(),
            }))
        } else {
            Ok(Response::new(GetTabletRouteResponse {
                found: false,
                route: None,
                leader_hint: self.leader_hint(),
            }))
        }
    }

    async fn list_nodes(
        &self,
        _request: Request<ListNodesRequest>,
    ) -> Result<Response<ListNodesResponse>, Status> {
        self.state
            .raft
            .read_index()
            .await
            .map_err(internal_status)?;
        let mut nodes = self
            .state_machine()
            .nodes()
            .into_iter()
            .map(|node| NodeInfo {
                node_id: node.node_id,
                address: node.address,
                role: match node.role {
                    ServerRole::CacheServer => "CacheServer".to_string(),
                    ServerRole::WalServer => "WalServer".to_string(),
                },
                state: match node.state {
                    NodeState::Up => "Up".to_string(),
                    NodeState::Down => "Down".to_string(),
                    NodeState::Tombstone => "Tombstone".to_string(),
                },
                last_heartbeat_ts: node.last_heartbeat_ts,
                cpu_usage: node.cpu_usage,
                memory_usage: node.memory_usage,
                disk_free_gb: node.disk_free_gb,
            })
            .collect::<Vec<_>>();
        nodes.sort_by_key(|node| node.node_id);
        Ok(Response::new(ListNodesResponse {
            nodes,
            leader_hint: self.leader_hint(),
        }))
    }

    async fn list_tablet_routes(
        &self,
        _request: Request<ListTabletRoutesRequest>,
    ) -> Result<Response<ListTabletRoutesResponse>, Status> {
        self.state
            .raft
            .read_index()
            .await
            .map_err(internal_status)?;
        let mut routes = self
            .state_machine()
            .routes()
            .into_iter()
            .map(|route| PbTabletRoute {
                tablet_id: route.tablet_id,
                leader_cache_node_id: route.leader_cache_node_id,
                wal_replica_node_ids: route.wal_replica_node_ids,
                epoch: route.epoch,
            })
            .collect::<Vec<_>>();
        routes.sort_by_key(|route| route.tablet_id);
        Ok(Response::new(ListTabletRoutesResponse {
            routes,
            leader_hint: self.leader_hint(),
        }))
    }

    async fn get_node_tablets(
        &self,
        request: Request<GetNodeTabletsRequest>,
    ) -> Result<Response<GetNodeTabletsResponse>, Status> {
        self.state
            .raft
            .read_index()
            .await
            .map_err(internal_status)?;
        let node_id = request.into_inner().node_id;
        let mut tablets = Vec::new();

        for route in self.state_machine().routes() {
            if route.leader_cache_node_id == node_id {
                tablets.push(NodeTabletInfo {
                    tablet_id: route.tablet_id,
                    relation: "LeaderCache".to_string(),
                    epoch: route.epoch,
                });
            }
            if route.wal_replica_node_ids.contains(&node_id) {
                tablets.push(NodeTabletInfo {
                    tablet_id: route.tablet_id,
                    relation: "WalReplica".to_string(),
                    epoch: route.epoch,
                });
            }
        }

        for record in self.state_machine().scheduler_commands() {
            if record.target_node_id == node_id {
                tablets.push(NodeTabletInfo {
                    tablet_id: record.command.tablet_id,
                    relation: format!(
                        "Scheduled{}",
                        match record.command.kind {
                            SchedulerCommandKind::DropTablet => "DropTablet",
                            SchedulerCommandKind::LoadTablet => "LoadTablet",
                        }
                    ),
                    epoch: record.command.epoch,
                });
            }
        }

        tablets.sort_by_key(|tablet| (tablet.tablet_id, tablet.relation.clone()));
        Ok(Response::new(GetNodeTabletsResponse {
            node_id,
            tablets,
            leader_hint: self.leader_hint(),
        }))
    }

    async fn list_scheduler_commands(
        &self,
        _request: Request<ListSchedulerCommandsRequest>,
    ) -> Result<Response<ListSchedulerCommandsResponse>, Status> {
        self.state
            .raft
            .read_index()
            .await
            .map_err(internal_status)?;
        let commands = self
            .state_machine()
            .scheduler_commands()
            .into_iter()
            .map(Self::map_scheduler_command_record)
            .collect();
        Ok(Response::new(ListSchedulerCommandsResponse {
            commands,
            leader_hint: self.leader_hint(),
        }))
    }

    async fn list_tablet_alerts(
        &self,
        _request: Request<ListTabletAlertsRequest>,
    ) -> Result<Response<ListTabletAlertsResponse>, Status> {
        self.state
            .raft
            .read_index()
            .await
            .map_err(internal_status)?;
        let alerts = self
            .state_machine()
            .tablet_alerts()
            .into_iter()
            .map(|alert| TabletAlert {
                tablet_id: alert.tablet_id,
                reason: alert.reason,
                created_at_ts: alert.created_at_ts,
            })
            .collect();
        Ok(Response::new(ListTabletAlertsResponse {
            alerts,
            leader_hint: self.leader_hint(),
        }))
    }

    async fn get_meta_cluster_status(
        &self,
        _request: Request<GetMetaClusterStatusRequest>,
    ) -> Result<Response<GetMetaClusterStatusResponse>, Status> {
        Ok(Response::new(GetMetaClusterStatusResponse {
            local_node: Some(self.local_meta_node_status().await),
            voters: self.state.meta_voters.clone(),
            leader_hint: self.leader_hint(),
        }))
    }

    async fn begin_tablet_migration(
        &self,
        request: Request<BeginTabletMigrationRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.ensure_leader().await?;

        let request = request.into_inner();
        let commands = self
            .scheduler()
            .begin_migration(
                request.tablet_id,
                request.source_node_id,
                request.target_node_id,
            )
            .map_err(internal_status)?;
        for command in commands {
            self.propose_command(command)
                .await
                .map_err(internal_status)?;
        }
        Ok(self.mutation_response())
    }

    async fn complete_tablet_migration(
        &self,
        request: Request<CompleteTabletMigrationRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        self.ensure_leader().await?;

        let request = request.into_inner();
        let command = self
            .scheduler()
            .complete_migration(request.tablet_id, request.target_node_id)
            .map_err(internal_status)?;
        self.propose_command(command)
            .await
            .map_err(internal_status)?;
        Ok(self.mutation_response())
    }
}

impl MetaGrpcService {
    async fn apply_scheduler_ack(&self, reporter_node_id: u64, command_id: u64) -> Result<()> {
        let Some(record) = self.state_machine().scheduler_command(command_id) else {
            return Ok(());
        };
        if record.target_node_id != reporter_node_id {
            anyhow::bail!(
                "scheduler command {} belongs to node {}, not reporter {}",
                command_id,
                record.target_node_id,
                reporter_node_id
            );
        }
        self.propose_command(MetaCommand::AckSchedulerCommand {
            command_id,
            acked_at_ts: now_ts(),
        })
        .await
    }

    async fn apply_scheduler_completion(
        &self,
        reporter_node_id: u64,
        command_id: u64,
    ) -> Result<()> {
        let Some(record) = self.state_machine().scheduler_command(command_id) else {
            return Ok(());
        };
        if record.target_node_id != reporter_node_id {
            anyhow::bail!(
                "scheduler command {} belongs to node {}, not reporter {}",
                command_id,
                record.target_node_id,
                reporter_node_id
            );
        }

        match record.command.kind {
            SchedulerCommandKind::DropTablet => {}
            SchedulerCommandKind::LoadTablet => {
                let route = self
                    .state_machine()
                    .route(record.command.tablet_id)
                    .with_context(|| {
                        format!(
                            "tablet route {} not found for completion",
                            record.command.tablet_id
                        )
                    })?;
                if route.epoch < record.command.epoch {
                    anyhow::bail!(
                        "tablet {} route epoch {} is behind completed command epoch {}",
                        record.command.tablet_id,
                        route.epoch,
                        record.command.epoch
                    );
                }

                if route.leader_cache_node_id == 0 && route.epoch == record.command.epoch {
                    let command = self
                        .scheduler()
                        .complete_migration(record.command.tablet_id, reporter_node_id)?;
                    self.propose_command(command).await?;
                } else if route.leader_cache_node_id != reporter_node_id {
                    anyhow::bail!(
                        "tablet {} route leader {} does not match completed load target {}",
                        record.command.tablet_id,
                        route.leader_cache_node_id,
                        reporter_node_id
                    );
                }
            }
        }

        self.propose_command(MetaCommand::CompleteSchedulerCommand {
            command_id,
            completed_at_ts: now_ts(),
        })
        .await
    }

    async fn apply_scheduler_failure(
        &self,
        reporter_node_id: u64,
        command_id: u64,
        reason: String,
    ) -> Result<()> {
        let Some(record) = self.state_machine().scheduler_command(command_id) else {
            return Ok(());
        };
        if record.target_node_id != reporter_node_id {
            anyhow::bail!(
                "scheduler command {} belongs to node {}, not reporter {}",
                command_id,
                record.target_node_id,
                reporter_node_id
            );
        }
        if reason.trim().is_empty() {
            anyhow::bail!("scheduler command {} failure reason is empty", command_id);
        }
        self.propose_command(MetaCommand::FailSchedulerCommand {
            command_id,
            failed_at_ts: now_ts(),
            reason,
        })
        .await
    }
}

#[tonic::async_trait]
impl RaftTransport for MetaGrpcService {
    async fn send_raft_message(
        &self,
        request: Request<RaftMessage>,
    ) -> Result<Response<RaftMessageAck>, Status> {
        self.state
            .raft
            .step(request.into_inner().data)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RaftMessageAck { accepted: true }))
    }
}

fn internal_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
