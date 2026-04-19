use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use tonic::{Request, Response, Status};

use crate::command::MetaCommand;
use crate::model::{
    NodeState, SchedulerCommand as DomainSchedulerCommand, SchedulerCommandKind, ServerNode,
    ServerRole,
};
use crate::pb::meta_service_server::MetaService;
use crate::pb::raft_transport_server::RaftTransport;
use crate::pb::{
    GetTabletRouteRequest, GetTabletRouteResponse, HeartbeatRequest, HeartbeatResponse, NodeInfo,
    RaftMessage, RaftMessageAck, SchedulerCommand,
};
use crate::raft_node::RaftNodeFacade;
use crate::state_machine::MetaStateMachine;

#[derive(Clone)]
pub struct MetaServiceState {
    pub raft: Arc<RaftNodeFacade>,
    pub state_machine: MetaStateMachine,
    pub advertised_leader: Option<String>,
}

#[derive(Clone)]
pub struct MetaGrpcService {
    state: MetaServiceState,
}

impl MetaGrpcService {
    pub fn new(state: MetaServiceState) -> Self {
        Self { state }
    }

    async fn propose_command(&self, command: MetaCommand) -> Result<()> {
        let bytes = serde_json::to_vec(&command).context("encode meta command")?;
        self.state.raft.propose(bytes).await
    }

    fn state_machine(&self) -> &MetaStateMachine {
        &self.state.state_machine
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

        let command = if self.state_machine().node(server_node.node_id).is_some() {
            MetaCommand::UpdateNodeHeartbeat {
                node_id: server_node.node_id,
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
            .drain_scheduler_commands(server_node.node_id)
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
                route: Some(crate::pb::TabletRoute {
                    tablet_id: route.tablet_id,
                    leader_cache_node_id: route.leader_cache_node_id,
                    wal_replica_node_ids: route.wal_replica_node_ids,
                    epoch: route.epoch,
                }),
                leader_hint: self.state.advertised_leader.clone().unwrap_or_default(),
            }))
        } else {
            Ok(Response::new(GetTabletRouteResponse {
                found: false,
                route: None,
                leader_hint: self.state.advertised_leader.clone().unwrap_or_default(),
            }))
        }
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
