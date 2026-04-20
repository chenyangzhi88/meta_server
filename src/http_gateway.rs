use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router, response::Html};
use serde::Serialize;
use tonic::transport::Endpoint;

use crate::command::MetaCommand;
use crate::meta_service::MetaServiceState;
use crate::model::{
    NodeState, SchedulerCommandKind, SchedulerCommandSpec, SchedulerCommandStatus, ServerRole,
};
use crate::pb::GetMetaClusterStatusRequest;
use crate::pb::meta_service_client::MetaServiceClient;

#[derive(Clone)]
pub struct HttpGatewayState {
    shared: Arc<MetaServiceState>,
}

impl HttpGatewayState {
    pub fn new(shared: Arc<MetaServiceState>) -> Self {
        Self { shared }
    }
}

pub fn router(shared: Arc<MetaServiceState>) -> Router {
    let state = HttpGatewayState::new(shared);
    Router::new()
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/api/meta/cluster", get(get_meta_cluster))
        .route("/api/nodes", get(list_nodes))
        .route("/api/tablets/routes", get(list_tablet_routes))
        .route("/api/nodes/:node_id/tablets", get(get_node_tablets))
        .route("/api/scheduler/commands", get(list_scheduler_commands))
        .route(
            "/api/scheduler/commands/:command_id/retry",
            post(retry_scheduler_command),
        )
        .route("/api/tablets/alerts", get(list_tablet_alerts))
        .route(
            "/api/tablets/:tablet_id/alerts/clear",
            post(clear_tablet_alert),
        )
        .with_state(state)
}

pub async fn serve(shared: Arc<MetaServiceState>, addr: std::net::SocketAddr) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(shared)).await?;
    Ok(())
}

#[derive(Serialize)]
struct ApiResponse<T> {
    leader_hint: String,
    data: T,
}

#[derive(Serialize)]
struct ApiError {
    error: String,
    leader_hint: String,
}

#[derive(Serialize)]
struct ActionResult {
    message: String,
}

#[derive(Serialize)]
struct NodeView {
    node_id: u64,
    address: String,
    role: String,
    state: String,
    last_heartbeat_ts: u64,
    cpu_usage: f64,
    memory_usage: f64,
    disk_free_gb: f64,
}

#[derive(Serialize)]
struct TabletRouteView {
    tablet_id: u64,
    leader_cache_node_id: u64,
    wal_replica_node_ids: Vec<u64>,
    epoch: u64,
}

#[derive(Serialize)]
struct NodeTabletInfoView {
    tablet_id: u64,
    relation: String,
    epoch: u64,
}

#[derive(Serialize)]
struct SchedulerCommandView {
    command_id: u64,
    target_node_id: u64,
    status: String,
    created_at_ts: u64,
    acked_at_ts: Option<u64>,
    completed_at_ts: Option<u64>,
    failed_at_ts: Option<u64>,
    failure_reason: Option<String>,
    command: SchedulerCommandPayload,
}

#[derive(Serialize)]
struct SchedulerCommandPayload {
    command_id: u64,
    kind: String,
    tablet_id: u64,
    epoch: u64,
    wal_replica_node_ids: Vec<u64>,
    detail: String,
}

#[derive(Serialize)]
struct TabletAlertView {
    tablet_id: u64,
    reason: String,
    created_at_ts: u64,
}

#[derive(Serialize)]
struct MetaClusterNodeView {
    node_id: u64,
    address: String,
    raft_role: String,
    leader_id: u64,
    health: String,
    observed_at_ts: u64,
    status_source: String,
    is_local: bool,
}

async fn healthz() -> &'static str {
    "ok"
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/observability.html"))
}

async fn get_meta_cluster(
    State(state): State<HttpGatewayState>,
) -> Result<Json<ApiResponse<Vec<MetaClusterNodeView>>>, (axum::http::StatusCode, Json<ApiError>)> {
    let mut nodes = Vec::new();
    let mut peer_ids = state.shared.meta_voters.clone();
    peer_ids.sort_unstable();

    for node_id in peer_ids {
        let configured_address = state
            .shared
            .meta_peers
            .get(&node_id)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let is_local = node_id == state.shared.meta_node_id;
        let reported = if is_local {
            Some(local_meta_cluster_node_view(&state).await)
        } else {
            fetch_remote_meta_cluster_node_view(node_id, &configured_address).await
        };
        nodes.push(reported.unwrap_or_else(|| MetaClusterNodeView {
            node_id,
            address: configured_address,
            raft_role: "Unknown".to_string(),
            leader_id: 0,
            health: "Unreachable".to_string(),
            observed_at_ts: 0,
            status_source: "unreachable".to_string(),
            is_local,
        }));
    }

    Ok(Json(ApiResponse {
        leader_hint: leader_hint(&state),
        data: nodes,
    }))
}

async fn list_nodes(
    State(state): State<HttpGatewayState>,
) -> Result<Json<ApiResponse<Vec<NodeView>>>, (axum::http::StatusCode, Json<ApiError>)> {
    ensure_linearized(&state).await?;
    let mut nodes = state
        .shared
        .state_machine
        .nodes()
        .into_iter()
        .map(|node| NodeView {
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
    Ok(Json(ApiResponse {
        leader_hint: leader_hint(&state),
        data: nodes,
    }))
}

async fn list_tablet_routes(
    State(state): State<HttpGatewayState>,
) -> Result<Json<ApiResponse<Vec<TabletRouteView>>>, (axum::http::StatusCode, Json<ApiError>)> {
    ensure_linearized(&state).await?;
    let mut routes = state
        .shared
        .state_machine
        .routes()
        .into_iter()
        .map(|route| TabletRouteView {
            tablet_id: route.tablet_id,
            leader_cache_node_id: route.leader_cache_node_id,
            wal_replica_node_ids: route.wal_replica_node_ids,
            epoch: route.epoch,
        })
        .collect::<Vec<_>>();
    routes.sort_by_key(|route| route.tablet_id);
    Ok(Json(ApiResponse {
        leader_hint: leader_hint(&state),
        data: routes,
    }))
}

async fn get_node_tablets(
    Path(node_id): Path<u64>,
    State(state): State<HttpGatewayState>,
) -> Result<Json<ApiResponse<Vec<NodeTabletInfoView>>>, (axum::http::StatusCode, Json<ApiError>)> {
    ensure_linearized(&state).await?;
    let mut tablets = Vec::new();
    for route in state.shared.state_machine.routes() {
        if route.leader_cache_node_id == node_id {
            tablets.push(NodeTabletInfoView {
                tablet_id: route.tablet_id,
                relation: "LeaderCache".to_string(),
                epoch: route.epoch,
            });
        }
        if route.wal_replica_node_ids.contains(&node_id) {
            tablets.push(NodeTabletInfoView {
                tablet_id: route.tablet_id,
                relation: "WalReplica".to_string(),
                epoch: route.epoch,
            });
        }
    }
    for record in state.shared.state_machine.scheduler_commands() {
        if record.target_node_id == node_id {
            tablets.push(NodeTabletInfoView {
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
    Ok(Json(ApiResponse {
        leader_hint: leader_hint(&state),
        data: tablets,
    }))
}

async fn list_scheduler_commands(
    State(state): State<HttpGatewayState>,
) -> Result<Json<ApiResponse<Vec<SchedulerCommandView>>>, (axum::http::StatusCode, Json<ApiError>)>
{
    ensure_linearized(&state).await?;
    let commands = state
        .shared
        .state_machine
        .scheduler_commands()
        .into_iter()
        .map(|record| SchedulerCommandView {
            command_id: record.command_id,
            target_node_id: record.target_node_id,
            status: match record.status {
                SchedulerCommandStatus::Pending => "Pending".to_string(),
                SchedulerCommandStatus::Acked => "Acked".to_string(),
                SchedulerCommandStatus::Completed => "Completed".to_string(),
                SchedulerCommandStatus::Failed => "Failed".to_string(),
            },
            created_at_ts: record.created_at_ts,
            acked_at_ts: record.acked_at_ts,
            completed_at_ts: record.completed_at_ts,
            failed_at_ts: record.failed_at_ts,
            failure_reason: record.failure_reason,
            command: SchedulerCommandPayload {
                command_id: record.command.command_id,
                kind: match record.command.kind {
                    SchedulerCommandKind::DropTablet => "DropTablet".to_string(),
                    SchedulerCommandKind::LoadTablet => "LoadTablet".to_string(),
                },
                tablet_id: record.command.tablet_id,
                epoch: record.command.epoch,
                wal_replica_node_ids: record.command.wal_replica_node_ids,
                detail: record.command.detail,
            },
        })
        .collect::<Vec<_>>();
    Ok(Json(ApiResponse {
        leader_hint: leader_hint(&state),
        data: commands,
    }))
}

async fn list_tablet_alerts(
    State(state): State<HttpGatewayState>,
) -> Result<Json<ApiResponse<Vec<TabletAlertView>>>, (axum::http::StatusCode, Json<ApiError>)> {
    ensure_linearized(&state).await?;
    let alerts = state
        .shared
        .state_machine
        .tablet_alerts()
        .into_iter()
        .map(|alert| TabletAlertView {
            tablet_id: alert.tablet_id,
            reason: alert.reason,
            created_at_ts: alert.created_at_ts,
        })
        .collect::<Vec<_>>();
    Ok(Json(ApiResponse {
        leader_hint: leader_hint(&state),
        data: alerts,
    }))
}

async fn retry_scheduler_command(
    Path(command_id): Path<u64>,
    State(state): State<HttpGatewayState>,
) -> Result<Json<ApiResponse<ActionResult>>, (axum::http::StatusCode, Json<ApiError>)> {
    ensure_leader(&state).await?;
    ensure_linearized(&state).await?;

    let record = state
        .shared
        .state_machine
        .scheduler_command(command_id)
        .ok_or_else(|| {
            api_error(
                &state,
                axum::http::StatusCode::NOT_FOUND,
                "scheduler command not found",
            )
        })?;
    if record.status != SchedulerCommandStatus::Failed {
        return Err(api_error(
            &state,
            axum::http::StatusCode::CONFLICT,
            "only failed scheduler commands can be retried",
        ));
    }
    if record.command.kind != SchedulerCommandKind::LoadTablet {
        return Err(api_error(
            &state,
            axum::http::StatusCode::CONFLICT,
            "only failed LoadTablet commands support retry",
        ));
    }
    let Some(target_node) = state.shared.state_machine.node(record.target_node_id) else {
        return Err(api_error(
            &state,
            axum::http::StatusCode::CONFLICT,
            "retry target node not found",
        ));
    };
    if target_node.state != NodeState::Up {
        return Err(api_error(
            &state,
            axum::http::StatusCode::CONFLICT,
            "retry target node is not Up",
        ));
    }

    let route = state
        .shared
        .state_machine
        .route(record.command.tablet_id)
        .ok_or_else(|| {
            api_error(
                &state,
                axum::http::StatusCode::CONFLICT,
                "tablet route not found",
            )
        })?;
    if route.leader_cache_node_id != 0 {
        return Err(api_error(
            &state,
            axum::http::StatusCode::CONFLICT,
            "tablet route has already converged; retry is no longer needed",
        ));
    }

    let retry = MetaCommand::CreateSchedulerCommand {
        target_node_id: record.target_node_id,
        spec: SchedulerCommandSpec {
            kind: SchedulerCommandKind::LoadTablet,
            tablet_id: record.command.tablet_id,
            epoch: record.command.epoch,
            wal_replica_node_ids: record.command.wal_replica_node_ids.clone(),
            detail: format!("manual retry of failed command {}", record.command_id),
        },
        created_at_ts: now_ts(),
    };
    let retry_bytes = serde_json::to_vec(&retry).map_err(|error| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError {
                error: error.to_string(),
                leader_hint: leader_hint(&state),
            }),
        )
    })?;
    state
        .shared
        .raft
        .propose(retry_bytes)
        .await
        .map_err(internal_api_error(&state))?;
    if state
        .shared
        .state_machine
        .tablet_alert(record.command.tablet_id)
        .is_some()
    {
        let clear = MetaCommand::ClearTabletOperatorAlert {
            tablet_id: record.command.tablet_id,
        };
        let clear_bytes = serde_json::to_vec(&clear).map_err(|error| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError {
                    error: error.to_string(),
                    leader_hint: leader_hint(&state),
                }),
            )
        })?;
        state
            .shared
            .raft
            .propose(clear_bytes)
            .await
            .map_err(internal_api_error(&state))?;
    }

    Ok(Json(ApiResponse {
        leader_hint: leader_hint(&state),
        data: ActionResult {
            message: format!(
                "retried failed load command {} for tablet {}",
                command_id, record.command.tablet_id
            ),
        },
    }))
}

async fn clear_tablet_alert(
    Path(tablet_id): Path<u64>,
    State(state): State<HttpGatewayState>,
) -> Result<Json<ApiResponse<ActionResult>>, (axum::http::StatusCode, Json<ApiError>)> {
    ensure_leader(&state).await?;
    ensure_linearized(&state).await?;

    if state.shared.state_machine.tablet_alert(tablet_id).is_none() {
        return Err(api_error(
            &state,
            axum::http::StatusCode::NOT_FOUND,
            "tablet alert not found",
        ));
    }

    let command = MetaCommand::ClearTabletOperatorAlert { tablet_id };
    let command_bytes = serde_json::to_vec(&command).map_err(|error| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError {
                error: error.to_string(),
                leader_hint: leader_hint(&state),
            }),
        )
    })?;
    state
        .shared
        .raft
        .propose(command_bytes)
        .await
        .map_err(internal_api_error(&state))?;

    Ok(Json(ApiResponse {
        leader_hint: leader_hint(&state),
        data: ActionResult {
            message: format!("cleared operator alert for tablet {}", tablet_id),
        },
    }))
}

async fn ensure_linearized(
    state: &HttpGatewayState,
) -> Result<(), (axum::http::StatusCode, Json<ApiError>)> {
    state.shared.raft.read_index().await.map_err(|error| {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError {
                error: error.to_string(),
                leader_hint: leader_hint(state),
            }),
        )
    })
}

async fn ensure_leader(
    state: &HttpGatewayState,
) -> Result<(), (axum::http::StatusCode, Json<ApiError>)> {
    let role = state.shared.raft.state_role().await;
    if role == raft::StateRole::Leader {
        return Ok(());
    }
    Err(api_error(
        state,
        axum::http::StatusCode::CONFLICT,
        "mutation must be sent to current leader",
    ))
}

fn leader_hint(state: &HttpGatewayState) -> String {
    state.shared.advertised_leader.clone().unwrap_or_default()
}

fn api_error(
    state: &HttpGatewayState,
    status: axum::http::StatusCode,
    error: &str,
) -> (axum::http::StatusCode, Json<ApiError>) {
    (
        status,
        Json(ApiError {
            error: error.to_string(),
            leader_hint: leader_hint(state),
        }),
    )
}

fn internal_api_error(
    state: &HttpGatewayState,
) -> impl FnOnce(anyhow::Error) -> (axum::http::StatusCode, Json<ApiError>) + '_ {
    move |error| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError {
                error: error.to_string(),
                leader_hint: leader_hint(state),
            }),
        )
    }
}

fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn local_meta_cluster_node_view(state: &HttpGatewayState) -> MetaClusterNodeView {
    let node_id = state.shared.meta_node_id;
    let address = state
        .shared
        .meta_peers
        .get(&node_id)
        .cloned()
        .or_else(|| state.shared.advertised_leader.clone())
        .unwrap_or_default();
    let leader_id = state.shared.raft.leader_id().await;
    let raft_role = match state.shared.raft.state_role().await {
        raft::StateRole::Leader => "Leader",
        raft::StateRole::Follower => "Follower",
        raft::StateRole::Candidate => "Candidate",
        raft::StateRole::PreCandidate => "PreCandidate",
    }
    .to_string();

    MetaClusterNodeView {
        node_id,
        address,
        raft_role,
        leader_id,
        health: "Reachable".to_string(),
        observed_at_ts: now_ts(),
        status_source: "local".to_string(),
        is_local: true,
    }
}

async fn fetch_remote_meta_cluster_node_view(
    node_id: u64,
    address: &str,
) -> Option<MetaClusterNodeView> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))
        .ok()?
        .connect_timeout(std::time::Duration::from_millis(400))
        .timeout(std::time::Duration::from_millis(800));
    let channel = endpoint.connect().await.ok()?;
    let mut client = MetaServiceClient::new(channel);
    let response = client
        .get_meta_cluster_status(GetMetaClusterStatusRequest {})
        .await
        .ok()?
        .into_inner();
    let local = response.local_node?;

    Some(MetaClusterNodeView {
        node_id: if local.node_id == 0 {
            node_id
        } else {
            local.node_id
        },
        address: if local.address.is_empty() {
            address.to_string()
        } else {
            local.address
        },
        raft_role: if local.raft_role.is_empty() {
            "Unknown".to_string()
        } else {
            local.raft_role
        },
        leader_id: local.leader_id,
        health: "Reachable".to_string(),
        observed_at_ts: local.observed_at_ts,
        status_source: "rpc".to_string(),
        is_local: false,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::Request;
    use serde_json::Value;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use tower::ServiceExt;

    use crate::meta_service::MetaGrpcService;
    use crate::meta_service::MetaServiceState;
    use crate::pb::meta_service_server::MetaServiceServer;
    use crate::pb::raft_transport_server::RaftTransportServer;
    use crate::raft_node::RaftNodeFacade;
    use crate::state_machine::MetaStateMachine;
    use crate::storage::MetaStorage;

    use super::router;

    #[tokio::test]
    async fn http_nodes_endpoint_returns_json() {
        let temp = tempfile::tempdir().unwrap();
        let storage = MetaStorage::open(temp.path()).unwrap();
        let state_machine = MetaStateMachine::default();
        let raft = Arc::new(
            RaftNodeFacade::bootstrap_single_node(1, storage, state_machine.clone(), None)
                .await
                .unwrap(),
        );

        let app = router(Arc::new(MetaServiceState {
            raft,
            state_machine,
            advertised_leader: Some("127.0.0.1:7001".to_string()),
            meta_node_id: 1,
            meta_voters: vec![1],
            meta_peers: [(1_u64, "127.0.0.1:7001".to_string())]
                .into_iter()
                .collect(),
        }));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/nodes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["leader_hint"], "127.0.0.1:7001");
        assert!(value["data"].is_array());
    }

    #[tokio::test]
    async fn http_meta_cluster_endpoint_returns_meta_nodes() {
        let temp = tempfile::tempdir().unwrap();
        let storage = MetaStorage::open(temp.path()).unwrap();
        let state_machine = MetaStateMachine::default();
        let raft = Arc::new(
            RaftNodeFacade::bootstrap_single_node(1, storage, state_machine.clone(), None)
                .await
                .unwrap(),
        );

        let app = router(Arc::new(MetaServiceState {
            raft,
            state_machine,
            advertised_leader: Some("127.0.0.1:7001".to_string()),
            meta_node_id: 1,
            meta_voters: vec![1, 2, 3],
            meta_peers: [
                (1_u64, "127.0.0.1:7001".to_string()),
                (2_u64, "127.0.0.1:7002".to_string()),
                (3_u64, "127.0.0.1:7003".to_string()),
            ]
            .into_iter()
            .collect(),
        }));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/meta/cluster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert!(value["data"].is_array());
        assert_eq!(value["data"][0]["node_id"], 1);
        assert_eq!(value["data"][0]["raft_role"], "Leader");
        assert_eq!(value["data"][0]["leader_id"], 1);
        assert_eq!(value["data"][0]["status_source"], "local");
    }

    #[tokio::test]
    async fn http_meta_cluster_endpoint_aggregates_remote_rpc_status() {
        let temp = tempfile::tempdir().unwrap();
        let storage = MetaStorage::open(temp.path()).unwrap();
        let state_machine = MetaStateMachine::default();
        let raft = Arc::new(
            RaftNodeFacade::bootstrap_single_node(1, storage, state_machine.clone(), None)
                .await
                .unwrap(),
        );

        let remote_temp = tempfile::tempdir().unwrap();
        let remote_storage = MetaStorage::open(remote_temp.path()).unwrap();
        let remote_state_machine = MetaStateMachine::default();
        let remote_raft = Arc::new(
            RaftNodeFacade::bootstrap_single_node(
                2,
                remote_storage,
                remote_state_machine.clone(),
                None,
            )
            .await
            .unwrap(),
        );
        let remote_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote_addr = remote_listener.local_addr().unwrap();
        let remote_service = MetaGrpcService::new(MetaServiceState {
            raft: remote_raft,
            state_machine: remote_state_machine,
            advertised_leader: Some(remote_addr.to_string()),
            meta_node_id: 2,
            meta_voters: vec![1, 2],
            meta_peers: [
                (1_u64, "127.0.0.1:7001".to_string()),
                (2_u64, remote_addr.to_string()),
            ]
            .into_iter()
            .collect(),
        });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let _remote_server = tokio::spawn(async move {
            Server::builder()
                .add_service(MetaServiceServer::new(remote_service.clone()))
                .add_service(RaftTransportServer::new(remote_service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(remote_listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        let app = router(Arc::new(MetaServiceState {
            raft,
            state_machine,
            advertised_leader: Some("127.0.0.1:7001".to_string()),
            meta_node_id: 1,
            meta_voters: vec![1, 2],
            meta_peers: [
                (1_u64, "127.0.0.1:7001".to_string()),
                (2_u64, remote_addr.to_string()),
            ]
            .into_iter()
            .collect(),
        }));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/meta/cluster")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["data"][1]["node_id"], 2);
        assert_eq!(value["data"][1]["health"], "Reachable");
        assert_eq!(value["data"][1]["status_source"], "rpc");
        assert!(value["data"][1]["observed_at_ts"].as_u64().unwrap() > 0);

        let _ = shutdown_tx.send(());
    }
}
