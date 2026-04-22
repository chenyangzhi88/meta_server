use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use meta_server::command::MetaCommand;
use meta_server::meta_service::{MetaGrpcService, MetaServiceState};
use meta_server::model::TabletRoute;
use meta_server::network::RaftNetwork;
use meta_server::pb::meta_service_client::MetaServiceClient;
use meta_server::pb::meta_service_server::MetaService;
use meta_server::pb::meta_service_server::MetaServiceServer;
use meta_server::pb::raft_transport_server::RaftTransportServer;
use meta_server::pb::{
    BeginTabletMigrationRequest, FailedCommand, GetMetaClusterStatusRequest, GetNodeTabletsRequest,
    GetTabletRouteRequest, HeartbeatRequest, ListNodesRequest, ListSchedulerCommandsRequest,
    ListTabletAlertsRequest, ListTabletRoutesRequest, NodeInfo,
};
use meta_server::raft_node::RaftNodeFacade;
use meta_server::scheduler::TabletScheduler;
use meta_server::state_machine::MetaStateMachine;
use meta_server::storage::MetaStorage;
use meta_server::wal_pb::wal_manager_service_server::WalManagerServiceServer;
use tempfile::tempdir;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::transport::Server;

fn cache_node(node_id: u64, address: &str) -> NodeInfo {
    NodeInfo {
        node_id,
        address: address.to_string(),
        role: "CacheServer".to_string(),
        state: "Up".to_string(),
        last_heartbeat_ts: 0,
        cpu_usage: 0.2,
        memory_usage: 0.3,
        disk_free_gb: 120.0,
        wal_server_info: None,
    }
}

struct TestNode {
    raft: Arc<RaftNodeFacade>,
    state_machine: MetaStateMachine,
    advertised_addr: String,
    _tempdir: tempfile::TempDir,
    shutdown_tx: Option<oneshot::Sender<()>>,
    server_handle: JoinHandle<()>,
}

impl TestNode {
    async fn client(&self) -> MetaServiceClient<tonic::transport::Channel> {
        MetaServiceClient::connect(format!("http://{}", self.advertised_addr))
            .await
            .unwrap()
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        self.server_handle.abort();
    }
}

async fn start_single_node() -> TestNode {
    let temp = tempdir().unwrap();
    let storage = MetaStorage::open(temp.path()).unwrap();
    let state_machine = MetaStateMachine::default();
    let raft = Arc::new(
        RaftNodeFacade::bootstrap_single_node(1, storage, state_machine.clone(), None)
            .await
            .unwrap(),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = MetaGrpcService::new(MetaServiceState {
        raft: raft.clone(),
        state_machine: state_machine.clone(),
        advertised_leader: Some(addr.to_string()),
        meta_node_id: 1,
        meta_voters: vec![1],
        meta_peers: [(1_u64, addr.to_string())].into_iter().collect(),
        wal_mutation_lock: Arc::new(Mutex::new(())),
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(MetaServiceServer::new(service.clone()))
            .add_service(WalManagerServiceServer::new(service.clone()))
            .add_service(RaftTransportServer::new(service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    TestNode {
        raft,
        state_machine,
        advertised_addr: addr.to_string(),
        _tempdir: temp,
        shutdown_tx: Some(shutdown_tx),
        server_handle,
    }
}

async fn start_three_node_cluster() -> Vec<TestNode> {
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(TcpListener::bind("127.0.0.1:0").await.unwrap());
    }

    let addresses: Vec<SocketAddr> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap())
        .collect();
    let voters = vec![1, 2, 3];

    let mut nodes = Vec::new();
    for node_id in voters.iter().copied() {
        let temp = tempdir().unwrap();
        let storage = MetaStorage::open(temp.path()).unwrap();
        let state_machine = MetaStateMachine::default();

        let peers = voters
            .iter()
            .copied()
            .filter(|peer_id| *peer_id != node_id)
            .map(|peer_id| (peer_id, addresses[(peer_id - 1) as usize].to_string()))
            .collect::<HashMap<_, _>>();
        let (network, rx) = RaftNetwork::new(peers);
        tokio::spawn(network.clone().run_sender(rx));

        let raft = Arc::new(
            RaftNodeFacade::bootstrap(
                node_id,
                voters.clone(),
                storage,
                state_machine.clone(),
                Some(network),
            )
            .await
            .unwrap(),
        );
        raft.spawn_tick_task(Duration::from_millis(100));

        let advertised_addr = addresses[(node_id - 1) as usize].to_string();
        let service = MetaGrpcService::new(MetaServiceState {
            raft: raft.clone(),
            state_machine: state_machine.clone(),
            advertised_leader: Some(advertised_addr.clone()),
            meta_node_id: node_id,
            meta_voters: voters.clone(),
            meta_peers: voters
                .iter()
                .copied()
                .map(|peer_id| (peer_id, addresses[(peer_id - 1) as usize].to_string()))
                .collect(),
            wal_mutation_lock: Arc::new(Mutex::new(())),
        });

        let listener = listeners.remove(0);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_handle = tokio::spawn(async move {
            Server::builder()
                .add_service(MetaServiceServer::new(service.clone()))
                .add_service(WalManagerServiceServer::new(service.clone()))
                .add_service(RaftTransportServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        });

        nodes.push(TestNode {
            raft,
            state_machine,
            advertised_addr,
            _tempdir: temp,
            shutdown_tx: Some(shutdown_tx),
            server_handle,
        });
    }

    tokio::time::sleep(Duration::from_millis(300)).await;
    nodes[0].raft.campaign().await.unwrap();
    wait_for_leader(&nodes, 1).await;
    nodes
}

async fn wait_for_leader(nodes: &[TestNode], leader_id: u64) {
    for _ in 0..50 {
        let mut ready = true;
        for node in nodes {
            if node.raft.leader_id().await != leader_id {
                ready = false;
                break;
            }
        }
        if ready {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("leader {leader_id} was not observed on all nodes");
}

async fn wait_for_route(nodes: &[TestNode], tablet_id: u64, leader_cache_node_id: u64, epoch: u64) {
    for _ in 0..50 {
        let converged = nodes.iter().all(|node| {
            node.state_machine.route(tablet_id).is_some_and(|route| {
                route.leader_cache_node_id == leader_cache_node_id && route.epoch == epoch
            })
        });
        if converged {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("tablet {tablet_id} did not converge to leader={leader_cache_node_id}, epoch={epoch}");
}

#[tokio::test]
async fn heartbeat_route_and_migration_flow_work() {
    let node = start_single_node().await;
    let service = MetaGrpcService::new(MetaServiceState {
        raft: node.raft.clone(),
        state_machine: node.state_machine.clone(),
        advertised_leader: Some(node.advertised_addr.clone()),
        meta_node_id: 1,
        meta_voters: vec![1],
        meta_peers: [(1_u64, node.advertised_addr.clone())]
            .into_iter()
            .collect(),
        wal_mutation_lock: Arc::new(Mutex::new(())),
    });

    let heartbeat = service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(11, "127.0.0.1:8101")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(heartbeat.accepted);
    assert!(heartbeat.commands.is_empty());
    assert!(node.state_machine.node(11).is_some());

    service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap();
    assert!(node.state_machine.node(12).is_some());

    node.raft
        .propose(
            serde_json::to_vec(&MetaCommand::UpsertTabletRoute(TabletRoute {
                tablet_id: 100,
                leader_cache_node_id: 11,
                wal_replica_node_ids: vec![201, 202, 203],
                epoch: 1,
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    let route = service
        .get_tablet_route(Request::new(GetTabletRouteRequest { tablet_id: 100 }))
        .await
        .unwrap()
        .into_inner();
    assert!(route.found);
    let route = route.route.unwrap();
    assert_eq!(route.leader_cache_node_id, 11);
    assert_eq!(route.epoch, 1);

    let mut client = node.client().await;
    client
        .begin_tablet_migration(BeginTabletMigrationRequest {
            tablet_id: 100,
            source_node_id: 11,
            target_node_id: 12,
        })
        .await
        .unwrap();

    let route = service
        .get_tablet_route(Request::new(GetTabletRouteRequest { tablet_id: 100 }))
        .await
        .unwrap()
        .into_inner()
        .route
        .unwrap();
    assert_eq!(route.leader_cache_node_id, 0);
    assert_eq!(route.epoch, 2);

    let source_commands = service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(11, "127.0.0.1:8101")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap()
        .into_inner()
        .commands;
    assert_eq!(source_commands.len(), 1);
    assert!(source_commands[0].command_id > 0);
    assert_eq!(source_commands[0].kind, "DropTablet");
    assert_eq!(source_commands[0].tablet_id, 100);

    let target_commands = service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap()
        .into_inner()
        .commands;
    assert_eq!(target_commands.len(), 1);
    let target_command_id = target_commands[0].command_id;
    assert_eq!(target_commands[0].kind, "LoadTablet");
    assert_eq!(target_commands[0].epoch, 2);

    let replayed_commands = service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap()
        .into_inner()
        .commands;
    assert_eq!(replayed_commands.len(), 1);
    assert_eq!(replayed_commands[0].command_id, target_command_id);

    let completed_commands = service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![target_command_id],
            completed_command_ids: vec![target_command_id],
            failed_commands: vec![],
        }))
        .await
        .unwrap()
        .into_inner()
        .commands;
    assert!(completed_commands.is_empty());

    let route = service
        .get_tablet_route(Request::new(GetTabletRouteRequest { tablet_id: 100 }))
        .await
        .unwrap()
        .into_inner()
        .route
        .unwrap();
    assert_eq!(route.leader_cache_node_id, 12);
    assert_eq!(route.epoch, 3);

    let completed_record = node
        .state_machine
        .scheduler_command(target_command_id)
        .unwrap();
    assert_eq!(
        completed_record.status,
        meta_server::model::SchedulerCommandStatus::Completed
    );
}

#[tokio::test]
async fn meta_cluster_status_rpc_reports_local_raft_view() {
    let temp = tempdir().unwrap();
    let storage = MetaStorage::open(temp.path()).unwrap();
    let state_machine = MetaStateMachine::default();
    let raft = Arc::new(
        RaftNodeFacade::bootstrap_single_node(1, storage, state_machine.clone(), None)
            .await
            .unwrap(),
    );
    let service = MetaGrpcService::new(MetaServiceState {
        raft,
        state_machine,
        advertised_leader: Some("127.0.0.1:7001".to_string()),
        meta_node_id: 1,
        meta_voters: vec![1],
        meta_peers: [(1_u64, "127.0.0.1:7001".to_string())]
            .into_iter()
            .collect(),
        wal_mutation_lock: Arc::new(Mutex::new(())),
    });

    let response = service
        .get_meta_cluster_status(Request::new(GetMetaClusterStatusRequest {}))
        .await
        .unwrap()
        .into_inner();
    let local = response.local_node.unwrap();

    assert_eq!(local.node_id, 1);
    assert_eq!(local.address, "127.0.0.1:7001");
    assert_eq!(local.raft_role, "Leader");
    assert_eq!(local.leader_id, 1);
    assert!(local.observed_at_ts > 0);
    assert!(local.is_voter);
    assert_eq!(response.voters, vec![1]);
}

#[tokio::test]
async fn three_node_cluster_replicates_routes_over_grpc() {
    let nodes = start_three_node_cluster().await;
    let mut leader_client = nodes[0].client().await;

    leader_client
        .heartbeat(HeartbeatRequest {
            node: Some(cache_node(31, "127.0.0.1:8301")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        })
        .await
        .unwrap();

    nodes[0]
        .raft
        .propose(
            serde_json::to_vec(&MetaCommand::UpsertTabletRoute(TabletRoute {
                tablet_id: 200,
                leader_cache_node_id: 31,
                wal_replica_node_ids: vec![401, 402, 403],
                epoch: 7,
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    wait_for_route(&nodes, 200, 31, 7).await;

    let response = leader_client
        .get_tablet_route(GetTabletRouteRequest { tablet_id: 200 })
        .await
        .unwrap()
        .into_inner();
    assert!(response.found);
    let route = response.route.unwrap();
    assert_eq!(route.tablet_id, 200);
    assert_eq!(route.leader_cache_node_id, 31);
    assert_eq!(route.epoch, 7);
    assert_eq!(route.wal_replica_node_ids, vec![401, 402, 403]);

    for node in &nodes {
        let route = node.state_machine.route(200).unwrap();
        assert_eq!(route.leader_cache_node_id, 31);
        assert_eq!(route.epoch, 7);
        assert_eq!(route.wal_replica_node_ids, vec![401, 402, 403]);
    }
}

#[tokio::test]
async fn wal_replay_restores_scheduler_command_ids() {
    let temp = tempdir().unwrap();

    {
        let storage = MetaStorage::open(temp.path()).unwrap();
        let state_machine = MetaStateMachine::default();
        let raft = Arc::new(
            RaftNodeFacade::bootstrap_single_node(1, storage, state_machine.clone(), None)
                .await
                .unwrap(),
        );

        for command in [
            MetaCommand::UpsertNode(meta_server::model::ServerNode {
                node_id: 11,
                address: "127.0.0.1:8101".to_string(),
                role: meta_server::model::ServerRole::CacheServer,
                state: meta_server::model::NodeState::Up,
                last_heartbeat_ts: 1,
                cpu_usage: 0.1,
                memory_usage: 0.2,
                disk_free_gb: 10.0,
                wal_server_info: None,
            }),
            MetaCommand::UpsertNode(meta_server::model::ServerNode {
                node_id: 12,
                address: "127.0.0.1:8102".to_string(),
                role: meta_server::model::ServerRole::CacheServer,
                state: meta_server::model::NodeState::Up,
                last_heartbeat_ts: 1,
                cpu_usage: 0.1,
                memory_usage: 0.2,
                disk_free_gb: 10.0,
                wal_server_info: None,
            }),
            MetaCommand::UpsertTabletRoute(TabletRoute {
                tablet_id: 100,
                leader_cache_node_id: 11,
                wal_replica_node_ids: vec![201, 202, 203],
                epoch: 1,
            }),
        ] {
            raft.propose(serde_json::to_vec(&command).unwrap())
                .await
                .unwrap();
        }

        let scheduler = TabletScheduler::new(state_machine.clone());
        for command in scheduler.begin_migration(100, 11, 12).unwrap() {
            raft.propose(serde_json::to_vec(&command).unwrap())
                .await
                .unwrap();
        }

        let node11_commands = state_machine.pending_scheduler_commands(11);
        let node12_commands = state_machine.pending_scheduler_commands(12);
        assert_eq!(node11_commands[0].command_id, 1);
        assert_eq!(node12_commands[0].command_id, 2);
        assert_eq!(state_machine.next_scheduler_command_id(), 3);
    }

    let storage = MetaStorage::open(temp.path()).unwrap();
    let restored_state_machine = MetaStateMachine::default();
    let restored_raft = Arc::new(
        RaftNodeFacade::bootstrap_single_node(1, storage, restored_state_machine.clone(), None)
            .await
            .unwrap(),
    );

    assert_eq!(restored_state_machine.next_scheduler_command_id(), 3);
    let restored_node11_commands = restored_state_machine.pending_scheduler_commands(11);
    let restored_node12_commands = restored_state_machine.pending_scheduler_commands(12);
    assert_eq!(restored_node11_commands[0].command_id, 1);
    assert_eq!(restored_node12_commands[0].command_id, 2);

    restored_raft
        .propose(
            serde_json::to_vec(&MetaCommand::UpsertTabletRoute(TabletRoute {
                tablet_id: 101,
                leader_cache_node_id: 12,
                wal_replica_node_ids: vec![301, 302, 303],
                epoch: 7,
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    let restored_scheduler = TabletScheduler::new(restored_state_machine.clone());
    for command in restored_scheduler.begin_migration(101, 12, 11).unwrap() {
        restored_raft
            .propose(serde_json::to_vec(&command).unwrap())
            .await
            .unwrap();
    }

    let new_node12_commands = restored_state_machine.pending_scheduler_commands(12);
    let new_node11_commands = restored_state_machine.pending_scheduler_commands(11);
    assert!(
        new_node12_commands
            .iter()
            .any(|command| command.command_id == 4)
    );
    assert!(
        new_node11_commands
            .iter()
            .any(|command| command.command_id == 3)
    );
    assert_eq!(restored_state_machine.next_scheduler_command_id(), 5);
}

#[tokio::test]
async fn heartbeat_failure_marks_command_failed_without_route_change() {
    let node = start_single_node().await;
    let service = MetaGrpcService::new(MetaServiceState {
        raft: node.raft.clone(),
        state_machine: node.state_machine.clone(),
        advertised_leader: Some(node.advertised_addr.clone()),
        meta_node_id: 1,
        meta_voters: vec![1],
        meta_peers: [(1_u64, node.advertised_addr.clone())]
            .into_iter()
            .collect(),
        wal_mutation_lock: Arc::new(Mutex::new(())),
    });

    service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(11, "127.0.0.1:8101")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap();
    service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap();

    node.raft
        .propose(
            serde_json::to_vec(&MetaCommand::UpsertTabletRoute(TabletRoute {
                tablet_id: 300,
                leader_cache_node_id: 11,
                wal_replica_node_ids: vec![501, 502, 503],
                epoch: 1,
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    let mut client = node.client().await;
    client
        .begin_tablet_migration(BeginTabletMigrationRequest {
            tablet_id: 300,
            source_node_id: 11,
            target_node_id: 12,
        })
        .await
        .unwrap();

    let target_commands = service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap()
        .into_inner()
        .commands;
    assert_eq!(target_commands.len(), 1);
    let target_command_id = target_commands[0].command_id;

    let response = service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![FailedCommand {
                command_id: target_command_id,
                reason: "load failed: wal replica unavailable".to_string(),
            }],
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(response.commands.is_empty());

    let record = node
        .state_machine
        .scheduler_command(target_command_id)
        .unwrap();
    assert_eq!(
        record.status,
        meta_server::model::SchedulerCommandStatus::Failed
    );
    assert_eq!(
        record.failure_reason.as_deref(),
        Some("load failed: wal replica unavailable")
    );

    let route = node.state_machine.route(300).unwrap();
    assert_eq!(route.leader_cache_node_id, 0);
    assert_eq!(route.epoch, 2);
}

#[tokio::test]
async fn failed_load_triggers_operator_alert_after_reconcile() {
    let temp = tempdir().unwrap();
    let storage = MetaStorage::open(temp.path()).unwrap();
    let state_machine = MetaStateMachine::default();
    let raft = Arc::new(
        RaftNodeFacade::bootstrap_single_node(1, storage, state_machine.clone(), None)
            .await
            .unwrap(),
    );
    let reconciler = meta_server::reconciler::MetaReconciler::new(
        raft.clone(),
        state_machine.clone(),
        Duration::from_secs(5),
    );

    let service = MetaGrpcService::new(MetaServiceState {
        raft: raft.clone(),
        state_machine: state_machine.clone(),
        advertised_leader: Some("127.0.0.1:7001".to_string()),
        meta_node_id: 1,
        meta_voters: vec![1],
        meta_peers: [(1_u64, "127.0.0.1:7001".to_string())]
            .into_iter()
            .collect(),
        wal_mutation_lock: Arc::new(Mutex::new(())),
    });

    service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(11, "127.0.0.1:8101")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap();
    service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap();

    raft.propose(
        serde_json::to_vec(&MetaCommand::UpsertTabletRoute(TabletRoute {
            tablet_id: 400,
            leader_cache_node_id: 11,
            wal_replica_node_ids: vec![601, 602, 603],
            epoch: 1,
        }))
        .unwrap(),
    )
    .await
    .unwrap();

    let scheduler = TabletScheduler::new(state_machine.clone());
    for command in scheduler.begin_migration(400, 11, 12).unwrap() {
        raft.propose(serde_json::to_vec(&command).unwrap())
            .await
            .unwrap();
    }

    let target_commands = service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap()
        .into_inner()
        .commands;
    let target_command_id = target_commands[0].command_id;

    service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![FailedCommand {
                command_id: target_command_id,
                reason: "disk full on target cache node".to_string(),
            }],
        }))
        .await
        .unwrap();

    reconciler.reconcile_once().await.unwrap();

    let alert = state_machine.tablet_alert(400).unwrap();
    assert!(alert.reason.contains("manual intervention required"));
    assert!(alert.reason.contains("disk full on target cache node"));
}

#[tokio::test]
async fn observability_queries_match_state_machine_views() {
    let node = start_single_node().await;
    let service = MetaGrpcService::new(MetaServiceState {
        raft: node.raft.clone(),
        state_machine: node.state_machine.clone(),
        advertised_leader: Some(node.advertised_addr.clone()),
        meta_node_id: 1,
        meta_voters: vec![1],
        meta_peers: [(1_u64, node.advertised_addr.clone())]
            .into_iter()
            .collect(),
        wal_mutation_lock: Arc::new(Mutex::new(())),
    });
    let reconciler = meta_server::reconciler::MetaReconciler::new(
        node.raft.clone(),
        node.state_machine.clone(),
        Duration::from_secs(5),
    );

    service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(11, "127.0.0.1:8101")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap();
    service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap();
    service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(NodeInfo {
                node_id: 21,
                address: "127.0.0.1:8201".to_string(),
                role: "WalServer".to_string(),
                state: "Up".to_string(),
                last_heartbeat_ts: 0,
                cpu_usage: 0.1,
                memory_usage: 0.2,
                disk_free_gb: 200.0,
                wal_server_info: None,
            }),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap();

    node.raft
        .propose(
            serde_json::to_vec(&MetaCommand::UpsertTabletRoute(TabletRoute {
                tablet_id: 500,
                leader_cache_node_id: 11,
                wal_replica_node_ids: vec![21, 22, 23],
                epoch: 5,
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    let mut client = node.client().await;
    client
        .begin_tablet_migration(BeginTabletMigrationRequest {
            tablet_id: 500,
            source_node_id: 11,
            target_node_id: 12,
        })
        .await
        .unwrap();

    let target_commands = service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        }))
        .await
        .unwrap()
        .into_inner()
        .commands;
    let load_command_id = target_commands[0].command_id;

    service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![FailedCommand {
                command_id: load_command_id,
                reason: "network stalled while loading tablet".to_string(),
            }],
        }))
        .await
        .unwrap();
    reconciler.reconcile_once().await.unwrap();

    let nodes_response = client
        .list_nodes(ListNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(nodes_response.nodes.len(), 3);
    assert_eq!(nodes_response.nodes[0].node_id, 11);
    assert_eq!(nodes_response.nodes[1].node_id, 12);
    assert_eq!(nodes_response.nodes[2].node_id, 21);

    let routes_response = client
        .list_tablet_routes(ListTabletRoutesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(routes_response.routes.len(), 1);
    assert_eq!(routes_response.routes[0].tablet_id, 500);
    assert_eq!(routes_response.routes[0].leader_cache_node_id, 0);
    assert_eq!(routes_response.routes[0].epoch, 6);

    let node11_tablets = client
        .get_node_tablets(GetNodeTabletsRequest { node_id: 11 })
        .await
        .unwrap()
        .into_inner();
    assert!(
        node11_tablets
            .tablets
            .iter()
            .any(|tablet| tablet.tablet_id == 500 && tablet.relation == "ScheduledDropTablet")
    );

    let node12_tablets = client
        .get_node_tablets(GetNodeTabletsRequest { node_id: 12 })
        .await
        .unwrap()
        .into_inner();
    assert!(
        node12_tablets
            .tablets
            .iter()
            .any(|tablet| tablet.tablet_id == 500 && tablet.relation == "ScheduledLoadTablet")
    );

    let node21_tablets = client
        .get_node_tablets(GetNodeTabletsRequest { node_id: 21 })
        .await
        .unwrap()
        .into_inner();
    assert!(
        node21_tablets
            .tablets
            .iter()
            .any(|tablet| tablet.tablet_id == 500 && tablet.relation == "WalReplica")
    );

    let commands_response = client
        .list_scheduler_commands(ListSchedulerCommandsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(commands_response.commands.len(), 2);
    assert!(commands_response.commands.iter().any(|command| {
        command
            .command
            .as_ref()
            .is_some_and(|cmd| cmd.kind == "DropTablet")
    }));
    assert!(commands_response.commands.iter().any(|command| {
        command.command_id == load_command_id
            && command.status == "Failed"
            && command.failure_reason == "network stalled while loading tablet"
    }));

    let alerts_response = client
        .list_tablet_alerts(ListTabletAlertsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(alerts_response.alerts.len(), 1);
    assert_eq!(alerts_response.alerts[0].tablet_id, 500);
    assert!(
        alerts_response.alerts[0]
            .reason
            .contains("manual intervention required")
    );
}

#[tokio::test]
async fn multi_node_observability_queries_match_replicated_state() {
    let nodes = start_three_node_cluster().await;
    let mut leader_client = nodes[0].client().await;

    leader_client
        .heartbeat(HeartbeatRequest {
            node: Some(cache_node(31, "127.0.0.1:8301")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        })
        .await
        .unwrap();
    leader_client
        .heartbeat(HeartbeatRequest {
            node: Some(cache_node(32, "127.0.0.1:8302")),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        })
        .await
        .unwrap();
    leader_client
        .heartbeat(HeartbeatRequest {
            node: Some(NodeInfo {
                node_id: 41,
                address: "127.0.0.1:8401".to_string(),
                role: "WalServer".to_string(),
                state: "Up".to_string(),
                last_heartbeat_ts: 0,
                cpu_usage: 0.1,
                memory_usage: 0.2,
                disk_free_gb: 320.0,
                wal_server_info: None,
            }),
            acked_command_ids: vec![],
            completed_command_ids: vec![],
            failed_commands: vec![],
        })
        .await
        .unwrap();

    nodes[0]
        .raft
        .propose(
            serde_json::to_vec(&MetaCommand::UpsertTabletRoute(TabletRoute {
                tablet_id: 900,
                leader_cache_node_id: 31,
                wal_replica_node_ids: vec![41, 42, 43],
                epoch: 9,
            }))
            .unwrap(),
        )
        .await
        .unwrap();

    leader_client
        .begin_tablet_migration(BeginTabletMigrationRequest {
            tablet_id: 900,
            source_node_id: 31,
            target_node_id: 32,
        })
        .await
        .unwrap();

    wait_for_route(&nodes, 900, 0, 10).await;

    nodes[0]
        .raft
        .propose(
            serde_json::to_vec(&MetaCommand::UpsertTabletOperatorAlert {
                tablet_id: 900,
                reason: "manual intervention required for tablet 900".to_string(),
                created_at_ts: 123,
            })
            .unwrap(),
        )
        .await
        .unwrap();

    let nodes_response = leader_client
        .list_nodes(ListNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(nodes_response.nodes.len(), 3);
    for replica in &nodes {
        let mut state_nodes = replica.state_machine.nodes();
        state_nodes.sort_by_key(|node| node.node_id);
        assert_eq!(state_nodes.len(), nodes_response.nodes.len());
        for (expected, actual) in state_nodes.iter().zip(nodes_response.nodes.iter()) {
            assert_eq!(expected.node_id, actual.node_id);
            assert_eq!(expected.address, actual.address);
        }
    }

    let routes_response = leader_client
        .list_tablet_routes(ListTabletRoutesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(routes_response.routes.len(), 1);
    assert_eq!(routes_response.routes[0].tablet_id, 900);
    assert_eq!(routes_response.routes[0].leader_cache_node_id, 0);
    assert_eq!(routes_response.routes[0].epoch, 10);
    for replica in &nodes {
        let route = replica.state_machine.route(900).unwrap();
        assert_eq!(route.leader_cache_node_id, 0);
        assert_eq!(route.epoch, 10);
    }

    let commands_response = leader_client
        .list_scheduler_commands(ListSchedulerCommandsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(commands_response.commands.len(), 2);
    for replica in &nodes {
        let commands = replica.state_machine.scheduler_commands();
        assert_eq!(commands.len(), commands_response.commands.len());
    }

    let alerts_response = leader_client
        .list_tablet_alerts(ListTabletAlertsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(alerts_response.alerts.len(), 1);
    assert_eq!(alerts_response.alerts[0].tablet_id, 900);
    for replica in &nodes {
        let alert = replica.state_machine.tablet_alert(900).unwrap();
        assert_eq!(alert.reason, alerts_response.alerts[0].reason);
    }
}
