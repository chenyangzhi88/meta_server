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
use meta_server::pb::{GetTabletRouteRequest, HeartbeatRequest, NodeInfo};
use meta_server::raft_node::RaftNodeFacade;
use meta_server::scheduler::TabletScheduler;
use meta_server::state_machine::MetaStateMachine;
use meta_server::storage::MetaStorage;
use tempfile::tempdir;
use tokio::net::TcpListener;
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
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(MetaServiceServer::new(service.clone()))
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
        });

        let listener = listeners.remove(0);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_handle = tokio::spawn(async move {
            Server::builder()
                .add_service(MetaServiceServer::new(service.clone()))
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
    });

    let heartbeat = service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(11, "127.0.0.1:8101")),
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

    let scheduler = TabletScheduler::new(node.state_machine.clone());
    for command in scheduler.begin_migration(100, 11, 12).unwrap() {
        node.raft
            .propose(serde_json::to_vec(&command).unwrap())
            .await
            .unwrap();
    }

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
        }))
        .await
        .unwrap()
        .into_inner()
        .commands;
    assert_eq!(source_commands.len(), 1);
    assert_eq!(source_commands[0].kind, "DropTablet");
    assert_eq!(source_commands[0].tablet_id, 100);

    let target_commands = service
        .heartbeat(Request::new(HeartbeatRequest {
            node: Some(cache_node(12, "127.0.0.1:8102")),
        }))
        .await
        .unwrap()
        .into_inner()
        .commands;
    assert_eq!(target_commands.len(), 1);
    assert_eq!(target_commands[0].kind, "LoadTablet");
    assert_eq!(target_commands[0].epoch, 2);

    let finish = scheduler.complete_migration(100, 12).unwrap();
    node.raft
        .propose(serde_json::to_vec(&finish).unwrap())
        .await
        .unwrap();

    let route = service
        .get_tablet_route(Request::new(GetTabletRouteRequest { tablet_id: 100 }))
        .await
        .unwrap()
        .into_inner()
        .route
        .unwrap();
    assert_eq!(route.leader_cache_node_id, 12);
    assert_eq!(route.epoch, 3);
}

#[tokio::test]
async fn three_node_cluster_replicates_routes_over_grpc() {
    let nodes = start_three_node_cluster().await;
    let mut leader_client = nodes[0].client().await;

    leader_client
        .heartbeat(HeartbeatRequest {
            node: Some(cache_node(31, "127.0.0.1:8301")),
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
