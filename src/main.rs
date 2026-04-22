use std::collections::HashMap;
use std::io::IsTerminal;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use meta_server::config::RuntimeConfig;
use meta_server::http_gateway;
use meta_server::meta_service::{MetaGrpcService, MetaServiceState};
use meta_server::network::RaftNetwork;
use meta_server::pb::meta_service_server::MetaServiceServer;
use meta_server::pb::raft_transport_server::RaftTransportServer;
use meta_server::raft_node::RaftNodeFacade;
use meta_server::reconciler::MetaReconciler;
use meta_server::state_machine::MetaStateMachine;
use meta_server::storage::MetaStorage;
use meta_server::wal_pb::wal_manager_service_server::WalManagerServiceServer;
use tokio::sync::Mutex;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(std::io::stderr().is_terminal())
        .init();

    let config = RuntimeConfig::load()?;
    tracing::info!(
        node_id = config.node_id,
        listen_addr = %config.listen_addr,
        storage_dir = %config.storage_dir,
        group_id = config.group_id.as_deref().unwrap_or("default"),
        config_path = config
            .config_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "env-only".to_string()),
        "starting meta server"
    );

    let state_machine = MetaStateMachine::default();
    let storage = MetaStorage::open(&config.storage_dir)?;

    let (network, rx) = RaftNetwork::new(config.peers.clone());
    tokio::spawn(network.clone().run_sender(rx));
    let raft = if config.voter_ids.len() == 1 && config.voter_ids[0] == config.node_id {
        RaftNodeFacade::bootstrap_single_node(
            config.node_id,
            storage,
            state_machine.clone(),
            Some(network),
        )
        .await?
    } else {
        RaftNodeFacade::bootstrap(
            config.node_id,
            config.voter_ids.clone(),
            storage,
            state_machine.clone(),
            Some(network),
        )
        .await?
    };
    raft.spawn_tick_task(Duration::from_millis(100));

    let service = MetaGrpcService::new(MetaServiceState {
        raft: Arc::new(raft),
        state_machine: state_machine.clone(),
        advertised_leader: config.advertised_leader.clone(),
        meta_node_id: config.node_id,
        meta_voters: config.voter_ids.clone(),
        meta_peers: {
            let mut peers = config.peers.clone();
            peers.insert(
                config.node_id,
                config
                    .advertised_leader
                    .clone()
                    .unwrap_or_else(|| config.listen_addr.clone()),
            );
            peers.into_iter().collect::<HashMap<_, _>>()
        },
        wal_mutation_lock: Arc::new(Mutex::new(())),
    });
    let reconciler = MetaReconciler::new(
        service.state().raft.clone(),
        state_machine,
        Duration::from_secs(config.heartbeat_timeout_secs),
    );
    reconciler.spawn(Duration::from_millis(config.reconcile_interval_ms));

    if let Some(http_listen_addr) = &config.http_listen_addr {
        let http_addr = http_listen_addr.parse()?;
        let shared = Arc::new(service.state().clone());
        tokio::spawn(async move {
            if let Err(error) = http_gateway::serve(shared, http_addr).await {
                tracing::error!(?error, "http gateway exited");
            }
        });
    }

    let addr = config.listen_addr.parse()?;
    Server::builder()
        .add_service(MetaServiceServer::new(service.clone()))
        .add_service(WalManagerServiceServer::new(service.clone()))
        .add_service(RaftTransportServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
