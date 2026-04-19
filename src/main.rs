use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use meta_server::meta_service::{MetaGrpcService, MetaServiceState};
use meta_server::network::RaftNetwork;
use meta_server::pb::meta_service_server::MetaServiceServer;
use meta_server::pb::raft_transport_server::RaftTransportServer;
use meta_server::raft_node::RaftNodeFacade;
use meta_server::state_machine::MetaStateMachine;
use meta_server::storage::MetaStorage;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();

    let node_id = std::env::var("META_NODE_ID")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    let listen_addr = std::env::var("META_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:7000".into());
    let storage_dir = std::env::var("META_STORAGE_DIR").unwrap_or_else(|_| "./meta_data".into());
    let advertised_leader = std::env::var("META_LEADER_HINT").ok();
    let voter_ids = std::env::var("META_VOTERS")
        .ok()
        .map(|value| parse_u64_csv(&value))
        .unwrap_or_else(|| vec![node_id]);
    let peers = std::env::var("META_PEERS")
        .ok()
        .map(|value| parse_peers(&value))
        .unwrap_or_default();

    let state_machine = MetaStateMachine::default();
    let storage = MetaStorage::open(storage_dir)?;

    let (network, rx) = RaftNetwork::new(peers);
    tokio::spawn(network.clone().run_sender(rx));
    let raft = if voter_ids.len() == 1 && voter_ids[0] == node_id {
        RaftNodeFacade::bootstrap_single_node(
            node_id,
            storage,
            state_machine.clone(),
            Some(network),
        )
        .await?
    } else {
        RaftNodeFacade::bootstrap(
            node_id,
            voter_ids,
            storage,
            state_machine.clone(),
            Some(network),
        )
        .await?
    };
    raft.spawn_tick_task(Duration::from_millis(100));

    let service = MetaGrpcService::new(MetaServiceState {
        raft: Arc::new(raft),
        state_machine,
        advertised_leader,
    });

    let addr = listen_addr.parse()?;
    Server::builder()
        .add_service(MetaServiceServer::new(service.clone()))
        .add_service(RaftTransportServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

fn parse_u64_csv(input: &str) -> Vec<u64> {
    input
        .split(',')
        .filter_map(|item| item.trim().parse::<u64>().ok())
        .collect()
}

fn parse_peers(input: &str) -> HashMap<u64, String> {
    input
        .split(',')
        .filter_map(|item| {
            let (id, addr) = item.split_once('=')?;
            let id = id.trim().parse::<u64>().ok()?;
            Some((id, addr.trim().to_string()))
        })
        .collect()
}
