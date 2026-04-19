use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::{Mutex, mpsc};
use tonic::transport::Channel;

use crate::pb::raft_transport_client::RaftTransportClient;
use crate::pb::{RaftMessage, RaftMessageAck};

#[derive(Clone)]
pub struct RaftNetwork {
    peers: Arc<HashMap<u64, String>>,
    tx: mpsc::Sender<OutboundRaftMessage>,
}

#[derive(Debug)]
pub struct OutboundRaftMessage {
    pub target_id: u64,
    pub payload: Vec<u8>,
}

impl RaftNetwork {
    pub fn new(peers: HashMap<u64, String>) -> (Self, mpsc::Receiver<OutboundRaftMessage>) {
        let (tx, rx) = mpsc::channel(1024);
        (
            Self {
                peers: Arc::new(peers),
                tx,
            },
            rx,
        )
    }

    pub async fn enqueue(&self, target_id: u64, payload: Vec<u8>) -> Result<()> {
        self.tx
            .send(OutboundRaftMessage { target_id, payload })
            .await
            .context("enqueue outbound raft message")
    }

    pub async fn run_sender(self, mut rx: mpsc::Receiver<OutboundRaftMessage>) {
        let clients = Arc::new(Mutex::new(
            HashMap::<u64, RaftTransportClient<Channel>>::new(),
        ));
        while let Some(message) = rx.recv().await {
            if let Err(error) = self.send_one(&clients, message).await {
                tracing::warn!(?error, "send raft message failed");
            }
        }
    }

    async fn send_one(
        &self,
        clients: &Arc<Mutex<HashMap<u64, RaftTransportClient<Channel>>>>,
        message: OutboundRaftMessage,
    ) -> Result<RaftMessageAck> {
        let address = self
            .peers
            .get(&message.target_id)
            .with_context(|| format!("raft peer {} not configured", message.target_id))?
            .clone();
        let mut clients = clients.lock().await;
        let client = if let Some(client) = clients.get_mut(&message.target_id) {
            client.clone()
        } else {
            let endpoint = format!("http://{address}");
            let client = RaftTransportClient::connect(endpoint)
                .await
                .with_context(|| format!("connect raft peer {}", message.target_id))?;
            clients.insert(message.target_id, client.clone());
            client
        };
        drop(clients);

        let mut client = client;
        let response = client
            .send_raft_message(RaftMessage {
                data: message.payload,
            })
            .await
            .context("invoke raft transport")?;
        Ok(response.into_inner())
    }
}
