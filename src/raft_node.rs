use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use protobuf::Message as ProtobufMessage;
use raft::eraftpb::{Entry, EntryType, Message as RaftMessageProto};
use raft::raw_node::RawNode;
use raft::{Config, StateRole};
use tokio::sync::Mutex;

use crate::command::MetaCommand;
use crate::network::RaftNetwork;
use crate::state_machine::MetaStateMachine;
use crate::storage::MetaStorage;

#[derive(Clone)]
pub struct RaftNodeFacade {
    inner: Arc<Mutex<RaftNodeCore>>,
    next_read_ctx: Arc<AtomicU64>,
}

struct RaftNodeCore {
    node_id: u64,
    raw_node: RawNode<MetaStorage>,
    storage: MetaStorage,
    state_machine: MetaStateMachine,
    network: Option<RaftNetwork>,
    applied_index: u64,
    applied_term: u64,
    completed_reads: HashMap<Vec<u8>, u64>,
}

impl RaftNodeFacade {
    pub async fn bootstrap(
        node_id: u64,
        voter_ids: Vec<u64>,
        storage: MetaStorage,
        state_machine: MetaStateMachine,
        network: Option<RaftNetwork>,
    ) -> Result<Self> {
        let config = Config {
            id: node_id,
            election_tick: 10,
            heartbeat_tick: 3,
            check_quorum: true,
            ..Default::default()
        };
        config.validate().context("validate raft config")?;

        let conf_state = (voter_ids, vec![]).into();
        storage.set_conf_state(conf_state);
        let raw_node =
            RawNode::with_default_logger(&config, storage.clone()).context("create raw node")?;

        Ok(Self {
            inner: Arc::new(Mutex::new(RaftNodeCore {
                node_id,
                raw_node,
                storage,
                state_machine,
                network,
                applied_index: 0,
                applied_term: 0,
                completed_reads: HashMap::new(),
            })),
            next_read_ctx: Arc::new(AtomicU64::new(1)),
        })
    }

    pub async fn bootstrap_single_node(
        node_id: u64,
        storage: MetaStorage,
        state_machine: MetaStateMachine,
        network: Option<RaftNetwork>,
    ) -> Result<Self> {
        let facade =
            Self::bootstrap(node_id, vec![node_id], storage, state_machine, network).await?;
        facade.campaign().await.context("campaign single node")?;
        facade.process_ready().await?;
        Ok(facade)
    }

    pub fn spawn_tick_task(&self, period: Duration) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(period);
            loop {
                interval.tick().await;
                if let Err(error) = this.tick_once().await {
                    tracing::warn!(?error, "raft tick failed");
                }
            }
        });
    }

    pub async fn propose(&self, payload: Vec<u8>) -> Result<()> {
        {
            let mut inner = self.inner.lock().await;
            inner
                .raw_node
                .propose(Vec::new(), payload)
                .context("raft propose")?;
        }
        self.process_ready().await
    }

    pub async fn campaign(&self) -> Result<()> {
        {
            let mut inner = self.inner.lock().await;
            inner.raw_node.campaign().context("raft campaign")?;
        }
        self.process_ready().await
    }

    pub async fn read_index(&self) -> Result<()> {
        let request_ctx = self
            .next_read_ctx
            .fetch_add(1, Ordering::Relaxed)
            .to_be_bytes()
            .to_vec();
        {
            let mut inner = self.inner.lock().await;
            if inner.raw_node.raft.state != StateRole::Leader {
                return Err(anyhow!(
                    "node {} is not leader, state {:?}",
                    inner.node_id,
                    inner.raw_node.raft.state
                ));
            }
            inner.raw_node.read_index(request_ctx.clone());
        }
        self.process_ready().await?;

        let mut inner = self.inner.lock().await;
        let applied = inner
            .completed_reads
            .remove(&request_ctx)
            .ok_or_else(|| anyhow!("read index did not complete for current request"))?;
        if applied > inner.applied_index {
            return Err(anyhow!(
                "read index {} advanced beyond applied index {}",
                applied,
                inner.applied_index
            ));
        }
        Ok(())
    }

    pub async fn step(&self, payload: Vec<u8>) -> Result<()> {
        let message =
            RaftMessageProto::parse_from_bytes(&payload).context("decode raft message")?;
        {
            let mut inner = self.inner.lock().await;
            inner.raw_node.step(message).context("raft step")?;
        }
        self.process_ready().await
    }

    pub async fn tick_once(&self) -> Result<()> {
        let should_process = {
            let mut inner = self.inner.lock().await;
            inner.raw_node.tick()
        };
        if should_process {
            self.process_ready().await?;
        }
        Ok(())
    }

    pub async fn state_role(&self) -> StateRole {
        let inner = self.inner.lock().await;
        inner.raw_node.raft.state
    }

    pub async fn leader_id(&self) -> u64 {
        let inner = self.inner.lock().await;
        inner.raw_node.raft.leader_id
    }

    async fn process_ready(&self) -> Result<()> {
        loop {
            let (outbound_messages, persisted_messages) = {
                let mut inner = self.inner.lock().await;
                if !inner.raw_node.has_ready() {
                    Self::mark_completed_reads(&mut inner);
                    break;
                }

                let mut ready = inner.raw_node.ready();

                if let Some(hs) = ready.hs() {
                    inner.storage.set_hard_state(hs.clone());
                }

                if !ready.snapshot().is_empty() {
                    inner.storage.install_snapshot(ready.snapshot().clone());
                }

                let entries = ready.take_entries();
                if !entries.is_empty() {
                    inner.storage.append(&entries)?;
                }

                let ready_read_states = ready.take_read_states();
                for read_state in ready_read_states {
                    inner
                        .completed_reads
                        .insert(read_state.request_ctx, read_state.index);
                }

                Self::apply_entries(
                    &mut inner,
                    ready.take_committed_entries(),
                    "ready committed entries",
                )?;

                let outbound_messages = ready.take_messages();
                let persisted_messages = ready.take_persisted_messages();

                let mut light_ready = inner.raw_node.advance(ready);
                Self::apply_entries(
                    &mut inner,
                    light_ready.take_committed_entries(),
                    "light ready committed entries",
                )?;
                let light_messages = light_ready.take_messages();
                inner.raw_node.advance_apply_to(inner.applied_index);

                let mut all_outbound = outbound_messages;
                all_outbound.extend(light_messages);
                Self::mark_completed_reads(&mut inner);
                (all_outbound, persisted_messages)
            };

            self.dispatch_messages(outbound_messages).await?;
            self.dispatch_messages(persisted_messages).await?;
        }

        Ok(())
    }

    fn apply_entries(inner: &mut RaftNodeCore, entries: Vec<Entry>, context: &str) -> Result<()> {
        for entry in entries {
            inner.applied_index = inner.applied_index.max(entry.index);
            inner.applied_term = entry.term;
            if entry.data.is_empty() {
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryNormal => {
                    let command: MetaCommand = serde_json::from_slice(&entry.data)
                        .with_context(|| format!("decode meta command from {context}"))?;
                    inner
                        .state_machine
                        .apply(command)
                        .with_context(|| format!("apply meta command from {context}"))?;
                }
                EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
                    return Err(anyhow!(
                        "config change entries are not implemented in {context}"
                    ));
                }
            }
        }

        inner.storage.create_snapshot(
            inner.state_machine.snapshot_bytes()?,
            inner.applied_index,
            inner.applied_term,
        );
        Ok(())
    }

    fn mark_completed_reads(inner: &mut RaftNodeCore) {
        inner.completed_reads.retain(|_, read_index| {
            if *read_index <= inner.applied_index {
                *read_index = inner.applied_index;
            }
            true
        });
    }

    async fn dispatch_messages(&self, messages: Vec<RaftMessageProto>) -> Result<()> {
        let network = {
            let inner = self.inner.lock().await;
            inner.network.clone()
        };
        let Some(network) = network else {
            return Ok(());
        };

        for message in messages {
            let target = message.to;
            let payload = message.write_to_bytes().context("encode raft message")?;
            network.enqueue(target, payload).await?;
        }
        Ok(())
    }
}
