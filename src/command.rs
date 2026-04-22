use serde::{Deserialize, Serialize};

use crate::model::{
    SchedulerCommandSpec, ServerNode, StreamAssignmentState, TabletRoute, WalRaftGroupState,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MetaCommand {
    UpsertNode(ServerNode),
    UpdateNodeHeartbeat {
        node_id: u64,
        last_heartbeat_ts: u64,
    },
    MarkNodeState {
        node_id: u64,
        state: crate::model::NodeState,
    },
    UpsertTabletRoute(TabletRoute),
    BumpTabletEpoch {
        tablet_id: u64,
        next_epoch: u64,
        next_leader_cache_node_id: Option<u64>,
    },
    CreateSchedulerCommand {
        target_node_id: u64,
        spec: SchedulerCommandSpec,
        created_at_ts: u64,
    },
    AckSchedulerCommand {
        command_id: u64,
        acked_at_ts: u64,
    },
    CompleteSchedulerCommand {
        command_id: u64,
        completed_at_ts: u64,
    },
    FailSchedulerCommand {
        command_id: u64,
        failed_at_ts: u64,
        reason: String,
    },
    UpsertTabletOperatorAlert {
        tablet_id: u64,
        reason: String,
        created_at_ts: u64,
    },
    ClearTabletOperatorAlert {
        tablet_id: u64,
    },
    CreateWalRaftGroup {
        cluster_name: String,
        raft_group_id: u64,
        epoch: u64,
        replicas: Vec<(u64, String)>,
        leader_node_id: u64,
        state: WalRaftGroupState,
    },
    UpdateWalRaftGroupState {
        cluster_name: String,
        raft_group_id: u64,
        epoch: u64,
        leader_node_id: Option<u64>,
        state: WalRaftGroupState,
    },
    DeleteWalRaftGroup {
        cluster_name: String,
        raft_group_id: u64,
        epoch: u64,
    },
    AssignStream {
        cluster_name: String,
        stream_id: u64,
        raft_group_id: u64,
        epoch: u64,
        state: StreamAssignmentState,
    },
    UnassignStream {
        cluster_name: String,
        stream_id: u64,
        epoch: u64,
    },
}
