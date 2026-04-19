use serde::{Deserialize, Serialize};

use crate::model::{ServerNode, TabletRoute};

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
}
