use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use protobuf::Message;
use raft::eraftpb::{HardState, Snapshot};

const SNAPSHOT_KEY: &[u8] = b"raft_snapshot";
const HARD_STATE_KEY: &[u8] = b"raft_hard_state";

#[derive(Clone)]
pub struct SnapshotStore {
    db: sled::Db,
    cached: Arc<RwLock<Option<Snapshot>>>,
    cached_hard_state: Arc<RwLock<Option<HardState>>>,
}

impl SnapshotStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let db = sled::open(dir.as_ref())
            .with_context(|| format!("open snapshot store at {}", dir.as_ref().display()))?;
        let cached = db
            .get(SNAPSHOT_KEY)
            .context("read snapshot record from snapshot store")?
            .map(|bytes| {
                Snapshot::parse_from_bytes(bytes.as_ref())
                    .context("decode snapshot from snapshot store")
            })
            .transpose()?;
        let cached_hard_state = db
            .get(HARD_STATE_KEY)
            .context("read hard state record from snapshot store")?
            .map(|bytes| {
                HardState::parse_from_bytes(bytes.as_ref())
                    .context("decode hard state from snapshot store")
            })
            .transpose()?;
        Ok(Self {
            db,
            cached: Arc::new(RwLock::new(cached)),
            cached_hard_state: Arc::new(RwLock::new(cached_hard_state)),
        })
    }

    pub fn load(&self) -> Option<Snapshot> {
        self.cached.read().clone()
    }

    pub fn persist(&self, snapshot: &Snapshot) -> Result<()> {
        let encoded = snapshot
            .write_to_bytes()
            .context("encode snapshot for snapshot store")?;
        self.db
            .insert(SNAPSHOT_KEY, encoded)
            .context("persist snapshot into snapshot store")?;
        self.db.flush().context("flush snapshot store")?;
        *self.cached.write() = Some(snapshot.clone());
        Ok(())
    }

    pub fn load_hard_state(&self) -> Option<HardState> {
        self.cached_hard_state.read().clone()
    }

    pub fn persist_hard_state(&self, hard_state: &HardState) -> Result<()> {
        let encoded = hard_state
            .write_to_bytes()
            .context("encode hard state for snapshot store")?;
        self.db
            .insert(HARD_STATE_KEY, encoded)
            .context("persist hard state into snapshot store")?;
        self.db.flush().context("flush snapshot store")?;
        *self.cached_hard_state.write() = Some(hard_state.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use protobuf::SingularPtrField;
    use raft::eraftpb::{ConfState, HardState, Snapshot, SnapshotMetadata};

    use super::SnapshotStore;

    #[test]
    fn persists_and_reloads_snapshot() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("snapshot_db");
        {
            let store = SnapshotStore::open(&path)?;
            let mut snapshot = Snapshot::default();
            let mut metadata = SnapshotMetadata::default();
            metadata.index = 7;
            metadata.term = 3;
            metadata.conf_state = SingularPtrField::some(ConfState::default());
            snapshot.metadata = SingularPtrField::some(metadata);
            snapshot.data = b"hello".to_vec().into();
            store.persist(&snapshot)?;
        }

        let reopened = SnapshotStore::open(&path)?;
        let restored = reopened.load().expect("snapshot should exist");
        assert_eq!(restored.get_metadata().index, 7);
        assert_eq!(restored.get_metadata().term, 3);
        assert_eq!(restored.data.as_ref(), b"hello");
        Ok(())
    }

    #[test]
    fn persists_and_reloads_hard_state() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("snapshot_db");
        {
            let store = SnapshotStore::open(&path)?;
            let mut hard_state = HardState::default();
            hard_state.term = 5;
            hard_state.vote = 2;
            hard_state.commit = 11;
            store.persist_hard_state(&hard_state)?;
        }

        let reopened = SnapshotStore::open(&path)?;
        let restored = reopened.load_hard_state().expect("hard state should exist");
        assert_eq!(restored.term, 5);
        assert_eq!(restored.vote, 2);
        assert_eq!(restored.commit, 11);
        Ok(())
    }
}
