use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::RwLock;
use protobuf::Message;
use raft::GetEntriesContext;
use raft::Result as RaftResult;
use raft::Storage;
use raft::eraftpb::{ConfState, Entry, HardState, Snapshot, SnapshotMetadata};
use raft::{Error as RaftError, RaftState};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct MetaStorage {
    inner: Arc<RwLock<StorageCore>>,
}

struct StorageCore {
    hard_state: HardState,
    conf_state: ConfState,
    entries: Vec<Entry>,
    snapshot: Snapshot,
    wal: File,
}

#[derive(Debug, Serialize, Deserialize)]
struct WalRecord {
    entry_bytes: Vec<u8>,
}

impl MetaStorage {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        create_dir_all(dir.as_ref()).context("create meta storage directory")?;
        let wal_path = PathBuf::from(dir.as_ref()).join("meta-raft.wal");
        let wal = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)
            .with_context(|| format!("open raft wal at {}", wal_path.display()))?;

        let (entries, hard_state) = Self::load_wal(&wal_path)?;

        Ok(Self {
            inner: Arc::new(RwLock::new(StorageCore {
                hard_state,
                conf_state: ConfState::default(),
                entries,
                snapshot: Snapshot::default(),
                wal,
            })),
        })
    }

    pub fn append(&self, new_entries: &[Entry]) -> Result<()> {
        let mut inner = self.inner.write();
        if let Some(first_new) = new_entries.first()
            && first_new.index < inner.entries.len() as u64
        {
            inner.entries.truncate(first_new.index as usize);
        }
        for entry in new_entries {
            let record = WalRecord {
                entry_bytes: entry.write_to_bytes().context("encode wal entry")?,
            };
            let encoded = serde_json::to_vec(&record).context("encode wal record")?;
            inner.wal.write_all(&encoded).context("append wal record")?;
            inner.wal.write_all(b"\n").context("append wal newline")?;
            inner.entries.push(entry.clone());
        }
        inner.wal.flush().context("flush raft wal")?;
        Ok(())
    }

    pub fn set_hard_state(&self, hard_state: HardState) {
        self.inner.write().hard_state = hard_state;
    }

    pub fn hard_state(&self) -> HardState {
        self.inner.read().hard_state.clone()
    }

    pub fn set_conf_state(&self, conf_state: ConfState) {
        self.inner.write().conf_state = conf_state;
    }

    pub fn conf_state(&self) -> ConfState {
        self.inner.read().conf_state.clone()
    }

    pub fn install_snapshot(&self, snapshot: Snapshot) {
        let mut inner = self.inner.write();
        let index = snapshot.get_metadata().index;
        let term = snapshot.get_metadata().term;
        let mut marker = Entry::default();
        marker.index = index;
        marker.term = term;
        inner.entries.clear();
        inner.entries.push(marker);
        inner.snapshot = snapshot;
    }

    pub fn create_snapshot(&self, data: Vec<u8>, index: u64, term: u64) {
        let mut snapshot = Snapshot::default();
        let mut metadata = SnapshotMetadata::default();
        metadata.index = index;
        metadata.term = term;
        metadata.conf_state = protobuf::SingularPtrField::some(self.conf_state());
        snapshot.metadata = protobuf::SingularPtrField::some(metadata);
        snapshot.data = data.into();
        self.install_snapshot(snapshot);
    }

    pub fn all_entries(&self) -> Vec<Entry> {
        self.inner.read().entries.clone()
    }

    fn load_wal(path: &Path) -> Result<(Vec<Entry>, HardState)> {
        if !path.exists() {
            return Ok((vec![Entry::default()], HardState::default()));
        }

        let file =
            File::open(path).with_context(|| format!("open wal for replay {}", path.display()))?;
        let reader = BufReader::new(file);
        let mut entries = vec![Entry::default()];
        for line in reader.lines() {
            let line = line.context("read wal line")?;
            if line.trim().is_empty() {
                continue;
            }
            let record: WalRecord =
                serde_json::from_str(&line).context("decode wal record during replay")?;
            let entry = Entry::parse_from_bytes(&record.entry_bytes)
                .context("decode wal entry during replay")?;
            if entry.index < entries.len() as u64 {
                entries.truncate(entry.index as usize);
            }
            entries.push(entry);
        }

        let mut hard_state = HardState::default();
        if let Some(last) = entries.last()
            && last.index > 0
        {
            hard_state.term = last.term;
            hard_state.commit = last.index;
        }
        Ok((entries, hard_state))
    }
}

impl Storage for MetaStorage {
    fn initial_state(&self) -> RaftResult<RaftState> {
        let inner = self.inner.read();
        Ok(RaftState {
            hard_state: inner.hard_state.clone(),
            conf_state: inner.conf_state.clone(),
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        let inner = self.inner.read();
        let start = low as usize;
        let end = high as usize;
        if start >= inner.entries.len() || end > inner.entries.len() || start > end {
            return Err(RaftError::Store(raft::StorageError::Unavailable));
        }

        let max_size = max_size.into().unwrap_or(u64::MAX) as usize;
        let mut total = 0usize;
        let mut out = Vec::new();
        for entry in &inner.entries[start..end] {
            total += entry.compute_size() as usize;
            if !out.is_empty() && total > max_size {
                break;
            }
            out.push(entry.clone());
        }
        Ok(out)
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        let inner = self.inner.read();
        inner
            .entries
            .get(idx as usize)
            .map(|entry| entry.term)
            .ok_or(RaftError::Store(raft::StorageError::Unavailable))
    }

    fn first_index(&self) -> RaftResult<u64> {
        let inner = self.inner.read();
        Ok(inner
            .entries
            .iter()
            .find(|entry| entry.index > 0)
            .map(|entry| entry.index)
            .unwrap_or(1))
    }

    fn last_index(&self) -> RaftResult<u64> {
        let inner = self.inner.read();
        Ok(inner
            .entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or_default())
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> RaftResult<Snapshot> {
        Ok(self.inner.read().snapshot.clone())
    }
}
