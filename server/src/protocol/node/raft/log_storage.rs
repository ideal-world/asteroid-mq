use super::TypeConfig;
use openraft::{storage::RaftLogStorage, LogId, RaftLogReader, RaftTypeConfig, Vote};
use openraft::{LogState, RaftLogId, StorageError};
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone)]
pub struct LogStorage<C: RaftTypeConfig = TypeConfig> {
    inner: Arc<tokio::sync::Mutex<LogStorageInner<C>>>,
}

impl Default for LogStorage<TypeConfig> {
    fn default() -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(LogStorageInner::default())),
        }
    }
}
#[derive(Debug, Default)]
pub struct LogStorageInner<C: RaftTypeConfig> {
    /// The last purged log id.
    last_purged_log_id: Option<LogId<C::NodeId>>,

    /// The Raft log.
    log: BTreeMap<u64, C::Entry>,

    /// The commit log id.
    committed: Option<LogId<C::NodeId>>,

    /// The current granted vote.
    vote: Option<Vote<C::NodeId>>,
}

impl<C: RaftTypeConfig> LogStorageInner<C> {
    pub fn new() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
        }
    }
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>>
    where
        C::Entry: Clone,
    {
        let response = self
            .log
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }

    async fn get_log_state(&self) -> Result<openraft::LogState<C>, StorageError<C::NodeId>> {
        let last = self
            .log
            .iter()
            .next_back()
            .map(|(_, ent)| *ent.get_log_id());

        let last_purged = self.last_purged_log_id;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> Result<(), StorageError<C::NodeId>> {
        self.committed = committed;
        Ok(())
    }
    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.committed)
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        self.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        Ok(self.vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<C>,
    ) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        // Simple implementation that calls the flush-before-return `append_to_log`.
        for entry in entries {
            self.log.insert(entry.get_log_id().index, entry);
        }
        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let keys = self
            .log
            .range(log_id.index..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        {
            let ld = &mut self.last_purged_log_id;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }

        {
            let keys = self
                .log
                .range(..=log_id.index)
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
            for key in keys {
                self.log.remove(&key);
            }
        }

        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for LogStorage<TypeConfig> {
    async fn try_get_log_entries<
        RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
    >(
        &mut self,
        range: RB,
    ) -> Result<
        Vec<<TypeConfig as openraft::RaftTypeConfig>::Entry>,
        openraft::StorageError<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        self.inner.lock().await.try_get_log_entries(range).await
    }
}

impl RaftLogStorage<TypeConfig> for LogStorage<TypeConfig> {
    type LogReader = Self;
    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<TypeConfig>,
    ) -> Result<(), openraft::StorageError<<TypeConfig as openraft::RaftTypeConfig>::NodeId>>
    where
        I: IntoIterator<Item = <TypeConfig as openraft::RaftTypeConfig>::Entry>
            + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        self.inner.lock().await.append(entries, callback).await
    }
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
    async fn get_log_state(
        &mut self,
    ) -> Result<
        openraft::LogState<TypeConfig>,
        openraft::StorageError<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        self.inner.lock().await.get_log_state().await
    }
    async fn purge(
        &mut self,
        log_id: openraft::LogId<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
    ) -> Result<(), openraft::StorageError<<TypeConfig as openraft::RaftTypeConfig>::NodeId>> {
        self.inner.lock().await.purge(log_id).await
    }
    async fn read_committed(
        &mut self,
    ) -> Result<
        Option<openraft::LogId<<TypeConfig as openraft::RaftTypeConfig>::NodeId>>,
        openraft::StorageError<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        self.inner.lock().await.read_committed().await
    }
    async fn save_committed(
        &mut self,
        committed: Option<openraft::LogId<<TypeConfig as openraft::RaftTypeConfig>::NodeId>>,
    ) -> Result<(), openraft::StorageError<<TypeConfig as openraft::RaftTypeConfig>::NodeId>> {
        self.inner.lock().await.save_committed(committed).await
    }
    async fn read_vote(
        &mut self,
    ) -> Result<
        Option<openraft::Vote<<TypeConfig as openraft::RaftTypeConfig>::NodeId>>,
        openraft::StorageError<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
    > {
        self.inner.lock().await.read_vote().await
    }
    async fn save_vote(
        &mut self,
        vote: &openraft::Vote<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
    ) -> Result<(), openraft::StorageError<<TypeConfig as openraft::RaftTypeConfig>::NodeId>> {
        self.inner.lock().await.save_vote(vote).await
    }
    async fn truncate(
        &mut self,
        log_id: openraft::LogId<<TypeConfig as openraft::RaftTypeConfig>::NodeId>,
    ) -> Result<(), openraft::StorageError<<TypeConfig as openraft::RaftTypeConfig>::NodeId>> {
        self.inner.lock().await.truncate(log_id).await
    }
}
