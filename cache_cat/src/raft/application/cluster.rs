use crate::error::{CacheCatError, Error, ProtocolError, StorageError};
use crate::raft::types::endpoint::Endpoint;
use crate::raft::types::entry::request::Request;
use crate::raft::types::raft_types::{Node, NodeId, Raft, TypeConfig};
use openraft::ReadPolicy::LeaseRead;
use openraft::alias::VoteOf;
use openraft::async_runtime::WatchReceiver;
use openraft::base::BoxStream;
use openraft::error::{ClientWriteError, Fatal, InitializeError, LinearizableReadError, RaftError};
use openraft::raft::linearizable_read::Linearizer;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse, SnapshotResponse,
    VoteRequest, VoteResponse, WriteResult,
};
use openraft::{ChangeMembers, ReadPolicy, Snapshot, WatchChangeHandle};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU16;
use tracing::{error, info};

pub struct Cluster {
    current_endpoint: Endpoint,
    watch_change_handle: WatchChangeHandle<TypeConfig>,
    last_master: Arc<AtomicU16>,
    raft: Raft,
}

impl Cluster {
    pub fn new(raft: Raft, current_endpoint: Endpoint) -> Self {
        let last_master = Arc::new(AtomicU16::new(raft.node_id().clone()));
        let last = last_master.clone();
        let watch_change_handle = raft.on_cluster_leader_change(move |_old, new| {
            let last = last.clone();
            async move {
                // 假设 LeaderId 有 node_id 字段，并且是 u16 类型
                last.store(new.0.node_id, std::sync::atomic::Ordering::Relaxed);
            }
        });
        Self {
            current_endpoint,
            watch_change_handle,
            last_master,
            raft,
        }
    }

    pub fn last_slave(&self) -> Vec<Endpoint> {
        let last_master_id = self.last_master.load(std::sync::atomic::Ordering::Relaxed);
        let mut res = Vec::new();
        for (node_id, node) in self.nodes() {
            if node_id != last_master_id {
                res.push(node.endpoint);
            }
        }
        res
    }

    //如果没有选出过leader就返回自己
    pub fn last_leader(&self) -> Endpoint {
        let node_id = self.last_master.load(std::sync::atomic::Ordering::Relaxed);
        let metrics_guard = self.raft.metrics();
        let metrics = metrics_guard.borrow_watched();
        metrics
            .membership_config
            .nodes()
            .find(|(id, _)| **id == node_id)
            .map(|(_, node)| node.endpoint.clone())
            .unwrap_or_else(|| self.current_endpoint.clone())
    }
    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }

    pub fn node_id(&self) -> &NodeId {
        self.raft.node_id()
    }

    pub async fn leader_addr(&self) -> Option<Endpoint> {
        let leader_id = self.raft.current_leader().await?;
        for (node_id, node) in self.nodes() {
            if node_id == leader_id {
                return Some(node.endpoint);
            }
        }
        None
    }

    pub fn nodes(&self) -> Vec<(NodeId, Node)> {
        let metrics_guard = self.raft.metrics();
        let metrics = metrics_guard.borrow_watched();
        let vec = metrics
            .membership_config
            .nodes()
            .map(|(node_id, node)| (*node_id, node.clone()))
            .collect::<Vec<_>>();
        vec
    }

    pub async fn initialize(&self, members: BTreeMap<NodeId, Node>) -> Result<(), CacheCatError> {
        if let Err(e) = self.raft.initialize(members).await {
            match e {
                RaftError::APIError(e) => match e {
                    InitializeError::NotAllowed(e) => {
                        info!("Already initialized: {}", e);
                    }
                    InitializeError::NotInMembers(e) => {
                        return Err(Error::config(e.to_string()));
                    }
                },
                RaftError::Fatal(e) => {
                    return Err(Error::internal(e.to_string()));
                }
            }
        }
        Ok(())
    }

    pub async fn trigger_snapshot(&self) -> Result<(), CacheCatError> {
        self.raft.trigger().snapshot().await.map_err(|e| {
            error!("snapshot error: {}", e);
            ProtocolError::Custom("snapshot error")
        })?;
        Ok(())
    }
    #[inline]
    pub async fn get_read_linearizer(
        &self,
        read_policy: ReadPolicy,
    ) -> Result<Linearizer<TypeConfig>, RaftError<TypeConfig, LinearizableReadError<TypeConfig>>>
    {
        self.raft.get_read_linearizer(read_policy).await
    }
    #[inline]
    pub async fn lease_read(&self) -> Result<(), CacheCatError> {
        let linearizer = self
            .raft
            .get_read_linearizer(LeaseRead)
            .await
            .map_err(|e| StorageError::ReadFailed(e.to_string()))?;
        linearizer
            .await_ready(&self.raft)
            .await
            .map_err(|e| StorageError::WriteFailed(e.to_string()))?;
        Ok(())
    }

    #[inline]
    pub async fn install_full_snapshot(
        &self,
        vote: VoteOf<TypeConfig>,
        snapshot: Snapshot<TypeConfig>,
    ) -> Result<SnapshotResponse<TypeConfig>, Fatal<TypeConfig>> {
        self.raft.install_full_snapshot(vote, snapshot).await
    }

    #[inline]
    pub fn voter_ids(&self) -> impl Iterator<Item = NodeId> {
        self.raft.voter_ids()
    }

    #[inline]
    pub async fn change_membership(
        &self,
        members: impl Into<ChangeMembers<TypeConfig>>,
    ) -> Result<ClientWriteResponse<TypeConfig>, RaftError<TypeConfig, ClientWriteError<TypeConfig>>>
    {
        self.raft.change_membership(members, true).await
    }

    #[inline]
    pub async fn add_learner(
        &self,
        node_id: NodeId,
        node: Node,
    ) -> Result<ClientWriteResponse<TypeConfig>, RaftError<TypeConfig, ClientWriteError<TypeConfig>>>
    {
        self.raft.add_learner(node_id, node, true).await
    }

    #[inline]
    pub async fn append_entries(
        &self,
        rpc: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RaftError<TypeConfig>> {
        self.raft.append_entries(rpc).await
    }

    #[inline]
    pub async fn client_write(
        &self,
        request: Request,
    ) -> Result<ClientWriteResponse<TypeConfig>, RaftError<TypeConfig, ClientWriteError<TypeConfig>>>
    {
        self.raft.client_write(request).await
    }
    #[inline]
    pub async fn client_write_many(
        &self,
        request: Vec<Request>,
    ) -> Result<
        BoxStream<'static, Result<WriteResult<TypeConfig>, Fatal<TypeConfig>>>,
        Fatal<TypeConfig>,
    > {
        self.raft.client_write_many(request).await
    }

    #[inline]
    pub async fn vote(
        &self,
        request: VoteRequest<TypeConfig>,
    ) -> Result<VoteResponse<TypeConfig>, RaftError<TypeConfig>> {
        self.raft.vote(request).await
    }
}
