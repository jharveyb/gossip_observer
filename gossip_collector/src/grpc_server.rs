use observer_common::types::{LdkNodeConfig, PeerConnectionInfo, SharedUsize};
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use tonic::{Request, Response, Status};

use observer_common::collectorrpc;

use crate::{node_manager::current_peers, peer_conn_manager::PeerConnManagerHandle};

// Any state we need to implement our RPC server.
pub struct CollectorServiceImpl {
    node: Arc<ldk_node::Node>,
    conn_manager: PeerConnManagerHandle,
    target_peer_count: SharedUsize,
}

impl CollectorServiceImpl {
    pub fn new(
        conn_manager: PeerConnManagerHandle,
        node: Arc<ldk_node::Node>,
        target_peer_count: SharedUsize,
    ) -> Self {
        Self {
            conn_manager,
            node,
            target_peer_count,
        }
    }
}

#[tonic::async_trait]
impl collectorrpc::collector_service_server::CollectorService for CollectorServiceImpl {
    async fn get_node_config(
        &self,
        _request: Request<collectorrpc::NodeConfigRequest>,
    ) -> Result<Response<collectorrpc::NodeConfigResponse>, Status> {
        let cfg = self.node.config();
        let node_id = self.node.node_id();

        let resp_cfg = LdkNodeConfig {
            listening_addresses: cfg.listening_addresses,
            node_alias: cfg.node_alias,
            node_id,
        };

        Ok(Response::new(resp_cfg.into()))
    }

    async fn post_eligible_peers(
        &self,
        req: Request<collectorrpc::EligiblePeersRequest>,
    ) -> Result<Response<collectorrpc::EligiblePeersResponse>, Status> {
        let peers: Vec<PeerConnectionInfo> = req
            .into_inner()
            .try_into()
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        for peer in peers {
            self.conn_manager.add_eligible_peer(peer);
        }

        Ok(Response::new(collectorrpc::EligiblePeersResponse {}))
    }

    async fn post_target_peer_count(
        &self,
        req: Request<collectorrpc::TargetPeerCountRequest>,
    ) -> Result<Response<collectorrpc::TargetPeerCountResponse>, Status> {
        let target = req.into_inner().target;
        self.target_peer_count.store(target as usize, SeqCst);
        Ok(Response::new(collectorrpc::TargetPeerCountResponse {}))
    }

    async fn get_current_peers(
        &self,
        _req: Request<collectorrpc::CurrentPeersRequest>,
    ) -> Result<Response<collectorrpc::CurrentPeersResponse>, Status> {
        let mut peers = current_peers(self.node.clone())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        peers.sort_unstable_by_key(|p| p.node_id);
        Ok(Response::new(peers.into()))
    }
}

pub fn create_service(
    conn_manager: PeerConnManagerHandle,
    node: Arc<ldk_node::Node>,
    target_peer_count: SharedUsize,
) -> collectorrpc::collector_service_server::CollectorServiceServer<CollectorServiceImpl> {
    collectorrpc::collector_service_server::CollectorServiceServer::new(CollectorServiceImpl::new(
        conn_manager,
        node,
        target_peer_count,
    ))
}
