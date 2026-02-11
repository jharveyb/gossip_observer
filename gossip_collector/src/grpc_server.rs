use observer_common::types::{Balances, LdkNodeConfig, PeerConnectionInfo, SharedUsize};
use std::sync::Arc;
use std::sync::atomic::Ordering::SeqCst;
use tokio_util::sync::CancellationToken;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status};

use observer_common::collectorrpc;
use observer_common::common::{
    BalancesRequest, BalancesResponse, OpenChannelRequest, OpenChannelResponse, ShutdownRequest,
    ShutdownResponse, UpdateChannelConfigRequest, UpdateChannelConfigResponse,
};
use tracing::info;

use crate::node_manager::{balances, current_peers, open_channel, random_channel_cfg};
use crate::peer_conn_manager::PeerConnManagerHandle;

// Any state we need to implement our RPC server.
pub struct CollectorServiceImpl {
    node: Arc<ldk_node::Node>,
    conn_manager: PeerConnManagerHandle,
    stop_token: CancellationToken,
    target_peer_count: SharedUsize,
}

impl CollectorServiceImpl {
    pub fn new(
        conn_manager: PeerConnManagerHandle,
        node: Arc<ldk_node::Node>,
        stop_token: CancellationToken,
        target_peer_count: SharedUsize,
    ) -> Self {
        Self {
            conn_manager,
            node,
            stop_token,
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
        // TODO: may need to stream this response?
        let mut peers = current_peers(self.node.clone())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        peers.sort_unstable_by_key(|p| p.node_id);
        Ok(Response::new(peers.into()))
    }

    async fn balances(
        &self,
        _req: Request<BalancesRequest>,
    ) -> Result<Response<BalancesResponse>, Status> {
        let node_copy = self.node.clone();
        let balance: Balances = tokio::task::spawn_blocking(move || balances(node_copy))
            .await
            .map_err(|e| Status::internal(format!("Balances: {}", e)))?
            .into();

        Ok(Response::new(balance.into()))
    }

    async fn open_channel(
        &self,
        request: Request<OpenChannelRequest>,
    ) -> Result<Response<OpenChannelResponse>, Status> {
        let req = request
            .into_inner()
            .try_into()
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        let channel_id = open_channel(
            self.node.clone(),
            req,
            None, // Default ChannelConfig
        )
        .await
        .map_err(|e| Status::internal(format!("Failed to open channel: {}", e)))?;

        // UserChannelId is u128, convert to bytes
        Ok(Response::new(OpenChannelResponse {
            local_channel_id: channel_id.0.to_le_bytes().to_vec(),
        }))
    }

    async fn update_channel_config(
        &self,
        _req: Request<UpdateChannelConfigRequest>,
    ) -> Result<Response<UpdateChannelConfigResponse>, Status> {
        let current_channels = self.node.list_channels();
        if current_channels.is_empty() {
            info!("No channels to update");
        }
        let mut updated_scids = Vec::new();
        for chan in current_channels {
            if let Some(scid) = chan.short_channel_id
                && chan.is_usable
            {
                let new_cfg = random_channel_cfg();
                self.node
                    .update_channel_config(
                        &chan.user_channel_id,
                        chan.counterparty_node_id,
                        new_cfg,
                    )
                    .map_err(|e| {
                        Status::internal(format!("Failed to update channel config: {}", e))
                    })?;
                updated_scids.push(scid);
            }
        }
        Ok(Response::new(UpdateChannelConfigResponse {
            scids: updated_scids,
        }))
    }

    async fn shutdown(
        &self,
        _req: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        // TODO: what do we want to dump from node before shutdown?
        // nodelist, channel list, peer list, etc.
        info!("Collector: grpc server: received shutdown request");
        let _ = self.node.stop();
        info!("Collector: grpc server: shut down LDK node");

        // Possible weird behavior since the gRPC server is using the same token?
        self.stop_token.cancel();
        info!("Collector: grpc server: sent shutdown signal");
        Ok(Response::new(ShutdownResponse {}))
    }
}

pub fn create_service(
    conn_manager: PeerConnManagerHandle,
    node: Arc<ldk_node::Node>,
    stop_token: CancellationToken,
    target_peer_count: SharedUsize,
) -> collectorrpc::collector_service_server::CollectorServiceServer<CollectorServiceImpl> {
    let server = collectorrpc::collector_service_server::CollectorServiceServer::new(
        CollectorServiceImpl::new(conn_manager, node, stop_token, target_peer_count),
    );
    server
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(observer_common::MAX_RECV_MSG_SIZE)
}
