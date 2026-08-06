use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use observer_common::collector_client::CollectorClient;
use rand::prelude::*;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status};

use observer_common::common::gossip_graph_chunk::Data;
use observer_common::common::{
    CollectorInfo, GossipGraphChunk, GossipGraphChunkResponse, ShutdownRequest, ShutdownResponse,
};
use observer_common::controllerrpc::{
    CollectorStatusResponse, OpenCollectorChannelRequest, OpenCollectorChannelResponse,
    RegisterCollectorResponse, StatusRequest, StatusResponse, UpdateChannelsRequest,
    UpdateChannelsResponse,
};
use observer_common::util::StatusExt;
use observer_common::{controllerrpc, util};
use tracing::{debug, info};

use crate::CommunityStats;
use crate::collector_manager::{CollectorManagerHandle, compute_status, handle_collector_info};
use crate::csv_reader::NodeAnnotatedRecord;

/// Cache of connected collector clients, keyed by gRPC socket. tonic channels
/// multiplex and reconnect internally, so one client per collector suffices;
/// evict on RPC failure so the next call redials a moved endpoint.
#[derive(Clone, Default)]
pub struct CollectorClientCache {
    inner: Arc<Mutex<HashMap<String, CollectorClient>>>,
}

impl CollectorClientCache {
    pub async fn get_or_connect(
        &self,
        socket: &str,
    ) -> Result<CollectorClient, tonic::transport::Error> {
        let cached = self
            .inner
            .lock()
            .expect("client cache lock poisoned")
            .get(socket)
            .cloned();
        if let Some(client) = cached {
            return Ok(client);
        }
        let client = CollectorClient::connect(socket).await?;
        self.inner
            .lock()
            .expect("client cache lock poisoned")
            .insert(socket.to_string(), client.clone());
        Ok(client)
    }

    pub fn evict(&self, socket: &str) {
        self.inner
            .lock()
            .expect("client cache lock poisoned")
            .remove(socket);
    }
}

pub struct ControllerServiceImpl {
    stop_token: CancellationToken,
    collector_manager: CollectorManagerHandle,
    collector_clients: CollectorClientCache,
}

impl ControllerServiceImpl {
    pub fn new(
        collector_manager: CollectorManagerHandle,
        stop_token: CancellationToken,
        collector_clients: CollectorClientCache,
    ) -> Self {
        Self {
            collector_manager,
            stop_token,
            collector_clients,
        }
    }

    pub async fn build_collector_client(
        &self,
        collector_uuid: &str,
    ) -> Result<CollectorClient, Status> {
        let collector = self
            .collector_manager
            .get_collector_by_uuid(collector_uuid)
            .await
            .or_internal()?
            .ok_or_else(|| {
                Status::not_found(format!("Collector {} not found or offline", collector_uuid))
            })?;

        self.collector_clients
            .get_or_connect(&collector.info.grpc_socket)
            .await
            .or_unavailable_ctx(|| format!("Failed to connect to collector {}", collector_uuid))
    }

    /// Shared body of register_collector / collector_status: record the
    /// heartbeat, and serve the community assignment when warranted.
    async fn handle_heartbeat(
        &self,
        request: Request<CollectorInfo>,
        provide_info: bool,
    ) -> Result<(), Status> {
        let info: observer_common::types::CollectorInfo =
            request.into_inner().try_into().or_invalid_argument()?;
        let collector_socket = info.grpc_socket.clone();
        match handle_collector_info(&self.collector_manager, info, provide_info)
            .await
            .or_internal()?
        {
            Some((stats, members)) => {
                // Note this includes a short settling sleep, bounding the
                // latency of the collector's register/status RPC.
                collector_registration_reply(collector_socket, stats, members)
                    .await
                    .or_internal()?;
                Ok(())
            }
            // A registering collector must have a community assignment.
            None if provide_info => Err(Status::internal("Collector mapping not found")),
            // Normal heartbeat; nothing to serve.
            None => Ok(()),
        }
    }
}

#[tonic::async_trait]
impl controllerrpc::controller_service_server::ControllerService for ControllerServiceImpl {
    async fn register_collector(
        &self,
        request: tonic::Request<CollectorInfo>,
    ) -> Result<tonic::Response<RegisterCollectorResponse>, Status> {
        self.handle_heartbeat(request, true).await?;
        Ok(Response::new(RegisterCollectorResponse {}))
    }

    async fn collector_status(
        &self,
        request: tonic::Request<CollectorInfo>,
    ) -> Result<tonic::Response<CollectorStatusResponse>, Status> {
        // Collectors switch to this endpoint once they have their assignment,
        // so we normally don't resend the eligible peer list. If the manager
        // still returns an assignment here, the collector re-registered within
        // the heartbeat window and we serve it anyway.
        self.handle_heartbeat(request, false).await?;
        Ok(Response::new(CollectorStatusResponse {}))
    }

    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<StatusResponse>, Status> {
        let status = compute_status(&self.collector_manager)
            .await
            .or_internal()?;
        Ok(Response::new(status.into()))
    }

    async fn shutdown(
        &self,
        _req: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        info!("Controller: grpc server: received shutdown request");
        info!("Controller: grpc server: sent shutdown signal");
        self.stop_token.cancel();
        Ok(Response::new(ShutdownResponse {}))
    }

    async fn open_channel(
        &self,
        request: Request<OpenCollectorChannelRequest>,
    ) -> Result<Response<OpenCollectorChannelResponse>, Status> {
        let OpenCollectorChannelRequest {
            uuid: collector_uuid,
            request,
        } = request.into_inner();

        let mut client = self.build_collector_client(&collector_uuid).await?;

        // Convert inner request to OpenChannelCommand and forward to collector
        let inner_req =
            request.ok_or_else(|| Status::invalid_argument("request is required"))?;
        let cmd = inner_req.try_into().or_invalid_argument()?;

        let channel_id = match client.open_channel(cmd).await {
            Ok(id) => id,
            Err(e) => {
                self.collector_clients.evict(client.endpoint());
                return Err(Status::internal(format!("Failed to open channel: {}", e)));
            }
        };

        info!(
            collector_uuid = %collector_uuid,
            "Controller: opened channel via collector"
        );

        Ok(Response::new(OpenCollectorChannelResponse {
            uuid: collector_uuid,
            response: Some(observer_common::common::OpenChannelResponse {
                local_channel_id: channel_id.into(),
            }),
        }))
    }

    async fn update_channels(
        &self,
        request: Request<UpdateChannelsRequest>,
    ) -> Result<Response<UpdateChannelsResponse>, Status> {
        let collector_uuid = request.into_inner().uuid;

        let mut client = self.build_collector_client(&collector_uuid).await?;
        let scids = match client.update_channel_cfgs().await {
            Ok(scids) => scids,
            Err(e) => {
                self.collector_clients.evict(client.endpoint());
                return Err(Status::internal(format!("Failed to update channels: {}", e)));
            }
        };
        info!(
            collector_uuid = %collector_uuid,
            scids = ?scids,
            "Controller: updated channels for collector"
        );

        Ok(Response::new(UpdateChannelsResponse {
            uuid: collector_uuid,
            response: Some(observer_common::common::UpdateChannelConfigResponse { scids }),
        }))
    }

    async fn post_gossip_graph_chunk(
        &self,
        request: Request<GossipGraphChunk>,
    ) -> Result<Response<GossipGraphChunkResponse>, Status> {
        let chunk = request.into_inner();
        let uuid = chunk.collector_uuid;
        let data = chunk
            .data
            .ok_or_else(|| Status::invalid_argument("data is required"))?;

        let (msg_type, count) = match data {
            Data::Nodes(batch) => {
                let nodes = util::try_convert_vec(batch.nodes).or_invalid_argument()?;
                let count = nodes.len();
                self.collector_manager
                    .push_gossip_nodes(uuid.clone(), nodes)
                    .or_internal()?;
                ("nodes", count)
            }
            Data::Channels(batch) => {
                let channels = util::try_convert_vec(batch.channels).or_invalid_argument()?;
                let count = channels.len();
                self.collector_manager
                    .push_gossip_channels(uuid.clone(), channels)
                    .or_internal()?;
                ("channels", count)
            }
        };
        debug!(uuid, msg_type, count, "Received gossip graph chunk");
        Ok(Response::new(GossipGraphChunkResponse {}))
    }
}

// Send the eligible peers, then set the target peer count.
pub async fn collector_registration_reply(
    collector_socket: String,
    stats: CommunityStats,
    members: Arc<Vec<NodeAnnotatedRecord>>,
) -> anyhow::Result<()> {
    let mut client = CollectorClient::connect(&collector_socket).await?;
    // Shuffle so we don't sample peers in the same order between collector
    // restarts. Convert the shared records straight to domain peers; records
    // with an unparseable pubkey or address (e.g. OnionV2) are omitted.
    let mut member_refs: Vec<&NodeAnnotatedRecord> =
        members.iter().filter(|m| m.has_sockets()).collect();
    member_refs.shuffle(&mut StdRng::from_os_rng());
    let eligible_peers: Vec<observer_common::types::PeerConnectionInfo> =
        util::try_convert_vec_permissive(member_refs);
    let peer_count = eligible_peers.len();
    client.send_eligible_peers(eligible_peers).await?;
    info!("Controller: sent {} peers to collector", peer_count);

    // Allow the collector to update, then set the appropriate target peer count.
    sleep(Duration::from_millis(250)).await;
    client.set_target_peer_count(stats.connection_count).await?;
    info!(
        "Controller: set target peer count to {}",
        stats.connection_count
    );
    Ok(())
}

pub fn create_service(
    collector_manager: CollectorManagerHandle,
    stop_token: CancellationToken,
    collector_clients: CollectorClientCache,
) -> controllerrpc::controller_service_server::ControllerServiceServer<ControllerServiceImpl> {
    let server = controllerrpc::controller_service_server::ControllerServiceServer::new(
        ControllerServiceImpl::new(collector_manager, stop_token, collector_clients),
    );
    server
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(observer_common::MAX_RECV_MSG_SIZE)
}
