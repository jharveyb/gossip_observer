use observer_common::collector_client::CollectorClient;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status};

use observer_common::common::{CollectorInfo, ShutdownRequest, ShutdownResponse};
use observer_common::controllerrpc::{
    CollectorStatusResponse, OpenCollectorChannelRequest, OpenCollectorChannelResponse,
    RegisterCollectorResponse, StatusRequest, StatusResponse, UpdateChannelsRequest,
    UpdateChannelsResponse,
};
use observer_common::{controllerrpc, util};
use tracing::info;

use crate::CommunityStats;
use crate::collector_manager::{CollectorManagerHandle, compute_status, handle_collector_info};
use crate::csv_reader::NodeAnnotatedRecord;

pub struct ControllerServiceImpl {
    stop_token: CancellationToken,
    collector_manager: CollectorManagerHandle,
}

impl ControllerServiceImpl {
    pub fn new(collector_manager: CollectorManagerHandle, stop_token: CancellationToken) -> Self {
        Self {
            collector_manager,
            stop_token,
        }
    }

    pub async fn build_collector_client(
        &self,
        collector_uuid: &str,
    ) -> Result<CollectorClient, Status> {
        // Look up collector by UUID
        let collector = self
            .collector_manager
            .get_collector_by_uuid(collector_uuid)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let collector = collector.ok_or_else(|| {
            Status::not_found(format!("Collector {} not found or offline", collector_uuid))
        })?;

        // Connect to collector's gRPC endpoint
        let client = CollectorClient::connect(&collector.info.grpc_socket)
            .await
            .map_err(|e| {
                Status::unavailable(format!(
                    "Failed to connect to collector {}: {}",
                    collector_uuid, e
                ))
            })?;

        Ok(client)
    }
}

#[tonic::async_trait]
impl controllerrpc::controller_service_server::ControllerService for ControllerServiceImpl {
    async fn register_collector(
        &self,
        request: tonic::Request<CollectorInfo>,
    ) -> Result<tonic::Response<RegisterCollectorResponse>, Status> {
        let info: observer_common::types::CollectorInfo = request
            .into_inner()
            .try_into()
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        let collector_socket = info.grpc_socket.clone();
        if let Some((stats, members)) = handle_collector_info(&self.collector_manager, info, true)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            tokio::spawn(collector_registration_reply(
                collector_socket,
                stats,
                members,
            ))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::internal(e.to_string()))?;
        } else {
            return Err(Status::internal("Collector mapping not found"));
        }

        Ok(Response::new(RegisterCollectorResponse {}))
    }

    async fn collector_status(
        &self,
        request: tonic::Request<CollectorInfo>,
    ) -> Result<tonic::Response<CollectorStatusResponse>, Status> {
        let info: observer_common::types::CollectorInfo = request
            .into_inner()
            .try_into()
            .map_err(|e: anyhow::Error| Status::internal(e.to_string()))?;
        // Collectors should switch to this endpoint once they received their assignment.
        // So we don't need to resend the eligible peer list.
        let collector_socket = info.grpc_socket.clone();
        if let Some((stats, members)) = handle_collector_info(&self.collector_manager, info, false)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
        {
            // If we're in this branch, something is off. A new collector should be
            // calling register(). We should serve the collector assignment anyways.
            tokio::spawn(collector_registration_reply(
                collector_socket,
                stats,
                members,
            ))
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::internal(e.to_string()))?;
        } else {
            // We have logging inside handle_collector_info().
            // TODO: Anything to do here?
        }
        Ok(Response::new(CollectorStatusResponse {}))
    }

    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<StatusResponse>, Status> {
        let status = compute_status(&self.collector_manager)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
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
        let req = request.into_inner();
        let collector_uuid = req.uuid.clone();

        // Look up collector by UUID
        let collector = self
            .collector_manager
            .get_collector_by_uuid(&collector_uuid)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let collector = collector.ok_or_else(|| {
            Status::not_found(format!("Collector {} not found or offline", collector_uuid))
        })?;

        // Connect to collector's gRPC endpoint
        let mut client = CollectorClient::connect(&collector.info.grpc_socket)
            .await
            .map_err(|e| {
                Status::unavailable(format!(
                    "Failed to connect to collector {}: {}",
                    collector_uuid, e
                ))
            })?;

        // Convert inner request to OpenChannelCommand and forward to collector
        let inner_req = req
            .request
            .ok_or_else(|| Status::invalid_argument("request is required"))?;
        let cmd = inner_req
            .try_into()
            .map_err(|e: anyhow::Error| Status::invalid_argument(e.to_string()))?;

        let channel_id = client
            .open_channel(cmd)
            .await
            .map_err(|e| Status::internal(format!("Failed to open channel: {}", e)))?;

        info!(
            collector_uuid = %collector_uuid,
            "Controller: opened channel via collector"
        );

        Ok(Response::new(OpenCollectorChannelResponse {
            uuid: collector_uuid,
            response: Some(observer_common::common::OpenChannelResponse {
                local_channel_id: channel_id,
            }),
        }))
    }

    async fn update_channels(
        &self,
        request: Request<UpdateChannelsRequest>,
    ) -> Result<Response<UpdateChannelsResponse>, Status> {
        let req = request.into_inner();
        let collector_uuid = req.uuid.clone();

        let mut client = self.build_collector_client(&collector_uuid).await?;
        let scids = client
            .update_channel_cfgs()
            .await
            .map_err(|e| Status::internal(format!("Failed to update channels: {}", e)))?;
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
}

// Send the eligible peers, then set the target peer count.
pub async fn collector_registration_reply(
    collector_socket: String,
    stats: CommunityStats,
    members: Vec<NodeAnnotatedRecord>,
) -> anyhow::Result<()> {
    let mut client = CollectorClient::connect(&collector_socket).await?;
    // Convert our 'records' to the protobuf type, then to the strongly-typed
    // PeerConnectionInfo type.
    let members_with_sockets = members
        .into_iter()
        .filter(|m| m.has_sockets())
        .collect::<Vec<NodeAnnotatedRecord>>();
    let peer_list: Vec<observer_common::common::PeerConnectionInfo> =
        util::try_convert_vec(members_with_sockets)?;
    // Don't fail on OnionV2 addresses, just omit them.
    let eligible_peers = util::try_convert_vec_permissive(peer_list);
    client.send_eligible_peers(&eligible_peers).await?;
    info!(
        "Controller: sent {} peer to collector",
        eligible_peers.len()
    );

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
) -> controllerrpc::controller_service_server::ControllerServiceServer<ControllerServiceImpl> {
    let server = controllerrpc::controller_service_server::ControllerServiceServer::new(
        ControllerServiceImpl::new(collector_manager, stop_token),
    );
    server
        .accept_compressed(CompressionEncoding::Zstd)
        .send_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(observer_common::MAX_RECV_MSG_SIZE)
}
