use ldk_node::PeerDetails;
use tonic::Request;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tracing::debug;

use crate::collectorrpc;
use crate::collectorrpc::collector_service_client::CollectorServiceClient;
use crate::common;
use crate::types as observer_types;
use crate::util;

/// A wrapper around the gRPC client for communicating with a collector.
#[derive(Debug, Clone)]
pub struct CollectorClient {
    pub client: CollectorServiceClient<Channel>,
    pub endpoint: String,
}

impl CollectorClient {
    // TODO: should we enable lazy connection, keepalives, etc.?
    // Or just recreate clients for each call
    pub async fn connect(endpoint: &str) -> Result<Self, tonic::transport::Error> {
        debug!(endpoint, "Connecting to collector");
        let initial_client = CollectorServiceClient::connect(endpoint.to_string()).await?;
        debug!(endpoint, "Connected to collector");
        let client = initial_client
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(crate::MAX_RECV_MSG_SIZE);
        Ok(Self {
            client,
            endpoint: endpoint.to_string(),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn send_eligible_peers(
        &mut self,
        peers: &[observer_types::PeerConnectionInfo],
    ) -> anyhow::Result<()> {
        let peers = peers
            .iter()
            .map(|p| common::PeerConnectionInfo::from(p.clone()))
            .collect::<Vec<_>>();

        let chunk_size = 256;
        let mut current_msg = Vec::with_capacity(chunk_size);
        for (idx, peer) in peers.into_iter().enumerate() {
            current_msg.push(peer);
            if idx % chunk_size == 0 {
                let req = Request::new(collectorrpc::EligiblePeersRequest { peers: current_msg });
                self.client.post_eligible_peers(req).await?;
                current_msg = Vec::with_capacity(chunk_size);
            }
        }

        let req = Request::new(collectorrpc::EligiblePeersRequest { peers: current_msg });
        self.client.post_eligible_peers(req).await?;
        Ok(())
    }

    pub async fn set_target_peer_count(&mut self, target: u32) -> anyhow::Result<()> {
        let req = Request::new(collectorrpc::TargetPeerCountRequest { target });
        self.client.post_target_peer_count(req).await?;
        Ok(())
    }

    pub async fn get_current_peers(&mut self) -> anyhow::Result<Vec<PeerDetails>> {
        let req = Request::new(collectorrpc::CurrentPeersRequest {});
        let resp = self.client.get_current_peers(req).await?;
        util::try_convert_vec(resp.into_inner().peers)
    }

    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        let req = Request::new(common::ShutdownRequest {});
        self.client.shutdown(req).await?;
        Ok(())
    }
}
