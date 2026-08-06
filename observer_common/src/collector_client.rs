use ldk_node::PeerDetails;
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
    pub async fn connect(endpoint: &str) -> Result<Self, tonic::transport::Error> {
        debug!(endpoint, "Connecting to collector");
        let channel = crate::connect_channel(endpoint).await?;
        debug!(endpoint, "Connected to collector");
        let client = CollectorServiceClient::new(channel)
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
        peers: Vec<observer_types::PeerConnectionInfo>,
    ) -> anyhow::Result<()> {
        const CHUNK_SIZE: usize = 256;

        let mut peers = peers.into_iter();
        loop {
            let batch: Vec<common::PeerConnectionInfo> =
                peers.by_ref().take(CHUNK_SIZE).map(Into::into).collect();
            if batch.is_empty() {
                break;
            }
            self.client
                .post_eligible_peers(collectorrpc::EligiblePeersRequest { peers: batch })
                .await?;
        }
        Ok(())
    }

    pub async fn set_target_peer_count(&mut self, target: u32) -> anyhow::Result<()> {
        self.client
            .post_target_peer_count(collectorrpc::TargetPeerCountRequest { target })
            .await?;
        Ok(())
    }

    pub async fn get_current_peers(&mut self) -> anyhow::Result<Vec<PeerDetails>> {
        let resp = self
            .client
            .get_current_peers(collectorrpc::CurrentPeersRequest {})
            .await?;
        util::try_convert_vec(resp.into_inner().peers)
    }

    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        self.client.shutdown(common::ShutdownRequest {}).await?;
        Ok(())
    }

    pub async fn get_balances(&mut self) -> anyhow::Result<observer_types::Balances> {
        let resp = self.client.balances(common::BalancesRequest {}).await?;
        Ok(resp.into_inner().into())
    }

    pub async fn open_channel(
        &mut self,
        cmd: observer_types::OpenChannelCommand,
    ) -> anyhow::Result<Vec<u8>> {
        let resp = self
            .client
            .open_channel(common::OpenChannelRequest::from(cmd))
            .await?;
        Ok(resp.into_inner().local_channel_id.to_vec())
    }

    pub async fn update_channel_cfgs(&mut self) -> anyhow::Result<Vec<u64>> {
        let resp = self
            .client
            .update_channel_config(common::UpdateChannelConfigRequest {})
            .await?;
        Ok(resp.into_inner().scids)
    }
}
