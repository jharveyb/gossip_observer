use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tracing::{debug, info};

use crate::common;
use crate::common::gossip_graph_chunk::Data;
use crate::controllerrpc::controller_service_client::ControllerServiceClient;
use crate::types as observer_types;

#[derive(Debug, Clone)]
pub struct ControllerClient {
    pub client: ControllerServiceClient<Channel>,
    pub endpoint: String,
}

impl ControllerClient {
    pub async fn connect(endpoint: &str) -> Result<Self, tonic::transport::Error> {
        debug!(endpoint, "Connecting to controller");
        let channel = crate::connect_channel(endpoint).await?;
        debug!(endpoint, "Connected to controller");
        let client = ControllerServiceClient::new(channel)
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

    pub async fn register(&mut self, info: observer_types::CollectorInfo) -> anyhow::Result<()> {
        self.client
            .register_collector(common::CollectorInfo::from(info))
            .await?;
        Ok(())
    }

    pub async fn send_status(&mut self, info: observer_types::CollectorInfo) -> anyhow::Result<()> {
        self.client
            .collector_status(common::CollectorInfo::from(info))
            .await?;
        Ok(())
    }

    async fn send_graph_chunk(&mut self, collector_uuid: &str, data: Data) -> anyhow::Result<()> {
        self.client
            .post_gossip_graph_chunk(common::GossipGraphChunk {
                collector_uuid: collector_uuid.to_string(),
                data: Some(data),
            })
            .await?;
        Ok(())
    }

    pub async fn send_gossip_graph(
        &mut self,
        collector_uuid: &str,
        nodes: Vec<observer_types::GossipNodeInfo>,
        channels: Vec<observer_types::GossipChannelInfo>,
    ) -> anyhow::Result<()> {
        const CHUNK_SIZE: usize = 1024;
        let (total_nodes, total_channels) = (nodes.len(), channels.len());

        let mut nodes = nodes.into_iter();
        loop {
            let batch: Vec<common::GossipNodeInfo> =
                nodes.by_ref().take(CHUNK_SIZE).map(Into::into).collect();
            if batch.is_empty() {
                break;
            }
            let data = Data::Nodes(common::GossipNodeInfoBatch { nodes: batch });
            self.send_graph_chunk(collector_uuid, data).await?;
        }

        let mut channels = channels.into_iter();
        loop {
            let batch: Vec<common::GossipChannelInfo> =
                channels.by_ref().take(CHUNK_SIZE).map(Into::into).collect();
            if batch.is_empty() {
                break;
            }
            let data = Data::Channels(common::GossipChannelInfoBatch { channels: batch });
            self.send_graph_chunk(collector_uuid, data).await?;
        }

        info!(
            total_nodes,
            total_channels, "Sent gossip graph to controller"
        );
        Ok(())
    }
}
