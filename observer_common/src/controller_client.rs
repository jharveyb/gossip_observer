use tonic::Request;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tracing::{debug, info};

use crate::common;
use crate::common::gossip_graph_chunk::Data;
use crate::controllerrpc::controller_service_client::ControllerServiceClient;
use crate::types as observer_types;
use crate::util;

#[derive(Debug, Clone)]
pub struct ControllerClient {
    pub client: ControllerServiceClient<Channel>,
    pub endpoint: String,
}

impl ControllerClient {
    pub async fn connect(endpoint: &str) -> Result<Self, tonic::transport::Error> {
        debug!(endpoint, "Connecting to controller");
        let initial_client = ControllerServiceClient::connect(endpoint.to_string()).await?;
        debug!(endpoint, "Connected to controller");
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

    pub async fn register(&mut self, info: observer_types::CollectorInfo) -> anyhow::Result<()> {
        let req = Request::new(info.into());
        self.client.register_collector(req).await?;
        Ok(())
    }

    pub async fn send_status(&mut self, info: observer_types::CollectorInfo) -> anyhow::Result<()> {
        let req = Request::new(info.into());
        self.client.collector_status(req).await?;
        Ok(())
    }

    pub async fn send_gossip_graph(
        &mut self,
        collector_uuid: &str,
        nodes: Vec<observer_types::GossipNodeInfo>,
        channels: Vec<observer_types::GossipChannelInfo>,
    ) -> anyhow::Result<()> {
        let chunk_size = 2000;

        for chunk in nodes.chunks(chunk_size) {
            let req = Request::new(common::GossipGraphChunk {
                collector_uuid: collector_uuid.to_string(),
                data: Some(Data::Nodes(common::GossipNodeInfoBatch {
                    nodes: util::convert_vec(chunk.to_vec()),
                })),
            });
            self.client.post_gossip_graph_chunk(req).await?;
        }

        for chunk in channels.chunks(chunk_size) {
            let req = Request::new(common::GossipGraphChunk {
                collector_uuid: collector_uuid.to_string(),
                data: Some(Data::Channels(common::GossipChannelInfoBatch {
                    channels: util::convert_vec(chunk.to_vec()),
                })),
            });
            self.client.post_gossip_graph_chunk(req).await?;
        }

        info!(
            total_nodes = nodes.len(),
            total_channels = channels.len(),
            "Sent gossip graph to controller"
        );
        Ok(())
    }
}
