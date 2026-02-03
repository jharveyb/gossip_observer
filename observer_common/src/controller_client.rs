use tonic::Request;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tracing::info;

use crate::controllerrpc::controller_service_client::ControllerServiceClient;
use crate::types as observer_types;

#[derive(Debug, Clone)]
pub struct ControllerClient {
    pub client: ControllerServiceClient<Channel>,
    pub endpoint: String,
}

impl ControllerClient {
    pub async fn connect(endpoint: &str) -> anyhow::Result<Self> {
        info!(endpoint, "Connecting to controller");
        let initial_client = ControllerServiceClient::connect(endpoint.to_string()).await?;
        info!(endpoint, "Connected to controller");
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
}
