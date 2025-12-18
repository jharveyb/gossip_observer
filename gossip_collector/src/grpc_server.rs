use observer_common::types::LdkNodeConfig;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use observer_proto::collector as CollectorRPC;

pub struct CollectorServiceImpl {
    node: Arc<ldk_node::Node>,
}

impl CollectorServiceImpl {
    pub fn new(node: Arc<ldk_node::Node>) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl CollectorRPC::collector_service_server::CollectorService for CollectorServiceImpl {
    async fn get_node_config(
        &self,
        _request: Request<CollectorRPC::NodeConfigRequest>,
    ) -> Result<Response<CollectorRPC::NodeConfigResponse>, Status> {
        let cfg = self.node.config();
        let node_id = self.node.node_id();

        let resp_cfg = LdkNodeConfig {
            listening_addresses: cfg.listening_addresses,
            node_alias: cfg.node_alias,
            node_id,
        };

        Ok(Response::new(resp_cfg.into()))
    }
}

pub fn create_service(
    node: Arc<ldk_node::Node>,
) -> CollectorRPC::collector_service_server::CollectorServiceServer<CollectorServiceImpl> {
    CollectorRPC::collector_service_server::CollectorServiceServer::new(CollectorServiceImpl::new(
        node,
    ))
}
