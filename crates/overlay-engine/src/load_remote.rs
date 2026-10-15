//! Count requests when dispatched, including those whose future is cancelled.
use crate::{
    gasp::{GASPError, GASPRemote},
    types::*,
};
use async_trait::async_trait;
pub(crate) struct MeteredRemote(pub Box<dyn GASPRemote>);
#[async_trait(?Send)]
impl GASPRemote for MeteredRemote {
    async fn get_initial_response(
        &self,
        request: &GASPInitialRequest,
    ) -> Result<GASPInitialResponse, GASPError> {
        crate::meter::peer_request();
        self.0.get_initial_response(request).await
    }
    async fn get_initial_reply(
        &self,
        response: &GASPInitialResponse,
    ) -> Result<GASPInitialReply, GASPError> {
        crate::meter::peer_request();
        self.0.get_initial_reply(response).await
    }
    async fn request_node(
        &self,
        graph_id: &str,
        txid: &str,
        output_index: u32,
        metadata: bool,
    ) -> Result<GASPNode, GASPError> {
        crate::meter::peer_request();
        self.0
            .request_node(graph_id, txid, output_index, metadata)
            .await
    }
    async fn submit_node(&self, node: &GASPNode) -> Result<Option<GASPNodeResponse>, GASPError> {
        crate::meter::peer_request();
        self.0.submit_node(node).await
    }
    fn batches(&self) -> bool {
        self.0.batches()
    }
    async fn request_nodes(
        &self,
        outpoints: &[crate::node_batch::BatchOutpoint],
    ) -> Result<crate::node_batch::NodeBatch, GASPError> {
        crate::meter::batch_request();
        self.0.request_nodes(outpoints).await
    }
}
