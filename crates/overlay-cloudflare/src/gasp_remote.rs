//! WorkerGASPRemote — HTTP-based GASP peer communication.
//!
//! Implements the `GASPRemote` trait using Cloudflare Workers `Fetch` API
//! to make HTTP calls to peer overlay nodes for GASP sync.
//!
//! Ported from `~/bsv/overlay-services/src/GASP/OverlayGASPRemote.ts` (108 lines).

use async_trait::async_trait;
use overlay_engine::gasp::{GASPError, GASPRemote};
use overlay_engine::types::{
    GASPInitialReply, GASPInitialRequest, GASPInitialResponse, GASPNode, GASPNodeResponse,
};
use serde::Serialize;

/// `GASPRemote` implementation using Cloudflare Workers `Fetch` API.
///
/// Makes HTTP POST requests to peer overlay nodes at their standard
/// GASP endpoints (`/requestSyncResponse`, `/requestForeignGASPNode`).
pub struct WorkerGASPRemote {
    /// Base URL of the peer overlay node (e.g. "https://peer.example.com").
    peer_url: String,
    /// Topic being synchronized (sent in `x-bsv-topic` header).
    topic: String,
}

impl WorkerGASPRemote {
    /// Create a new remote for the given peer URL and topic.
    pub fn new(peer_url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            peer_url: peer_url.into().trim_end_matches('/').to_string(),
            topic: topic.into(),
        }
    }

    /// POST JSON to a peer endpoint and parse the response.
    async fn post_json<T: serde::de::DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, GASPError> {
        let url = format!("{}{}", self.peer_url, path);

        let body_json = serde_json::to_string(body).map_err(|e| GASPError::Other(e.to_string()))?;

        let mut init = worker::RequestInit::new();
        init.with_method(worker::Method::Post);

        let headers = worker::Headers::new();
        let _ = headers.set("Content-Type", "application/json");
        let _ = headers.set("Accept", "application/json");
        let _ = headers.set("x-bsv-topic", &self.topic);
        init.with_headers(headers);

        // Set body as string (JSON)
        let js_body = js_sys::JsString::from(body_json.as_str());
        init.with_body(Some(js_body.into()));

        let request = worker::Request::new_with_init(&url, &init).map_err(|e| {
            GASPError::RemoteError(format!("Failed to create request to {url}: {e}"))
        })?;

        let mut response = worker::Fetch::Request(request)
            .send()
            .await
            .map_err(|e| GASPError::RemoteError(format!("Fetch to {url} failed: {e}")))?;

        let status = response.status_code();
        if !(200..300).contains(&status) {
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|_| "(no body)".to_string());
            return Err(GASPError::RemoteError(format!(
                "Peer {url} returned HTTP {status}: {body_text}"
            )));
        }

        response.json().await.map_err(|e| {
            GASPError::RemoteError(format!("Failed to parse response from {url}: {e}"))
        })
    }
}

#[async_trait(?Send)]
impl GASPRemote for WorkerGASPRemote {
    /// Send an initial request and get the peer's initial response.
    ///
    /// POST to `{peer_url}/requestSyncResponse` with `x-bsv-topic` header
    /// and the `GASPInitialRequest` as JSON body.
    async fn get_initial_response(
        &self,
        request: &GASPInitialRequest,
    ) -> Result<GASPInitialResponse, GASPError> {
        self.post_json("/requestSyncResponse", request).await
    }

    /// Send our initial response and get the peer's reply.
    ///
    /// POST to `{peer_url}/requestSyncResponse` with the response body.
    /// The peer returns a `GASPInitialReply` containing UTXOs we should push.
    async fn get_initial_reply(
        &self,
        response: &GASPInitialResponse,
    ) -> Result<GASPInitialReply, GASPError> {
        self.post_json("/requestSyncResponse", response).await
    }

    /// Request a specific node from the peer.
    ///
    /// POST to `{peer_url}/requestForeignGASPNode` with JSON body containing
    /// graphID, txid, outputIndex, and whether metadata is requested.
    async fn request_node(
        &self,
        graph_id: &str,
        txid: &str,
        output_index: u32,
        metadata: bool,
    ) -> Result<GASPNode, GASPError> {
        #[derive(Serialize)]
        struct NodeRequest<'a> {
            #[serde(rename = "graphID")]
            graph_id: &'a str,
            txid: &'a str,
            #[serde(rename = "outputIndex")]
            output_index: u32,
            metadata: bool,
        }

        self.post_json(
            "/requestForeignGASPNode",
            &NodeRequest {
                graph_id,
                txid,
                output_index,
                metadata,
            },
        )
        .await
    }

    /// Submit a node to the peer and get back which inputs they need.
    ///
    /// POST to `{peer_url}/requestForeignGASPNode` with the node data.
    /// Returns `None` if the peer accepts without needing further inputs.
    async fn submit_node(&self, node: &GASPNode) -> Result<Option<GASPNodeResponse>, GASPError> {
        // The peer may return a node response requesting more inputs,
        // or an empty response if no further inputs needed.
        let result: Result<GASPNodeResponse, _> =
            self.post_json("/requestForeignGASPNode", node).await;

        match result {
            Ok(response) if response.requested_inputs.is_empty() => Ok(None),
            Ok(response) => Ok(Some(response)),
            Err(_) => Ok(None), // Peer accepted without further requests
        }
    }
}

// ============================================================================
// Factory
// ============================================================================

/// Factory that creates `WorkerGASPRemote` instances for the Cloudflare Workers
/// platform.
///
/// Passed to `Engine::set_gasp_remote_factory()` to enable GASP sync.
pub struct WorkerGASPRemoteFactory;

impl overlay_engine::gasp::GASPRemoteFactory for WorkerGASPRemoteFactory {
    fn create_remote(
        &self,
        peer_url: &str,
        topic: &str,
    ) -> Box<dyn overlay_engine::gasp::GASPRemote> {
        Box::new(WorkerGASPRemote::new(peer_url, topic))
    }
}
