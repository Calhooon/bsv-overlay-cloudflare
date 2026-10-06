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

/// How a call to a peer failed, before it is given a `GASPError` class.
#[derive(Debug)]
enum PeerFailure {
    /// The request could not be built on our side.
    Local(String),
    /// The request could not be built for, or did not reach, the peer.
    Transport(String),
    /// The peer answered outside 2xx.
    Status {
        url: String,
        status: u16,
        body: String,
    },
    /// The peer answered 2xx with a body that is not the expected JSON.
    BadBody(String),
}

impl PeerFailure {
    /// The class of a failure that says nothing about what the peer holds.
    fn into_fault(self) -> GASPError {
        match self {
            PeerFailure::Local(message) => GASPError::Other(message),
            PeerFailure::Transport(message) | PeerFailure::BadBody(message) => {
                GASPError::RemoteError(message)
            }
            PeerFailure::Status { url, status, body } => {
                GASPError::RemoteError(format!("Peer {url} returned HTTP {status}: {body}"))
            }
        }
    }

    /// The class of a failed `/requestForeignGASPNode` REQUEST for a node.
    ///
    /// HTTP 400, and only 400, is the DEFINITE answer "I do not hold that
    /// outpoint" (`GASPError::NodeNotFound`): the reference peer answers 400
    /// with a masked body for every throw of `provideForeignGASPNode`
    /// (`overlay-express/src/OverlayExpress.ts:2208-2219`), and our own route
    /// answers 400 for `EngineError::NodeNotFound`. The engine prunes a
    /// manager-named input on that class and on no other (the D8 decoy rule,
    /// `GASPSync::process_incoming_node`). Everything else (the request did
    /// not reach the peer, a 5xx, a 429, any other status, a body that does
    /// not parse) is a fault of the moment: `RemoteError`, the UTXO fails
    /// and the next sync asks again.
    fn into_node_request_error(self) -> GASPError {
        match self {
            PeerFailure::Status {
                url,
                status: 400,
                body,
            } => GASPError::NodeNotFound(format!("Peer {url} returned HTTP 400: {body}")),
            other => other.into_fault(),
        }
    }
}

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
    ) -> Result<T, PeerFailure> {
        let url = format!("{}{}", self.peer_url, path);

        let body_json =
            serde_json::to_string(body).map_err(|e| PeerFailure::Local(e.to_string()))?;

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
            PeerFailure::Transport(format!("Failed to create request to {url}: {e}"))
        })?;

        let mut response = worker::Fetch::Request(request)
            .send()
            .await
            .map_err(|e| PeerFailure::Transport(format!("Fetch to {url} failed: {e}")))?;

        let status = response.status_code();
        if !(200..300).contains(&status) {
            let body_text = response
                .text()
                .await
                .unwrap_or_else(|_| "(no body)".to_string());
            return Err(PeerFailure::Status {
                url,
                status,
                body: body_text,
            });
        }

        response
            .json()
            .await
            .map_err(|e| PeerFailure::BadBody(format!("Failed to parse response from {url}: {e}")))
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
        self.post_json("/requestSyncResponse", request)
            .await
            .map_err(PeerFailure::into_fault)
    }

    /// Send our initial response and get the peer's reply.
    ///
    /// POST to `{peer_url}/requestSyncResponse` with the response body.
    /// The peer returns a `GASPInitialReply` containing UTXOs we should push.
    async fn get_initial_reply(
        &self,
        response: &GASPInitialResponse,
    ) -> Result<GASPInitialReply, GASPError> {
        self.post_json("/requestSyncResponse", response)
            .await
            .map_err(PeerFailure::into_fault)
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
        .map_err(PeerFailure::into_node_request_error)
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

#[cfg(test)]
mod tests {
    use super::PeerFailure;
    use overlay_engine::gasp::GASPError;

    fn status(status: u16) -> PeerFailure {
        PeerFailure::Status {
            url: "https://peer.example/requestForeignGASPNode".into(),
            status,
            body: r#"{"status":"error","message":"Request could not be processed"}"#.into(),
        }
    }

    /// bsv-low #530 (D8, lens fold): the engine prunes a manager-named input
    /// on `NodeNotFound` alone, so this mapping decides what is a decoy and
    /// what is a fault of the moment. 400 is the one definite answer.
    #[test]
    fn a_node_request_is_definitely_refused_by_http_400_and_by_nothing_else() {
        let definite = status(400).into_node_request_error();
        assert!(
            matches!(&definite, GASPError::NodeNotFound(m) if m.contains("HTTP 400")),
            "{definite:?}"
        );

        let faults = [
            status(500),
            status(502),
            status(503),
            status(429),
            status(404),
            status(401),
            status(408),
            PeerFailure::Transport("Fetch to https://peer.example failed: timeout".into()),
            PeerFailure::BadBody("Failed to parse response: expected value".into()),
        ];
        for failure in faults {
            let shown = format!("{failure:?}");
            let class = failure.into_node_request_error();
            assert!(
                matches!(class, GASPError::RemoteError(_)),
                "{shown} -> {class:?}"
            );
        }
        assert!(matches!(
            PeerFailure::Local("serialize".into()).into_node_request_error(),
            GASPError::Other(_)
        ));
    }

    /// A 400 from any OTHER call (the initial request of a sync) is not an
    /// answer about a node: it stays a fault and fails the peer's sync.
    #[test]
    fn a_400_outside_a_node_request_is_a_fault() {
        assert!(matches!(
            status(400).into_fault(),
            GASPError::RemoteError(_)
        ));
        let shown = status(500).into_fault().to_string();
        assert!(shown.contains("returned HTTP 500"), "{shown}");
    }
}
