//! bsv-low #670 (the lens's HIGH-3): the auth door's store-fault 503 on the
//! REAL path. wasm32 only, under Node (`wasm-bindgen-test-runner`), where a
//! `worker::Request` exists: the middleware's own `process_auth_with_storage`
//! runs over a store whose every call fails (only the store is faked), its
//! `Err` is rendered by the door's own `auth_fault_reply` and stamped by the
//! door's CORS, and the HIGH-1 guard runs on real requests.
//!
//! ```sh
//! cd workers && CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=<wasm-bindgen-test-runner 0.2.126> \
//!   cargo test -p low-app-layer --target wasm32-unknown-unknown --test door_wasm
//! ```
#![cfg(target_arch = "wasm32")]

use bsv_middleware_cloudflare::types::StoredSession;
use bsv_middleware_cloudflare::{
    process_auth_with_storage, AuthCloudflareError, AuthMiddlewareOptions, SessionStorage,
};
use low_app_layer::auth::{auth_fault_class, auth_fault_reply, auth_input_guard, AuthFaultClass};
use wasm_bindgen_test::wasm_bindgen_test;
use worker::{Headers, Method, Request, RequestInit, Response};

/// Writes fail the way Cloudflare's KV failed on 2026-10-15; reads the way the
/// DO fetch did, platform reference id and all.
struct FailingStore;

fn kv(text: &str) -> AuthCloudflareError {
    AuthCloudflareError::KvError(text.into())
}
const KV_PUT_500: &str = "js error: KV PUT failed: 500 Internal Server Error";
const DO_FETCH_FAULT: &str =
    "auth-session-store fetch: Error: internal error; reference = dc291hl7example";

#[async_trait::async_trait(?Send)]
impl SessionStorage for FailingStore {
    async fn get_session(
        &self,
        _: &str,
    ) -> bsv_middleware_cloudflare::Result<Option<StoredSession>> {
        Err(kv(DO_FETCH_FAULT))
    }
    async fn get_session_by_identity(
        &self,
        k: &str,
    ) -> bsv_middleware_cloudflare::Result<Option<StoredSession>> {
        self.get_session(k).await
    }
    async fn save_session(&self, _: &StoredSession) -> bsv_middleware_cloudflare::Result<()> {
        Err(kv(KV_PUT_500))
    }
    async fn update_session(&self, s: &StoredSession) -> bsv_middleware_cloudflare::Result<()> {
        self.save_session(s).await
    }
    async fn try_consume_nonce(
        &self,
        _: &str,
        _: &str,
        _: Option<u64>,
    ) -> bsv_middleware_cloudflare::Result<bool> {
        Err(kv(KV_PUT_500))
    }
    async fn release_nonce(&self, _: &str, _: &str) -> bsv_middleware_cloudflare::Result<()> {
        Err(kv(KV_PUT_500))
    }
}

fn opts() -> AuthMiddlewareOptions {
    AuthMiddlewareOptions {
        server_private_key: format!("{:064x}", 1),
        allow_unauthenticated: false,
        session_ttl_seconds: 3600,
        ..Default::default()
    }
}

fn request(path: &str, headers: &[(&str, String)], body: Option<&[u8]>) -> Request {
    let h = Headers::new();
    for (k, v) in headers {
        h.set(k, v).unwrap();
    }
    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_headers(h);
    if let Some(b) = body {
        init.with_body(Some(worker::js_sys::Uint8Array::from(b).into()));
    }
    Request::new_with_init(&format!("https://app-layer.test{path}"), &init).unwrap()
}

fn initial_request() -> Vec<u8> {
    let key = bsv_rs::primitives::PrivateKey::from_hex(&format!("{:064x}", 2)).unwrap();
    let mut m =
        bsv_rs::auth::AuthMessage::new(bsv_rs::auth::MessageType::InitialRequest, key.public_key());
    m.initial_nonce = Some(bsv_middleware_cloudflare::core::auth::generate_random_nonce());
    m.requested_certificates = Some(Default::default());
    serde_json::to_vec(&m).unwrap()
}

/// The reply as `serve` sends it: the door's reply, then the door's CORS.
fn as_served(e: &AuthCloudflareError) -> Response {
    let mut resp = auth_fault_reply(e).unwrap();
    low_app_layer::cors::add_cors_headers(&mut resp);
    resp
}

async fn assert_the_503(mut resp: Response) {
    assert_eq!(resp.status_code(), 503);
    let h = resp.headers();
    assert_eq!(h.get("Retry-After").unwrap().as_deref(), Some("2"));
    assert_eq!(
        h.get("Content-Type").unwrap().as_deref(),
        Some("application/json")
    );
    assert_eq!(h.get("Cache-Control").unwrap().as_deref(), Some("no-store"));
    assert_eq!(
        h.get("Access-Control-Allow-Origin").unwrap().as_deref(),
        Some("*")
    );
    let expose = h.get("Access-Control-Expose-Headers").unwrap().unwrap();
    assert!(expose.split(", ").any(|n| n == "Retry-After"), "{expose}");
    let text = resp.text().await.unwrap();
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["code"], "ERR_AUTH_UNAVAILABLE");
    assert!(!text.contains("reference"), "{text}");
}

#[wasm_bindgen_test]
async fn s670_g_a_store_fault_through_the_middleware_is_the_doors_503() {
    // The handshake: the middleware's InitialRequest handler saves the
    // session, the store fails, the door answers.
    let hs = request(
        "/.well-known/auth",
        &[("content-type", "application/json".into())],
        Some(&initial_request()),
    );
    let Err(e) = process_auth_with_storage(hs, &FailingStore, &opts()).await else {
        panic!("the handshake must fault at the store");
    };
    assert_eq!(auth_fault_class(&e), AuthFaultClass::Transport, "{e}");
    assert_the_503(as_served(&e)).await;
    // A general message: its session lookup fails at the store.
    let key = bsv_rs::primitives::PrivateKey::from_hex(&format!("{:064x}", 2)).unwrap();
    let nonce = bsv_middleware_cloudflare::core::auth::generate_random_nonce;
    let general = request(
        "/owed",
        &[
            ("x-bsv-auth-version", "0.1".into()),
            ("x-bsv-auth-identity-key", key.public_key().to_hex()),
            ("x-bsv-auth-message-type", "general".into()),
            ("x-bsv-auth-nonce", nonce()),
            ("x-bsv-auth-your-nonce", nonce()),
            ("x-bsv-auth-request-id", nonce()),
            ("x-bsv-auth-signature", "00".into()),
        ],
        Some(b"{}"),
    );
    let Err(e) = process_auth_with_storage(general, &FailingStore, &opts()).await else {
        panic!("the session lookup must fault at the store");
    };
    assert_eq!(auth_fault_class(&e), AuthFaultClass::Transport, "{e}");
    assert_the_503(as_served(&e)).await;
}

#[wasm_bindgen_test]
async fn s670_g_the_guard_on_real_requests() {
    // A 600-byte nonce: the caller's 400, the middleware never asked.
    let long = request(
        "/owed",
        &[("x-bsv-auth-your-nonce", "A".repeat(600))],
        Some(b"{}"),
    );
    let Err(mut resp) = auth_input_guard(long).await.unwrap() else {
        panic!("a 600-byte nonce must not reach the middleware");
    };
    assert_eq!(resp.status_code(), 400);
    assert_eq!(resp.headers().get("Retry-After").unwrap(), None);
    assert_eq!(
        resp.text().await.unwrap(),
        r#"{"error":"authentication handshake failed: x-bsv-auth-your-nonce over 128 bytes"}"#
    );
    // A 17 KiB handshake body: the 400.
    let big = request("/.well-known/auth", &[], Some(&vec![b' '; 17 * 1024]));
    let Err(mut resp) = auth_input_guard(big).await.unwrap() else {
        panic!("a 17 KiB handshake body must not reach the middleware");
    };
    assert_eq!(resp.status_code(), 400);
    assert_eq!(
        resp.text().await.unwrap(),
        r#"{"error":"authentication handshake failed: handshake body over 16384 bytes"}"#
    );
    // A real handshake passes, re-built with the same method, URL, headers
    // and bytes.
    let real = initial_request();
    let hs = request(
        "/.well-known/auth",
        &[("content-type", "application/json".into())],
        Some(&real),
    );
    let Ok(mut passed) = auth_input_guard(hs).await.unwrap() else {
        panic!("a real handshake must pass");
    };
    assert_eq!(passed.method(), Method::Post);
    assert_eq!(
        passed.url().unwrap().as_str(),
        "https://app-layer.test/.well-known/auth"
    );
    assert_eq!(
        passed.headers().get("content-type").unwrap().as_deref(),
        Some("application/json")
    );
    assert_eq!(passed.bytes().await.unwrap(), real);
}
