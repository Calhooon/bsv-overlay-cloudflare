//! #318 — the REAL crypto round-trip for the auth verification.
//!
//! The standing rule (the bsv-low `signin.ts` lesson): a mocked verify once
//! hid a broken counterparty pairing, so the auth cells here run REAL BRC-103
//! mutual authentication with REAL random secp256k1 keys — a client `Peer`
//! and a server `Peer` from `bsv-rs` (the SDK whose auth implementation
//! `bsv-middleware-cloudflare` ports 1:1 — its `auth.rs` header: "a 1:1 port
//! of auth-express-middleware… Express delegates all protocol logic to
//! `Peer`. We implement it directly (same logic)"), wired over an in-memory
//! pair transport. The server-side signature verification is the SDK's real
//! `handle_incoming_message` path; the verified identity it emits is then fed
//! to THE seam production uses (`low_app_layer::auth::resolve_view_identity`).
//!
//! Modelling boundary (Rule 17), stated honestly: the worker's runtime path
//! runs `bsv_middleware_cloudflare::process_auth`, whose internal
//! `verify_message_signature` is private and whose `worker::Request` cannot
//! be constructed off-wasm — no native test can execute it (the same boundary
//! the low-watchtower's suite has; its full-path proof was the live AuthFetch
//! e2e). What THIS file proves with real crypto: the BRC-103 protocol legs
//! (handshake, per-message ECDSA, identity binding, tamper refusal) and the
//! seam those legs feed. The full worker path is proven at deploy time by the
//! orchestrator's staging probe (handshake + authenticated GET against the
//! deployed worker), per the lane report.

use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use bsv_rs::auth::transports::{Transport, TransportCallback};
use bsv_rs::auth::{AuthMessage, MessageType, Peer, PeerOptions};
use bsv_rs::primitives::PrivateKey;
use bsv_rs::wallet::ProtoWallet;

use low_app_layer::auth::{resolve_view_identity, AuthMode, CallerAuth, IdentityDecision};

// ── in-memory pair transport ────────────────────────────────────────────────

type CallbackSlot = Arc<RwLock<Option<Box<TransportCallback>>>>;

/// What the man-in-the-middle does to a General message in flight.
#[derive(Clone, Copy, PartialEq)]
enum Mitm {
    /// Deliver verbatim (the honest wire).
    None,
    /// Flip one payload byte AFTER signing (tamper leg).
    TamperPayload,
    /// Replace the sender identity key with the attacker's (signature
    /// unchanged) — the counterparty-pairing leg `signin.ts` was about.
    SwapIdentityKey,
}

/// One end of an in-memory wire: `send` delivers to the OTHER end's
/// registered callback (i.e. the other Peer's `handle_incoming_message`).
struct PairEnd {
    mine: CallbackSlot,
    other: CallbackSlot,
    /// Active MITM behavior for General messages sent FROM this end.
    mitm: Arc<Mutex<Mitm>>,
    /// The attacker key used by `SwapIdentityKey`.
    attacker_key: bsv_rs::primitives::PublicKey,
    /// Records any delivery error the receiving peer raised (the refusal
    /// evidence for the negative legs).
    delivery_errors: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Transport for PairEnd {
    async fn send(&self, message: &AuthMessage) -> bsv_rs::Result<()> {
        let mut message = message.clone();
        if message.message_type == MessageType::General {
            match *self.mitm.lock().unwrap() {
                Mitm::None => {}
                Mitm::TamperPayload => {
                    if let Some(p) = message.payload.as_mut() {
                        if let Some(b) = p.first_mut() {
                            *b ^= 0x01;
                        }
                    }
                }
                Mitm::SwapIdentityKey => {
                    message.identity_key = self.attacker_key.clone();
                }
            }
        }
        let fut = {
            let guard = self.other.read().unwrap();
            let cb = guard
                .as_ref()
                .expect("receiving peer must have started (set_callback)");
            cb(message)
        };
        // Deliver ASYNCHRONOUSLY (like a real network hop): a synchronous
        // in-line await would process the peer's reply before the sender has
        // registered its handshake waiter, deadlocking the exchange.
        let errors = self.delivery_errors.clone();
        tokio::spawn(async move {
            if let Err(e) = fut.await {
                errors.lock().unwrap().push(e.to_string());
            }
        });
        Ok(())
    }

    fn set_callback(&self, callback: Box<TransportCallback>) {
        *self.mine.write().unwrap() = Some(callback);
    }

    fn clear_callback(&self) {
        *self.mine.write().unwrap() = None;
    }
}

struct Wire {
    client_end: Option<PairEnd>,
    server_end: Option<PairEnd>,
    mitm: Arc<Mutex<Mitm>>,
    delivery_errors: Arc<Mutex<Vec<String>>>,
}

fn wire(attacker_key: bsv_rs::primitives::PublicKey) -> Wire {
    let a: CallbackSlot = Arc::default();
    let b: CallbackSlot = Arc::default();
    let mitm = Arc::new(Mutex::new(Mitm::None));
    let delivery_errors = Arc::new(Mutex::new(Vec::new()));
    Wire {
        client_end: Some(PairEnd {
            mine: a.clone(),
            other: b.clone(),
            mitm: mitm.clone(),
            attacker_key: attacker_key.clone(),
            delivery_errors: delivery_errors.clone(),
        }),
        server_end: Some(PairEnd {
            mine: b,
            other: a,
            mitm: mitm.clone(),
            attacker_key,
            delivery_errors: delivery_errors.clone(),
        }),
        mitm,
        delivery_errors,
    }
}

/// The received-General log on the server: (sender identity hex, payload).
type ReceivedLog = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

struct Rig {
    client: Peer<ProtoWallet, PairEnd>,
    _server: Arc<Peer<ProtoWallet, PairEnd>>,
    client_identity_hex: String,
    received: ReceivedLog,
    mitm: Arc<Mutex<Mitm>>,
    delivery_errors: Arc<Mutex<Vec<String>>>,
}

async fn rig() -> Rig {
    let client_key = PrivateKey::random();
    let server_key = PrivateKey::random();
    let attacker_key = PrivateKey::random().public_key();
    let client_identity_hex = client_key.public_key().to_hex();

    let mut w = wire(attacker_key);
    let client = Peer::new(PeerOptions {
        wallet: ProtoWallet::new(Some(client_key)),
        transport: w.client_end.take().unwrap(),
        certificates_to_request: None,
        session_manager: None,
        auto_persist_last_session: false,
        originator: Some("low-app-layer-318-test-client".into()),
    });
    let server = Arc::new(Peer::new(PeerOptions {
        wallet: ProtoWallet::new(Some(server_key)),
        transport: w.server_end.take().unwrap(),
        certificates_to_request: None,
        session_manager: None,
        auto_persist_last_session: false,
        originator: Some("low-app-layer-318-test-server".into()),
    }));
    client.start();
    // `start_server` (not `start`): the lightweight `start` callback ignores
    // incoming InitialRequest; the server side must route EVERY message type
    // through the real `handle_incoming_message` verification path.
    server.start_server();

    let received: ReceivedLog = Arc::default();
    let sink = received.clone();
    server
        .listen_for_general_messages(move |sender, payload| {
            let sink = sink.clone();
            Box::pin(async move {
                sink.lock().unwrap().push((sender.to_hex(), payload));
                Ok(())
            })
        })
        .await;

    Rig {
        client,
        _server: server,
        client_identity_hex,
        received,
        mitm: w.mitm,
        delivery_errors: w.delivery_errors,
    }
}

const TIMEOUT_MS: u64 = 10_000;

async fn send(rig: &Rig, payload: &[u8]) -> bsv_rs::Result<()> {
    tokio::time::timeout(
        std::time::Duration::from_millis(TIMEOUT_MS),
        rig.client.to_peer(payload, None, Some(TIMEOUT_MS)),
    )
    .await
    .expect("BRC-103 exchange must not hang")
}

/// Delivery is asynchronous (a spawned hop, like a real network) — poll for
/// the expected observable instead of asserting immediately, and fail LOUDLY
/// on timeout rather than passing vacuously.
async fn wait_for(cond: impl Fn() -> bool, what: &str) {
    for _ in 0..400 {
        if cond() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for {what}");
}

// ── the cells ───────────────────────────────────────────────────────────────

/// The honest round trip: REAL handshake + REAL per-message ECDSA, server
/// verifies and reports the client's REAL identity key — which then drives
/// the production seam: session identity wins, and a mismatching
/// `?identity=` claim is REFUSED (never coerced) in BOTH modes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_keys_roundtrip_feeds_the_identity_seam() {
    let rig = rig().await;
    let payload = b"GET /results (identity-scoped view)".to_vec();
    send(&rig, &payload)
        .await
        .expect("honest exchange verifies");
    let received = rig.received.clone();
    wait_for(
        move || received.lock().unwrap().len() == 1,
        "the honest General message to be verified and delivered",
    )
    .await;

    let got = rig.received.lock().unwrap().clone();
    assert_eq!(
        got.len(),
        1,
        "server must deliver exactly one General message"
    );
    let (sender_hex, got_payload) = &got[0];
    assert_eq!(got_payload, &payload, "payload must survive the wire");
    assert_eq!(
        sender_hex.to_ascii_lowercase(),
        rig.client_identity_hex.to_ascii_lowercase(),
        "the server-verified sender must be the client's REAL identity key"
    );
    assert!(
        rig.delivery_errors.lock().unwrap().is_empty(),
        "no verification error on the honest leg"
    );

    // The verified identity drives THE production seam.
    let caller = CallerAuth::verified(sender_hex);
    let session_id = sender_hex.to_ascii_lowercase();
    let other_identity = PrivateKey::random()
        .public_key()
        .to_hex()
        .to_ascii_lowercase();
    for mode in [AuthMode::Lenient, AuthMode::Strict] {
        // No query param → the authenticated session's view is served.
        assert_eq!(
            resolve_view_identity(mode, &caller, None),
            IdentityDecision::Serve(Some(session_id.clone())),
        );
        // A DIFFERENT ?identity= claim → refused, never coerced.
        assert_eq!(
            resolve_view_identity(mode, &caller, Some(&other_identity)),
            IdentityDecision::RefuseMismatch {
                session_identity: session_id.clone(),
                query_identity: other_identity.clone(),
            },
        );
        // The caller's own key as the param → served (no self-mismatch).
        assert_eq!(
            resolve_view_identity(mode, &caller, Some(&session_id.to_ascii_uppercase())),
            IdentityDecision::Serve(Some(session_id.clone())),
        );
    }
}

/// Tamper leg: a payload byte flipped AFTER signing must be REFUSED by the
/// server's real signature verification — the general callback never fires.
/// (Also a positive control for the honest cell: same rig, same code path,
/// the only difference is the flipped byte.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tampered_payload_is_refused_by_real_verification() {
    let rig = rig().await;
    // Handshake honestly first (the tamper targets General messages only) —
    // the positive control: the SAME rig and code path delivers when honest.
    send(&rig, b"honest warm-up").await.expect("handshake");
    let received = rig.received.clone();
    wait_for(
        move || received.lock().unwrap().len() == 1,
        "the honest warm-up message",
    )
    .await;

    *rig.mitm.lock().unwrap() = Mitm::TamperPayload;
    let _ = send(&rig, b"tampered-in-flight").await;
    let errors = rig.delivery_errors.clone();
    wait_for(
        move || !errors.lock().unwrap().is_empty(),
        "the receiver to refuse the tampered message",
    )
    .await;

    assert_eq!(
        rig.received.lock().unwrap().len(),
        1,
        "the tampered General message must NOT reach the application"
    );
    let errors = rig.delivery_errors.lock().unwrap().clone();
    assert!(
        errors.iter().any(|e| {
            let e = e.to_ascii_lowercase();
            e.contains("signature") || e.contains("verif")
        }),
        "the receiver must refuse with a signature-verification error; got {errors:?}"
    );
}

/// Counterparty-pairing leg (the `signin.ts` axis): the SAME valid signature
/// presented under a DIFFERENT claimed identity key must be refused — i.e.
/// verification genuinely binds the sender identity, not just the bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signature_under_swapped_identity_is_refused() {
    let rig = rig().await;
    send(&rig, b"honest warm-up").await.expect("handshake");
    let received = rig.received.clone();
    wait_for(
        move || received.lock().unwrap().len() == 1,
        "the honest warm-up message",
    )
    .await;

    *rig.mitm.lock().unwrap() = Mitm::SwapIdentityKey;
    let _ = send(&rig, b"who-am-i-really").await;
    // The refusal is observable as a delivery error (an unknown-identity /
    // failed-verification refusal) — wait for it so the non-delivery
    // assertion below cannot pass by racing the hop.
    let errors = rig.delivery_errors.clone();
    wait_for(
        move || !errors.lock().unwrap().is_empty(),
        "the receiver to refuse the identity-swapped message",
    )
    .await;

    let got = rig.received.lock().unwrap().clone();
    assert_eq!(
        got.len(),
        1,
        "a General message claiming the attacker's identity over the client's \
         signature must NOT be delivered"
    );
    // And in particular nothing was delivered under the attacker's identity.
    assert!(
        got.iter()
            .all(|(sender, _)| sender.eq_ignore_ascii_case(&rig.client_identity_hex)),
        "no delivered message may carry a sender other than the real client"
    );
}

/// The SHARPEST counterparty leg: the attacker has a LIVE authenticated
/// session of their own (so the "no session with sender" cheap refusal
/// cannot fire), and the client's genuinely-valid signature is replayed
/// under the attacker's identity. The refusal must come from the REAL
/// signature verification — the key derivation binds the counterparty
/// identity + session nonce, which is exactly the pairing a mocked verify
/// once hid (`signin.ts`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn swapped_identity_with_live_attacker_session_is_refused_by_signature() {
    let client_key = PrivateKey::random();
    let server_key = PrivateKey::random();
    let attacker_key = PrivateKey::random();
    let attacker_pub = attacker_key.public_key();
    let client_pub_hex = client_key.public_key().to_hex();
    let attacker_pub_hex = attacker_pub.to_hex();

    // Three-party wiring: client and attacker each talk to the server's
    // inbound slot; the server's outbound goes through a ROUTER slot that
    // forwards to whichever counterparty is currently handshaking.
    let client_slot: CallbackSlot = Arc::default();
    let attacker_slot: CallbackSlot = Arc::default();
    let server_slot: CallbackSlot = Arc::default();
    let server_outbound: CallbackSlot = Arc::default();
    let client_mitm = Arc::new(Mutex::new(Mitm::None));
    let errors = Arc::new(Mutex::new(Vec::new()));

    #[derive(Clone, Copy)]
    enum To {
        Client,
        Attacker,
    }
    let route = Arc::new(Mutex::new(To::Attacker));
    {
        let route = route.clone();
        let cs = client_slot.clone();
        let as_ = attacker_slot.clone();
        *server_outbound.write().unwrap() = Some(Box::new(move |m| {
            let target = match *route.lock().unwrap() {
                To::Client => cs.clone(),
                To::Attacker => as_.clone(),
            };
            let guard = target.read().unwrap();
            (guard.as_ref().expect("routed peer must have started"))(m)
        }));
    }

    let mk_end = |mine: &CallbackSlot, other: &CallbackSlot, mitm: &Arc<Mutex<Mitm>>| PairEnd {
        mine: mine.clone(),
        other: other.clone(),
        mitm: mitm.clone(),
        attacker_key: attacker_pub.clone(),
        delivery_errors: errors.clone(),
    };
    let honest_mitm = Arc::new(Mutex::new(Mitm::None));

    let client = Peer::new(PeerOptions {
        wallet: ProtoWallet::new(Some(client_key)),
        transport: mk_end(&client_slot, &server_slot, &client_mitm),
        certificates_to_request: None,
        session_manager: None,
        auto_persist_last_session: false,
        originator: Some("low-app-layer-318-client".into()),
    });
    let attacker = Peer::new(PeerOptions {
        wallet: ProtoWallet::new(Some(attacker_key)),
        transport: mk_end(&attacker_slot, &server_slot, &honest_mitm),
        certificates_to_request: None,
        session_manager: None,
        auto_persist_last_session: false,
        originator: Some("low-app-layer-318-attacker".into()),
    });
    let server = Arc::new(Peer::new(PeerOptions {
        wallet: ProtoWallet::new(Some(server_key)),
        transport: mk_end(&server_slot, &server_outbound, &honest_mitm),
        certificates_to_request: None,
        session_manager: None,
        auto_persist_last_session: false,
        originator: Some("low-app-layer-318-server".into()),
    }));
    client.start();
    attacker.start();
    server.start_server();

    let received: ReceivedLog = Arc::default();
    let sink = received.clone();
    server
        .listen_for_general_messages(move |sender, payload| {
            let sink = sink.clone();
            Box::pin(async move {
                sink.lock().unwrap().push((sender.to_hex(), payload));
                Ok(())
            })
        })
        .await;

    // 1. The attacker authenticates HONESTLY — a live, verified session.
    tokio::time::timeout(
        std::time::Duration::from_millis(TIMEOUT_MS),
        attacker.to_peer(b"attacker's own traffic", None, Some(TIMEOUT_MS)),
    )
    .await
    .expect("attacker handshake must not hang")
    .expect("attacker's own honest session verifies");
    let waiter = received.clone();
    wait_for(
        move || waiter.lock().unwrap().len() == 1,
        "the attacker's own honest message",
    )
    .await;

    // 2. The client authenticates honestly too.
    *route.lock().unwrap() = To::Client;
    tokio::time::timeout(
        std::time::Duration::from_millis(TIMEOUT_MS),
        client.to_peer(b"client's honest traffic", None, Some(TIMEOUT_MS)),
    )
    .await
    .expect("client handshake must not hang")
    .expect("client's honest session verifies");
    let waiter = received.clone();
    wait_for(
        move || waiter.lock().unwrap().len() == 2,
        "the client's honest message",
    )
    .await;

    // 3. The client's next VALID signature rides under the attacker's
    //    identity. The attacker HAS a session, so only the signature
    //    verification itself can refuse — and it must.
    let errors_before = errors.lock().unwrap().len();
    *client_mitm.lock().unwrap() = Mitm::SwapIdentityKey;
    let _ = tokio::time::timeout(
        std::time::Duration::from_millis(TIMEOUT_MS),
        client.to_peer(b"claims-to-be-the-attacker", None, Some(TIMEOUT_MS)),
    )
    .await
    .expect("send must not hang");
    let waiter = errors.clone();
    wait_for(
        move || waiter.lock().unwrap().len() > errors_before,
        "the server to refuse the swapped-identity message",
    )
    .await;

    let errs = errors.lock().unwrap().clone();
    assert!(
        errs.iter()
            .any(|e| e.to_ascii_lowercase().contains("signature")),
        "with a live attacker session the refusal must come from SIGNATURE \
         verification (the counterparty pairing), not a missing session; got {errs:?}"
    );
    let got = received.lock().unwrap().clone();
    assert_eq!(got.len(), 2, "the swapped message must never be delivered");
    assert!(
        !got.iter().any(|(_, p)| p == b"claims-to-be-the-attacker"),
        "the swapped payload must not be delivered under ANY identity"
    );
    // Sanity on attribution of the two honest messages.
    assert!(got[0].0.eq_ignore_ascii_case(&attacker_pub_hex));
    assert!(got[1].0.eq_ignore_ascii_case(&client_pub_hex));
}

/// The sanity guard the tamper cells depend on (Rule 12c, an SDK-trap
/// variant: an inert MITM would make both negative cells pass vacuously):
/// drive the REAL `PairEnd::send` and assert the bytes the wire DELIVERS
/// genuinely differ from the bytes given to it — never a re-implementation
/// of the edit beside the code under test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mitm_edits_are_not_inert_through_the_real_wire() {
    let attacker = PrivateKey::random().public_key();
    let mut w = wire(attacker.clone());
    let sender_end = w.client_end.take().unwrap();
    // Capture what the wire delivers to the "server" side.
    let delivered: Arc<Mutex<Vec<AuthMessage>>> = Arc::default();
    let sink = delivered.clone();
    {
        let mut slot = w.server_end.as_ref().unwrap().mine.write().unwrap();
        *slot = Some(Box::new(move |m| {
            let sink = sink.clone();
            Box::pin(async move {
                sink.lock().unwrap().push(m);
                Ok(())
            })
        }));
    }

    let honest_key = PrivateKey::random().public_key();
    let mut msg = AuthMessage::new(MessageType::General, honest_key.clone());
    msg.payload = Some(vec![0xAB, 0xCD]);

    *w.mitm.lock().unwrap() = Mitm::TamperPayload;
    sender_end.send(&msg).await.unwrap();
    let waiter = delivered.clone();
    wait_for(
        move || waiter.lock().unwrap().len() == 1,
        "the tampered message to be delivered",
    )
    .await;
    *w.mitm.lock().unwrap() = Mitm::SwapIdentityKey;
    sender_end.send(&msg).await.unwrap();
    let waiter = delivered.clone();
    wait_for(
        move || waiter.lock().unwrap().len() == 2,
        "the identity-swapped message to be delivered",
    )
    .await;

    let got = delivered.lock().unwrap().clone();
    // TamperPayload: the DELIVERED payload differs from the signed one.
    assert_eq!(got[0].payload, Some(vec![0xAA, 0xCD]), "first byte flipped");
    assert_eq!(got[0].identity_key.to_hex(), honest_key.to_hex());
    // SwapIdentityKey: the DELIVERED identity is the attacker's; payload intact.
    assert_eq!(got[1].identity_key.to_hex(), attacker.to_hex());
    assert_eq!(got[1].payload, Some(vec![0xAB, 0xCD]));
}
