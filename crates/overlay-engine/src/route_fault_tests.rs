// Engine 0.2.0 (bsv-overlay-cloudflare #57): a route fault counts nothing
// against the peer. The witnesses run through a mock TRANSPORT: the peer's
// calls are answered by an HTTP status and body, classified as the Worker's
// remote classifies them (`route_fault::of_answer`, else a fault of the
// peer, `RemoteError`).

/// A peer behind a transport that answers every call with one HTTP status
/// and body: what the platform or the peer said, before any GASP.
struct HttpAnswerRemote {
    status: u16,
    body: &'static str,
}

impl HttpAnswerRemote {
    fn fail<T>(&self) -> Result<T, GASPError> {
        let detail = format!("Peer returned HTTP {}: {}", self.status, self.body);
        Err(match crate::route_fault::of_answer(self.status, self.body) {
            Some(class) => GASPError::RouteFault {
                class: class.to_string(),
                detail,
            },
            None => GASPError::RemoteError(detail),
        })
    }
}

#[async_trait(?Send)]
impl crate::gasp::GASPRemote for HttpAnswerRemote {
    async fn get_initial_response(
        &self,
        _: &crate::types::GASPInitialRequest,
    ) -> Result<crate::types::GASPInitialResponse, GASPError> {
        self.fail()
    }
    async fn get_initial_reply(
        &self,
        _: &crate::types::GASPInitialResponse,
    ) -> Result<crate::types::GASPInitialReply, GASPError> {
        self.fail()
    }
    async fn request_node(
        &self,
        _: &str,
        _: &str,
        _: u32,
        _: bool,
    ) -> Result<crate::types::GASPNode, GASPError> {
        self.fail()
    }
    async fn submit_node(
        &self,
        _: &crate::types::GASPNode,
    ) -> Result<Option<crate::types::GASPNodeResponse>, GASPError> {
        self.fail()
    }
}

struct HttpAnswerFactory {
    status: u16,
    body: &'static str,
    created: std::rc::Rc<Cell<u64>>,
}

impl crate::gasp::GASPRemoteFactory for HttpAnswerFactory {
    fn create_remote(&self, _: &str, _: &str) -> Box<dyn crate::gasp::GASPRemote> {
        self.created.set(self.created.get() + 1);
        Box::new(HttpAnswerRemote {
            status: self.status,
            body: self.body,
        })
    }
}

const ROUTE_PEER: &str = "https://your-peer.example.workers.dev";

/// One engine syncing `tm_test` from [`ROUTE_PEER`] through a transport
/// that answers `status` and `body`; the peer's cadence starts at three
/// quiet syncs so a write of it shows.
async fn route_engine(
    status: u16,
    body: &'static str,
) -> (Engine, std::rc::Rc<MemoryStorage>, std::rc::Rc<Cell<u64>>) {
    let store = std::rc::Rc::new(MemoryStorage::new());
    let created = std::rc::Rc::new(Cell::new(0));
    let mut sync_config: SyncConfiguration = HashMap::new();
    sync_config.insert(
        "tm_test".to_string(),
        SyncTarget::Peers(vec![ROUTE_PEER.to_string()]),
    );
    let mut engine = Engine::new(
        HashMap::new(),
        HashMap::new(),
        Box::new(store.clone()),
        None,
        EngineConfig {
            sync_configuration: sync_config,
            ..Default::default()
        },
    );
    engine.set_gasp_remote_factory(Box::new(HttpAnswerFactory {
        status,
        body,
        created: created.clone(),
    }));
    let host = crate::gasp::peer_origin(ROUTE_PEER);
    store
        .set_peer_cadence(
            &host,
            "tm_test",
            &crate::sync_load::PeerCadence {
                quiet_syncs: 3,
                retry_after_secs: 0,
                advertisement: String::new(),
            },
        )
        .await
        .unwrap();
    (engine, store, created)
}

#[tokio::test]
async fn route_057_eight_1042_route_failures_leave_the_peers_health_untouched() {
    let (engine, store, created) = route_engine(404, "error code: 1042").await;
    let host = crate::gasp::peer_origin(ROUTE_PEER);
    let ticks = crate::gasp::PEER_QUARANTINE_THRESHOLD;
    for tick in 1..=ticks {
        let result = engine.start_gasp_sync().await.unwrap();
        store.advance_clock(900);
        let topic = &result.topics_synced["tm_test"];
        // The sync result names the route and the peer.
        assert_eq!(
            topic.route_faults,
            vec![crate::route_fault::RouteFaultSeen {
                peer: ROUTE_PEER.to_string(),
                class: crate::route_fault::class::CF_1042.to_string(),
            }],
            "tick {tick}"
        );
        assert_eq!(
            topic.route_faults[0].label(),
            format!("{ROUTE_PEER} route_fault:cf_1042")
        );
        assert!(
            topic.errors.len() == 1 && topic.errors[0].contains("route_fault:cf_1042"),
            "tick {tick}: {:?}",
            topic.errors
        );
        assert!(topic.peers_skipped.is_empty(), "tick {tick}");
        assert_eq!(created.get(), tick, "the peer is attempted every tick");
    }
    // The peer's health rows are untouched: no failure, no attempt stamp, the
    // cadence as it was.
    let health = store.get_peer_sync_health(&host, "tm_test").await.unwrap();
    assert_eq!(health.consecutive_failures, 0);
    assert_eq!(health.secs_since_last_attempt, None);
    assert!(!crate::gasp::peer_sync_quarantined(&health));
    let cadence = store.get_peer_cadence(&host, "tm_test").await.unwrap();
    assert_eq!(cadence.quiet_syncs, 3);
    // The ninth tick still reaches for the peer: never quarantined by a route.
    let result = engine.start_gasp_sync().await.unwrap();
    assert!(result.topics_synced["tm_test"].peers_skipped.is_empty());
    assert_eq!(created.get(), ticks + 1);
}

#[tokio::test]
async fn route_057_eight_peer_5xx_answers_quarantine_as_before() {
    let (engine, store, created) = route_engine(503, "Service Unavailable").await;
    let host = crate::gasp::peer_origin(ROUTE_PEER);
    let ticks = crate::gasp::PEER_QUARANTINE_THRESHOLD;
    for tick in 1..=ticks {
        let result = engine.start_gasp_sync().await.unwrap();
        store.advance_clock(900);
        let topic = &result.topics_synced["tm_test"];
        assert!(topic.route_faults.is_empty(), "tick {tick}");
        assert_eq!(topic.errors.len(), 1, "tick {tick}");
    }
    let health = store.get_peer_sync_health(&host, "tm_test").await.unwrap();
    assert_eq!(health.consecutive_failures, ticks);
    assert!(crate::gasp::peer_sync_quarantined(&health));
    // The ninth tick skips the peer with no request: the quarantine judged it.
    let result = engine.start_gasp_sync().await.unwrap();
    let skipped = &result.topics_synced["tm_test"].peers_skipped;
    assert_eq!(skipped.len(), 1);
    assert_eq!(skipped[0].reason, "quarantine");
    assert_eq!(created.get(), ticks);
}
