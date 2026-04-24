//! EngineBuilder — composable configuration for the Overlay Services Engine.
//!
//! Provides a builder pattern for registering topic managers, lookup services,
//! and configuration before constructing the Engine.

use crate::advertiser::Advertiser;
use crate::broadcaster::{ArcBroadcaster, Broadcaster};
use crate::engine::{Engine, EngineConfig};
use crate::lookup_service::LookupService;
use crate::storage::Storage;
use crate::topic_manager::TopicManager;
use std::collections::HashMap;

/// Builder for constructing an [`Engine`] with composable configuration.
///
/// # Example
/// ```rust,ignore
/// let engine = EngineBuilder::new(Box::new(MemoryStorage::new()))
///     .with_topic("tm_ship", Box::new(SHIPTopicManager::new()))
///     .with_lookup("ls_ship", Box::new(SHIPLookupService::new(storage)))
///     .with_hosting_url("https://my-overlay.example.com")
///     .with_ship_trackers(vec!["https://overlay-us-1.bsvb.tech".into()])
///     .build();
/// ```
pub struct EngineBuilder {
    storage: Box<dyn Storage>,
    managers: HashMap<String, Box<dyn TopicManager>>,
    lookup_services: HashMap<String, Box<dyn LookupService>>,
    advertiser: Option<Box<dyn Advertiser>>,
    broadcaster: Option<Box<dyn Broadcaster>>,
    arc_broadcaster: Option<Box<dyn ArcBroadcaster>>,
    chain_tracker: Option<Box<dyn bsv_rs::transaction::ChainTracker>>,
    hosting_url: Option<String>,
    ship_trackers: Vec<String>,
    slap_trackers: Vec<String>,
    suppress_default_sync_ads: bool,
}

impl EngineBuilder {
    /// Create a new builder with the given storage backend.
    pub fn new(storage: Box<dyn Storage>) -> Self {
        Self {
            storage,
            managers: HashMap::new(),
            lookup_services: HashMap::new(),
            advertiser: None,
            broadcaster: None,
            arc_broadcaster: None,
            chain_tracker: None,
            hosting_url: None,
            ship_trackers: Vec::new(),
            slap_trackers: Vec::new(),
            suppress_default_sync_ads: true,
        }
    }

    /// Register a topic manager.
    pub fn with_topic(mut self, name: impl Into<String>, manager: Box<dyn TopicManager>) -> Self {
        self.managers.insert(name.into(), manager);
        self
    }

    /// Register a lookup service.
    pub fn with_lookup(mut self, name: impl Into<String>, service: Box<dyn LookupService>) -> Self {
        self.lookup_services.insert(name.into(), service);
        self
    }

    /// Set the advertiser for SHIP/SLAP advertisement management.
    pub fn with_advertiser(mut self, advertiser: Box<dyn Advertiser>) -> Self {
        self.advertiser = Some(advertiser);
        self
    }

    /// Set the broadcaster for SHIP propagation to peer nodes.
    pub fn with_broadcaster(mut self, broadcaster: Box<dyn Broadcaster>) -> Self {
        self.broadcaster = Some(broadcaster);
        self
    }

    /// Set the ARC broadcaster for network broadcast to miners.
    pub fn with_arc_broadcaster(mut self, arc: Box<dyn ArcBroadcaster>) -> Self {
        self.arc_broadcaster = Some(arc);
        self
    }

    /// Set the chain tracker for SPV verification.
    pub fn with_chain_tracker(
        mut self,
        tracker: Box<dyn bsv_rs::transaction::ChainTracker>,
    ) -> Self {
        self.chain_tracker = Some(tracker);
        self
    }

    /// Set the hosting URL for this overlay node.
    pub fn with_hosting_url(mut self, url: impl Into<String>) -> Self {
        self.hosting_url = Some(url.into());
        self
    }

    /// Add SHIP tracker URLs for bootstrapping peer discovery.
    pub fn with_ship_trackers(mut self, trackers: Vec<String>) -> Self {
        self.ship_trackers = trackers;
        self
    }

    /// Add SLAP tracker URLs for bootstrapping service discovery.
    pub fn with_slap_trackers(mut self, trackers: Vec<String>) -> Self {
        self.slap_trackers = trackers;
        self
    }

    /// Control whether default SHIP/SLAP sync advertisements are suppressed.
    pub fn suppress_default_sync_advertisements(mut self, suppress: bool) -> Self {
        self.suppress_default_sync_ads = suppress;
        self
    }

    /// Build the Engine.
    pub fn build(self) -> Engine {
        Engine::with_all(
            self.managers,
            self.lookup_services,
            self.storage,
            self.advertiser,
            self.broadcaster,
            self.arc_broadcaster,
            self.chain_tracker,
            EngineConfig {
                hosting_url: self.hosting_url,
                ship_trackers: self.ship_trackers,
                slap_trackers: self.slap_trackers,
                sync_configuration: HashMap::new(),
                suppress_default_sync_advertisements: self.suppress_default_sync_ads,
            },
        )
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryStorage;
    use crate::topic_manager::TopicManagerError;
    use crate::types::*;
    use async_trait::async_trait;

    struct DummyTM;
    #[async_trait(?Send)]
    impl TopicManager for DummyTM {
        async fn identify_admissible_outputs(
            &self,
            _: &[u8],
            _: &[u8],
            _: Option<&[u8]>,
            _: SubmitMode,
        ) -> Result<AdmittanceInstructions, TopicManagerError> {
            Ok(AdmittanceInstructions::default())
        }
        async fn get_documentation(&self) -> String {
            "Dummy".into()
        }
        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "dummy".into(),
                ..Default::default()
            }
        }
    }

    struct DummyLS;
    #[async_trait(?Send)]
    impl LookupService for DummyLS {
        fn admission_mode(&self) -> AdmissionMode {
            AdmissionMode::LockingScript
        }
        fn spend_notification_mode(&self) -> SpendNotificationMode {
            SpendNotificationMode::None
        }
        async fn output_admitted_by_topic(
            &self,
            _: &OutputAdmittedByTopic,
        ) -> Result<(), crate::lookup_service::LookupServiceError> {
            Ok(())
        }
        async fn output_evicted(
            &self,
            _: &str,
            _: u32,
        ) -> Result<(), crate::lookup_service::LookupServiceError> {
            Ok(())
        }
        async fn lookup(
            &self,
            _: &LookupQuestion,
        ) -> Result<Vec<UTXOReference>, crate::lookup_service::LookupServiceError> {
            Ok(vec![])
        }
        async fn get_documentation(&self) -> String {
            "Dummy".into()
        }
        async fn get_metadata(&self) -> ServiceMetadata {
            ServiceMetadata {
                name: "dummy-ls".into(),
                ..Default::default()
            }
        }
    }

    #[tokio::test]
    async fn builder_creates_engine_with_topics() {
        let engine = EngineBuilder::new(Box::new(MemoryStorage::new()))
            .with_topic("tm_test", Box::new(DummyTM))
            .with_lookup("ls_test", Box::new(DummyLS))
            .build();

        let managers = engine.list_topic_managers().await;
        assert!(managers.contains_key("tm_test"));
        assert_eq!(managers["tm_test"].name, "dummy");

        let services = engine.list_lookup_service_providers().await;
        assert!(services.contains_key("ls_test"));
    }

    #[tokio::test]
    async fn builder_with_multiple_topics() {
        let engine = EngineBuilder::new(Box::new(MemoryStorage::new()))
            .with_topic("tm_alpha", Box::new(DummyTM))
            .with_topic("tm_beta", Box::new(DummyTM))
            .with_lookup("ls_alpha", Box::new(DummyLS))
            .with_lookup("ls_beta", Box::new(DummyLS))
            .build();

        let managers = engine.list_topic_managers().await;
        assert_eq!(managers.len(), 2);
    }

    #[test]
    fn builder_with_hosting_url() {
        let engine = EngineBuilder::new(Box::new(MemoryStorage::new()))
            .with_hosting_url("https://overlay.example.com")
            .build();

        assert_eq!(
            engine.config().hosting_url.as_deref(),
            Some("https://overlay.example.com")
        );
    }

    #[test]
    fn builder_with_trackers() {
        let engine = EngineBuilder::new(Box::new(MemoryStorage::new()))
            .with_ship_trackers(vec!["https://ship1.example.com".into()])
            .with_slap_trackers(vec!["https://slap1.example.com".into()])
            .build();

        assert_eq!(engine.config().ship_trackers.len(), 1);
        assert_eq!(engine.config().slap_trackers.len(), 1);
    }

    #[test]
    fn builder_empty_is_valid() {
        let engine = EngineBuilder::new(Box::new(MemoryStorage::new())).build();
        assert!(engine.config().hosting_url.is_none());
    }

    #[test]
    fn builder_sync_config_defaults() {
        let engine = EngineBuilder::new(Box::new(MemoryStorage::new()))
            .with_topic("tm_custom", Box::new(DummyTM))
            .build();

        // Custom topics default to SHIP sync
        assert!(engine.config().sync_configuration.contains_key("tm_custom"));
    }
}
