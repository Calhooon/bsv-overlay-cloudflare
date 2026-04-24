//! Advertiser trait — manages SHIP/SLAP overlay advertisements.
//!
//! The Advertiser creates, discovers, and revokes advertisements that tell
//! other overlay nodes which topics/services this node supports.
//!
//! Ported from `~/bsv/overlay-services/src/Advertiser.ts`.

use async_trait::async_trait;

use crate::types::{Advertisement, AdvertisementData, Protocol, TaggedBEEF};

/// Manages SHIP and SLAP advertisements for an overlay node.
///
/// SHIP advertisements tell other nodes "I host topic X at URL Y".
/// SLAP advertisements tell other nodes "I provide lookup service X at URL Y".
///
/// The Engine calls `sync_advertisements()` to keep ads in sync with
/// configured topic managers and lookup services.
#[async_trait(?Send)]
pub trait Advertiser {
    /// Create new advertisements for the given topics/services.
    ///
    /// Returns a TaggedBEEF transaction containing the advertisement outputs.
    /// The Engine will submit this transaction to the overlay.
    async fn create_advertisements(
        &self,
        ads: &[AdvertisementData],
    ) -> Result<TaggedBEEF, AdvertiserError>;

    /// Find all existing advertisements for the given protocol (SHIP or SLAP).
    ///
    /// Queries the overlay to discover this node's own published advertisements.
    async fn find_all_advertisements(
        &self,
        protocol: Protocol,
    ) -> Result<Vec<Advertisement>, AdvertiserError>;

    /// Revoke existing advertisements by spending their UTXOs.
    ///
    /// Returns a TaggedBEEF transaction that spends the advertisement outputs.
    async fn revoke_advertisements(
        &self,
        advertisements: &[Advertisement],
    ) -> Result<TaggedBEEF, AdvertiserError>;

    /// Parse an output script to extract an advertisement.
    ///
    /// Used when processing transactions to identify advertisement outputs.
    fn parse_advertisement(&self, output_script: &[u8]) -> Option<Advertisement>;
}

/// Errors from Advertiser operations.
#[derive(Debug, thiserror::Error)]
pub enum AdvertiserError {
    /// Failed to create advertisement transaction.
    #[error("creation failed: {0}")]
    CreationFailed(String),

    /// Failed to find advertisements.
    #[error("lookup failed: {0}")]
    LookupFailed(String),

    /// Failed to revoke advertisements.
    #[error("revocation failed: {0}")]
    RevocationFailed(String),

    /// Failed to parse advertisement from script.
    #[error("parse failed: {0}")]
    ParseFailed(String),

    /// Generic error.
    #[error("{0}")]
    Other(String),
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Protocol;

    /// Mock advertiser that tracks calls.
    struct MockAdvertiser {
        ads: std::sync::Mutex<Vec<Advertisement>>,
    }

    impl MockAdvertiser {
        fn new() -> Self {
            Self {
                ads: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait(?Send)]
    impl Advertiser for MockAdvertiser {
        async fn create_advertisements(
            &self,
            ads: &[AdvertisementData],
        ) -> Result<TaggedBEEF, AdvertiserError> {
            let topics: Vec<String> = ads
                .iter()
                .map(|a| match a.protocol {
                    Protocol::Ship => "tm_ship".to_string(),
                    Protocol::Slap => "tm_slap".to_string(),
                })
                .collect();

            for ad in ads {
                self.ads.lock().unwrap().push(Advertisement {
                    protocol: ad.protocol,
                    identity_key: "02mock".to_string(),
                    domain: "https://mock.example.com".to_string(),
                    topic_or_service: ad.topic_or_service_name.clone(),
                    beef: None,
                    output_index: None,
                });
            }

            Ok(TaggedBEEF::new(vec![0x01], topics))
        }

        async fn find_all_advertisements(
            &self,
            protocol: Protocol,
        ) -> Result<Vec<Advertisement>, AdvertiserError> {
            Ok(self
                .ads
                .lock()
                .unwrap()
                .iter()
                .filter(|a| a.protocol == protocol)
                .cloned()
                .collect())
        }

        async fn revoke_advertisements(
            &self,
            advertisements: &[Advertisement],
        ) -> Result<TaggedBEEF, AdvertiserError> {
            let mut ads = self.ads.lock().unwrap();
            for ad in advertisements {
                ads.retain(|a| a.topic_or_service != ad.topic_or_service);
            }
            Ok(TaggedBEEF::new(vec![0x02], vec![]))
        }

        fn parse_advertisement(&self, _output_script: &[u8]) -> Option<Advertisement> {
            None
        }
    }

    #[tokio::test]
    async fn test_create_and_find_advertisements() {
        let adv = MockAdvertiser::new();

        let data = vec![
            AdvertisementData {
                protocol: Protocol::Ship,
                topic_or_service_name: "tm_test".to_string(),
            },
            AdvertisementData {
                protocol: Protocol::Slap,
                topic_or_service_name: "ls_test".to_string(),
            },
        ];

        let beef = adv.create_advertisements(&data).await.unwrap();
        assert_eq!(beef.topics.len(), 2);

        let ship_ads = adv.find_all_advertisements(Protocol::Ship).await.unwrap();
        assert_eq!(ship_ads.len(), 1);
        assert_eq!(ship_ads[0].topic_or_service, "tm_test");

        let slap_ads = adv.find_all_advertisements(Protocol::Slap).await.unwrap();
        assert_eq!(slap_ads.len(), 1);
        assert_eq!(slap_ads[0].topic_or_service, "ls_test");
    }

    #[tokio::test]
    async fn test_revoke_advertisements() {
        let adv = MockAdvertiser::new();

        adv.create_advertisements(&[AdvertisementData {
            protocol: Protocol::Ship,
            topic_or_service_name: "tm_test".to_string(),
        }])
        .await
        .unwrap();

        let ads = adv.find_all_advertisements(Protocol::Ship).await.unwrap();
        assert_eq!(ads.len(), 1);

        adv.revoke_advertisements(&ads).await.unwrap();

        let ads = adv.find_all_advertisements(Protocol::Ship).await.unwrap();
        assert!(ads.is_empty());
    }

    #[tokio::test]
    async fn test_advertiser_is_object_safe() {
        let adv: Box<dyn Advertiser> = Box::new(MockAdvertiser::new());
        let result = adv.find_all_advertisements(Protocol::Ship).await.unwrap();
        assert!(result.is_empty());
    }
}
