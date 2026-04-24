//! WalletAdvertiser — creates, finds, and revokes SHIP/SLAP advertisements.
//!
//! Uses bsv-rs overlay admin token functions for PushDrop creation/decoding.
//! The actual wallet operations (createAction, signAction) are delegated to
//! a WalletInterface trait so the advertiser can work with any wallet backend.
//!
//! Ported from `~/bsv/overlay-discovery-services/src/WalletAdvertiser.ts`.

use async_trait::async_trait;
use bsv_rs::overlay::{create_signed_overlay_admin_token, decode_overlay_admin_token};
use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
use overlay_engine::advertiser::{Advertiser, AdvertiserError};
use overlay_engine::types::*;

use crate::validation::{is_advertisable_uri, is_valid_topic_or_service_name};

/// Satoshi value for each advertisement output.
const _AD_TOKEN_VALUE: u64 = 1;

/// WalletAdvertiser — manages SHIP/SLAP advertisements via PushDrop tokens.
///
/// This is a simplified implementation that handles:
/// - Creating advertisement locking scripts via `create_overlay_admin_token()`
/// - Parsing advertisement scripts via `decode_overlay_admin_token()`
/// - Tracking created advertisements in memory for testing
///
/// The full wallet integration (createAction, signAction for on-chain transactions)
/// requires a WalletInterface implementation, which varies by deployment context
/// (Cloudflare Workers, local wallet-cli, etc.).
pub struct WalletAdvertiser {
    /// The advertiser's root private key. Used directly by
    /// [`bsv_rs::overlay::create_signed_overlay_admin_token`] to BRC-42-
    /// derive the locking pubkey AND sign the 5-field PushDrop. Without
    /// the private side, we couldn't produce the signature that modern
    /// `tm_ship` / `tm_slap` validators require per the TS spec.
    private_key: PrivateKey,
    /// The advertiser's identity key (cached so `identity_key_hex()` is
    /// allocation-free).
    identity_key: PublicKey,
    /// The URI where this node's services are hosted.
    advertisable_uri: String,
    /// Advertisements created during this session (for testing/tracking).
    created_ads: std::sync::Mutex<Vec<Advertisement>>,
}

impl WalletAdvertiser {
    /// Create a new WalletAdvertiser.
    ///
    /// # Arguments
    /// - `private_key` - The operator's private key (identity + signing).
    ///   Required (not just the pubkey) because SHIP/SLAP tokens are
    ///   BRC-42-signed per the TS reference at
    ///   `@bsv/overlay-discovery-services/src/WalletAdvertiser.ts`.
    /// - `advertisable_uri` - The URI to advertise (must pass BRC-101
    ///   validation).
    ///
    /// # Errors
    /// Returns [`AdvertiserError::CreationFailed`] if `advertisable_uri`
    /// fails BRC-101 validation (bad scheme, path != `/`, localhost).
    pub fn new(private_key: &PrivateKey, advertisable_uri: &str) -> Result<Self, AdvertiserError> {
        if !is_advertisable_uri(advertisable_uri) {
            return Err(AdvertiserError::CreationFailed(format!(
                "Non-advertisable URI: {advertisable_uri}"
            )));
        }

        Ok(Self {
            private_key: private_key.clone(),
            identity_key: PublicKey::from_private_key(private_key),
            advertisable_uri: advertisable_uri.to_string(),
            created_ads: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Get the advertiser's identity key as hex.
    pub fn identity_key_hex(&self) -> String {
        hex::encode(self.identity_key.to_compressed())
    }

    /// Build a SIGNED locking script for a SHIP or SLAP advertisement.
    ///
    /// Produces the 5-field PushDrop shape that `tm_ship` / `tm_slap` /
    /// `@bsv/overlay-discovery-services` validators admit (byte-exact
    /// match to `@bsv/sdk 1.10.1` — verified in
    /// `overlay-discovery/tests/ts_sdk_parity.rs`).
    pub fn build_ad_script(
        &self,
        protocol: Protocol,
        topic_or_service: &str,
    ) -> Result<bsv_rs::script::LockingScript, AdvertiserError> {
        if !is_valid_topic_or_service_name(topic_or_service) {
            return Err(AdvertiserError::CreationFailed(format!(
                "Invalid topic/service name: {topic_or_service}"
            )));
        }

        Ok(create_signed_overlay_admin_token(
            &self.private_key,
            protocol,
            &self.advertisable_uri,
            topic_or_service,
        ))
    }
}

#[async_trait(?Send)]
impl Advertiser for WalletAdvertiser {
    async fn create_advertisements(
        &self,
        ads: &[AdvertisementData],
    ) -> Result<TaggedBEEF, AdvertiserError> {
        let mut topics_set = std::collections::HashSet::new();
        let mut created = Vec::new();

        for ad in ads {
            let _script = self.build_ad_script(ad.protocol, &ad.topic_or_service_name)?;

            let topic = match ad.protocol {
                Protocol::Ship => "tm_ship",
                Protocol::Slap => "tm_slap",
            };
            topics_set.insert(topic.to_string());

            let advertisement = Advertisement {
                protocol: ad.protocol,
                identity_key: self.identity_key_hex(),
                domain: self.advertisable_uri.clone(),
                topic_or_service: ad.topic_or_service_name.clone(),
                beef: None,
                output_index: Some(created.len() as u32),
            };
            created.push(advertisement);
        }

        // Store for later retrieval
        self.created_ads
            .lock()
            .unwrap()
            .extend(created.iter().cloned());

        // In a full implementation, this would call wallet.createAction() to produce
        // a real transaction with the PushDrop outputs. For now, we return a placeholder
        // TaggedBEEF. The real wallet integration happens in the deployment crate.
        Ok(TaggedBEEF::new(
            vec![], // Placeholder — real impl produces actual BEEF
            topics_set.into_iter().collect(),
        ))
    }

    async fn find_all_advertisements(
        &self,
        protocol: Protocol,
    ) -> Result<Vec<Advertisement>, AdvertiserError> {
        // In a full implementation, this queries the overlay via LookupResolver.
        // For now, return advertisements created during this session.
        Ok(self
            .created_ads
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
        if advertisements.is_empty() {
            return Err(AdvertiserError::RevocationFailed(
                "Must provide advertisements to revoke".into(),
            ));
        }

        let mut topics_set = std::collections::HashSet::new();

        // Remove from tracked ads
        {
            let mut created = self.created_ads.lock().unwrap();
            for ad in advertisements {
                let topic = match ad.protocol {
                    Protocol::Ship => "tm_ship",
                    Protocol::Slap => "tm_slap",
                };
                topics_set.insert(topic.to_string());
                created
                    .retain(|a| a.topic_or_service != ad.topic_or_service || a.domain != ad.domain);
            }
        }

        // In a full implementation, this spends the advertisement UTXOs.
        Ok(TaggedBEEF::new(
            vec![], // Placeholder
            topics_set.into_iter().collect(),
        ))
    }

    fn parse_advertisement(&self, output_script: &[u8]) -> Option<Advertisement> {
        let script = bsv_rs::script::Script::from_binary(output_script).ok()?;
        let locking_script: bsv_rs::script::LockingScript = script.into();
        let token = decode_overlay_admin_token(&locking_script).ok()?;

        Some(Advertisement {
            protocol: token.protocol,
            identity_key: token.identity_key_hex(),
            domain: token.domain,
            topic_or_service: token.topic_or_service,
            beef: None,
            output_index: None,
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_advertiser() -> WalletAdvertiser {
        let pk = PrivateKey::random();
        WalletAdvertiser::new(&pk, "https://overlay.example.com").unwrap()
    }

    // ── Construction ───────────────────────────────────────────────────

    #[test]
    fn rejects_invalid_uri() {
        let pk = PrivateKey::random();
        let result = WalletAdvertiser::new(&pk, "http://localhost");
        assert!(result.is_err());
    }

    #[test]
    fn accepts_valid_uri() {
        let adv = make_advertiser();
        assert!(!adv.identity_key_hex().is_empty());
        assert_eq!(adv.identity_key_hex().len(), 66); // 33 bytes compressed
    }

    // ── Script building ────────────────────────────────────────────────

    #[test]
    fn build_ship_ad_script() {
        let adv = make_advertiser();
        let script = adv.build_ad_script(Protocol::Ship, "tm_test").unwrap();

        // Should be decodable
        let token = decode_overlay_admin_token(&script).unwrap();
        assert_eq!(token.protocol, Protocol::Ship);
        assert_eq!(token.domain, "https://overlay.example.com");
        assert_eq!(token.topic_or_service, "tm_test");
    }

    #[test]
    fn build_slap_ad_script() {
        let adv = make_advertiser();
        let script = adv.build_ad_script(Protocol::Slap, "ls_test").unwrap();

        let token = decode_overlay_admin_token(&script).unwrap();
        assert_eq!(token.protocol, Protocol::Slap);
        assert_eq!(token.topic_or_service, "ls_test");
    }

    #[test]
    fn build_ad_rejects_invalid_name() {
        let adv = make_advertiser();
        let result = adv.build_ad_script(Protocol::Ship, "invalid_name");
        assert!(result.is_err());
    }

    // ── Create advertisements ──────────────────────────────────────────

    #[tokio::test]
    async fn create_ship_and_slap_ads() {
        let adv = make_advertiser();

        let data = vec![
            AdvertisementData {
                protocol: Protocol::Ship,
                topic_or_service_name: "tm_test".into(),
            },
            AdvertisementData {
                protocol: Protocol::Slap,
                topic_or_service_name: "ls_test".into(),
            },
        ];

        let beef = adv.create_advertisements(&data).await.unwrap();
        assert_eq!(beef.topics.len(), 2);
        assert!(beef.topics.contains(&"tm_ship".to_string()));
        assert!(beef.topics.contains(&"tm_slap".to_string()));
    }

    #[tokio::test]
    async fn create_rejects_invalid_topic() {
        let adv = make_advertiser();
        let data = vec![AdvertisementData {
            protocol: Protocol::Ship,
            topic_or_service_name: "INVALID".into(),
        }];
        let result = adv.create_advertisements(&data).await;
        assert!(result.is_err());
    }

    // ── Find advertisements ────────────────────────────────────────────

    #[tokio::test]
    async fn find_returns_created_ads() {
        let adv = make_advertiser();

        adv.create_advertisements(&[AdvertisementData {
            protocol: Protocol::Ship,
            topic_or_service_name: "tm_test".into(),
        }])
        .await
        .unwrap();

        let ship_ads = adv.find_all_advertisements(Protocol::Ship).await.unwrap();
        assert_eq!(ship_ads.len(), 1);
        assert_eq!(ship_ads[0].topic_or_service, "tm_test");
        assert_eq!(ship_ads[0].domain, "https://overlay.example.com");

        let slap_ads = adv.find_all_advertisements(Protocol::Slap).await.unwrap();
        assert!(slap_ads.is_empty());
    }

    // ── Revoke advertisements ──────────────────────────────────────────

    #[tokio::test]
    async fn revoke_removes_ads() {
        let adv = make_advertiser();

        adv.create_advertisements(&[AdvertisementData {
            protocol: Protocol::Ship,
            topic_or_service_name: "tm_test".into(),
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
    async fn revoke_empty_errors() {
        let adv = make_advertiser();
        let result = adv.revoke_advertisements(&[]).await;
        assert!(result.is_err());
    }

    // ── Parse advertisement ────────────────────────────────────────────

    #[test]
    fn parse_ship_ad_from_script() {
        let adv = make_advertiser();
        let script = adv.build_ad_script(Protocol::Ship, "tm_test").unwrap();
        let script_bytes = script.to_binary();

        let parsed = adv.parse_advertisement(&script_bytes).unwrap();
        assert_eq!(parsed.protocol, Protocol::Ship);
        assert_eq!(parsed.domain, "https://overlay.example.com");
        assert_eq!(parsed.topic_or_service, "tm_test");
        assert_eq!(parsed.identity_key, adv.identity_key_hex());
    }

    #[test]
    fn parse_slap_ad_from_script() {
        let adv = make_advertiser();
        let script = adv.build_ad_script(Protocol::Slap, "ls_test").unwrap();
        let script_bytes = script.to_binary();

        let parsed = adv.parse_advertisement(&script_bytes).unwrap();
        assert_eq!(parsed.protocol, Protocol::Slap);
        assert_eq!(parsed.topic_or_service, "ls_test");
    }

    #[test]
    fn parse_invalid_script_returns_none() {
        let adv = make_advertiser();
        assert!(adv.parse_advertisement(&[0x76, 0xa9]).is_none());
        assert!(adv.parse_advertisement(&[]).is_none());
    }

    // ── Cross-SDK vector: roundtrip create → parse ─────────────────────

    #[test]
    fn create_and_parse_roundtrip_ship() {
        let adv = make_advertiser();
        let script = adv
            .build_ad_script(Protocol::Ship, "tm_custom_topic")
            .unwrap();
        let bytes = script.to_binary();

        let parsed = adv.parse_advertisement(&bytes).unwrap();
        assert_eq!(parsed.protocol, Protocol::Ship);
        assert_eq!(parsed.identity_key, adv.identity_key_hex());
        assert_eq!(parsed.domain, "https://overlay.example.com");
        assert_eq!(parsed.topic_or_service, "tm_custom_topic");
    }

    #[test]
    fn create_and_parse_roundtrip_slap() {
        let adv = make_advertiser();
        let script = adv
            .build_ad_script(Protocol::Slap, "ls_custom_service")
            .unwrap();
        let bytes = script.to_binary();

        let parsed = adv.parse_advertisement(&bytes).unwrap();
        assert_eq!(parsed.protocol, Protocol::Slap);
        assert_eq!(parsed.topic_or_service, "ls_custom_service");
    }

    // ── Object safety ──────────────────────────────────────────────────

    #[tokio::test]
    async fn advertiser_trait_is_object_safe() {
        let adv: Box<dyn Advertiser> = Box::new(make_advertiser());
        let ads = adv.find_all_advertisements(Protocol::Ship).await.unwrap();
        assert!(ads.is_empty());
    }
}
