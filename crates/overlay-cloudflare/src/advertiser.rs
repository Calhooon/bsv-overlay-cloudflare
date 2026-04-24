//! `CloudflareAdvertiser` — produces real on-chain SHIP/SLAP advertisements
//! for the rust-overlay deployment.
//!
//! # Why this exists
//!
//! The stock [`overlay_discovery::advertiser::WalletAdvertiser`] can build
//! advertisement locking scripts but returns an **empty** [`TaggedBEEF`]
//! from `create_advertisements` — the bundled impl has an explicit TODO:
//! *"full wallet integration happens in the deployment crate."* That
//! deployment crate is us.
//!
//! Without a real advertiser, the every-15-minutes
//! [`overlay_engine::Engine::sync_advertisements`] no-ops, so our overlay
//! publishes no on-chain `SHIP`/`SLAP` tokens and no peer (bsvb.tech, other
//! rust-overlays) can discover the topics we carry. GASP sync then has no
//! peers and every UHRP advert lives only on this instance — the network
//! is effectively isolated.
//!
//! `CloudflareAdvertiser` closes that loop. It mirrors the
//! [`bsv-storage-cloudflare` `/advertise` route][ref] one-for-one:
//!
//! 1. Build the PushDrop locking script via [`WalletAdvertiser::build_ad_script`]
//!    (`protocol` / `identity` / `domain` / `topic_or_service` 4-field encoding).
//! 2. `wallet-infra.createAction` with the script as the sole output.
//! 3. Sign change inputs locally via [`crate::wallet::signer::sign_transaction`]
//!    (RFC 6979 deterministic ECDSA → byte-exact raw tx → byte-exact txid).
//! 4. `wallet-infra.processAction` — records + broadcasts via ARC.
//! 5. Wrap in AtomicBEEF and return as [`TaggedBEEF`] tagged `tm_ship` /
//!    `tm_slap`; the engine feeds it to its own `submit` which runs
//!    [`overlay_discovery::ship::SHIPTopicManager`] /
//!    [`overlay_discovery::slap::SLAPTopicManager`] → writes to D1.
//!
//! `find_all_advertisements` does **not** go through `LookupResolver` — it
//! reads directly from our own [`SHIPStorage`] / [`SLAPStorage`] D1 tables
//! filtered by our identity key + hosting URL. That's both faster than a
//! network query and the correct answer for "what have WE published?": no
//! other node is authoritative about our own advertisements.
//!
//! `revoke_advertisements` is intentionally a no-op for v1. Revoke requires
//! spending our own PushDrop UTXOs with a properly signed unlocking script,
//! which lives in `bsv_rs::script::templates::PushDrop::unlock`. Deferred
//! until an operational need forces it (typically when a topic is retired
//! — rare). See task #19 follow-up.
//!
//! [ref]: ~/bsv/bsv-storage-cloudflare/src/routes/advertise.rs

use async_trait::async_trait;
use bsv_rs::primitives::ec::{PrivateKey, PublicKey};
use bsv_rs::transaction::Beef;
use overlay_discovery::advertiser::WalletAdvertiser;
use overlay_discovery::ship::storage::SHIPStorage;
use overlay_discovery::slap::storage::SLAPStorage;
use overlay_engine::advertiser::{Advertiser, AdvertiserError};
use overlay_engine::types::{Advertisement, AdvertisementData, Protocol, TaggedBEEF};
use std::rc::Rc;

use crate::wallet::client::Wallet;
use crate::wallet::signer::sign_transaction;
use crate::wallet::types::{CreateActionOutput, CreateActionRequest, ProcessActionRequest};

/// Wallet-infra endpoint env var. Shared with `crate::wallet::client` so
/// operators set exactly one variable.
pub const WALLET_STORAGE_URL_VAR: &str = "WALLET_STORAGE_URL";

/// Satoshis locked inside each SHIP/SLAP advert token. Matches the TS
/// reference (`WalletAdvertiser.ts` const `AD_TOKEN_VALUE = 1`).
const AD_TOKEN_VALUE: u64 = 1;

/// Basket name for our own SHIP/SLAP advertisement UTXOs. Not
/// user-configurable — `/admin/syncAdvertisements` only knows to look here.
const AD_BASKET: &str = "overlay advertisements";

/// CF Worker implementation of [`Advertiser`].
///
/// Construction is cheap — holds references to Rc-shared D1 storage and
/// the admin private key. A fresh [`Wallet`] is built on demand inside
/// `create_advertisements` to keep the BRC-103 handshake state scoped to
/// one operation (same design as [`crate::wallet::client::Wallet::from_env`]).
pub struct CloudflareAdvertiser {
    /// Operator identity key. Used for both BRC-103 auth against
    /// wallet-infra AND for signing the PushDrop output scripts.
    private_key: PrivateKey,
    /// `https://<hostname>` — what we advertise as our service endpoint.
    /// Already validated by the caller against `is_advertisable_uri`.
    hosting_url: String,
    /// `https://<your-wallet-storage>` or override. Used by the
    /// [`Wallet`] we construct per call.
    wallet_storage_url: String,
    /// Inner stub — reused for `build_ad_script` (PushDrop field layout is
    /// identical in stock vs CF impl) and for `parse_advertisement`.
    inner: WalletAdvertiser,
    /// Our own SHIP records — queried by `find_all_advertisements` for
    /// `Protocol::Ship`.
    ship_storage: Rc<dyn SHIPStorage>,
    /// Our own SLAP records — queried by `find_all_advertisements` for
    /// `Protocol::Slap`.
    slap_storage: Rc<dyn SLAPStorage>,
}

impl CloudflareAdvertiser {
    /// Construct an advertiser. Returns an error if `hosting_url` fails
    /// BRC-101 validation (same rules as `WalletAdvertiser::new`).
    pub fn new(
        private_key: PrivateKey,
        hosting_url: String,
        wallet_storage_url: String,
        ship_storage: Rc<dyn SHIPStorage>,
        slap_storage: Rc<dyn SLAPStorage>,
    ) -> Result<Self, AdvertiserError> {
        let inner = WalletAdvertiser::new(&private_key, &hosting_url)
            .map_err(|e| AdvertiserError::CreationFailed(e.to_string()))?;
        Ok(Self {
            private_key,
            hosting_url,
            wallet_storage_url,
            inner,
            ship_storage,
            slap_storage,
        })
    }

    /// Operator identity as 66-char compressed hex. Used by
    /// `find_all_advertisements` to filter storage rows down to our own
    /// adverts.
    fn identity_key_hex(&self) -> String {
        PublicKey::from_private_key(&self.private_key)
            .to_compressed()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}

#[async_trait(?Send)]
impl Advertiser for CloudflareAdvertiser {
    async fn create_advertisements(
        &self,
        ads: &[AdvertisementData],
    ) -> Result<TaggedBEEF, AdvertiserError> {
        worker::console_log!(
            "advertiser.create_advertisements: n={} hosting_url={}",
            ads.len(),
            self.hosting_url
        );
        if ads.is_empty() {
            // Engine's caller already guards this (`if !all_to_create.is_empty()`),
            // but defence-in-depth: a zero-output createAction would be
            // rejected by wallet-infra with an opaque serde error.
            return Ok(TaggedBEEF::new(vec![], vec![]));
        }

        // Build one CreateActionOutput per advertisement.
        let mut outputs: Vec<CreateActionOutput> = Vec::with_capacity(ads.len());
        let mut topics_set = std::collections::HashSet::new();
        for ad in ads {
            let lock = self
                .inner
                .build_ad_script(ad.protocol, &ad.topic_or_service_name)
                .map_err(|e| AdvertiserError::CreationFailed(e.to_string()))?;
            let proto_name = match ad.protocol {
                Protocol::Ship => "SHIP",
                Protocol::Slap => "SLAP",
            };
            outputs.push(CreateActionOutput {
                locking_script: lock.to_hex(),
                satoshis: AD_TOKEN_VALUE,
                output_description: format!(
                    "{proto_name} advertisement of {}",
                    ad.topic_or_service_name
                ),
                basket: Some(AD_BASKET.to_string()),
                tags: vec![
                    format!("protocol:{proto_name}"),
                    format!("topic:{}", ad.topic_or_service_name),
                    format!("domain:{}", self.hosting_url),
                ],
                // `customInstructions IS NOT NULL` is wallet-infra's
                // "ours" predicate — a spendable=1 flip on processAction
                // depends on this. Stuff the protocol+topic here so
                // /renew-style revocations can rebuild context.
                custom_instructions: Some(
                    serde_json::json!({
                        "kind": "overlay-advertisement",
                        "protocol": proto_name,
                        "topicOrService": ad.topic_or_service_name,
                    })
                    .to_string(),
                ),
            });
            let topic = match ad.protocol {
                Protocol::Ship => "tm_ship",
                Protocol::Slap => "tm_slap",
            };
            topics_set.insert(topic.to_string());
        }

        // Build a Wallet scoped to this call. Same per-call-client pattern
        // as bsv-storage-cloudflare's `Wallet::from_env` — the BRC-103
        // session state is mutated per RPC and can't safely cross calls.
        let wallet = Wallet::new(self.private_key.clone(), self.wallet_storage_url.clone());

        // createAction: unsigned template + selected P2PKH change inputs.
        let create_req = CreateActionRequest {
            outputs,
            inputs: vec![],
            input_beef: None,
            description: "SHIP/SLAP advertisement issuance".to_string(),
            randomize_outputs: false,
        };
        worker::console_log!(
            "advertiser: createAction outputs={} wallet_url={}",
            create_req.outputs.len(),
            self.wallet_storage_url
        );
        let template = wallet
            .create_action(&create_req)
            .await
            .map_err(|e| AdvertiserError::CreationFailed(format!("createAction: {e}")))?;
        worker::console_log!(
            "advertiser: createAction OK inputs={} outputs={} ref={}",
            template.inputs.len(),
            template.outputs.len(),
            template.reference
        );

        // Sign P2PKH change inputs locally.
        let signed = sign_transaction(&template, &self.private_key).map_err(|e| {
            worker::console_log!("advertiser: sign_transaction FAILED {e:?}");
            AdvertiserError::CreationFailed(format!("sign: {e:?}"))
        })?;
        worker::console_log!(
            "advertiser: sign_transaction OK txid={} raw_tx_len={}",
            signed.txid,
            signed.raw_tx.len()
        );

        // processAction: record + broadcast via ARC.
        let process_req = ProcessActionRequest {
            is_new_tx: true,
            is_send_with: false,
            is_no_send: false,
            is_delayed: false,
            reference: Some(template.reference.clone()),
            txid: Some(signed.txid.clone()),
            raw_tx: Some(signed.raw_tx.clone()),
            send_with: vec![],
        };
        wallet.process_action(&process_req).await.map_err(|e| {
            worker::console_log!("advertiser: processAction FAILED {e}");
            AdvertiserError::CreationFailed(format!("processAction: {e}"))
        })?;
        worker::console_log!("advertiser: processAction OK");

        // Wrap the signed tx + its input ancestry in an AtomicBEEF.
        // `Engine::submit` will run tm_ship/tm_slap over this BEEF.
        let mut beef = Beef::from_binary(&template.input_beef).map_err(|e| {
            worker::console_log!("advertiser: Beef::from_binary FAILED {e}");
            AdvertiserError::CreationFailed(format!("Beef::from_binary: {e}"))
        })?;
        beef.merge_raw_tx(signed.raw_tx.clone(), None);
        let atomic_beef = beef.to_binary_atomic(&signed.txid).map_err(|e| {
            worker::console_log!("advertiser: to_binary_atomic FAILED {e}");
            AdvertiserError::CreationFailed(format!("to_binary_atomic: {e}"))
        })?;
        worker::console_log!(
            "advertiser: AtomicBEEF built {} bytes topics={:?}",
            atomic_beef.len(),
            topics_set
        );

        let tagged = TaggedBEEF::new(atomic_beef, topics_set.into_iter().collect());

        // God-tier B: fan out our SHIP/SLAP self-advertisements to every
        // mainnet tm_X peer discovered via DEFAULT_SLAP_TRACKERS. Without
        // this, only the Engine's local ls_ship propagation runs, and no
        // mainnet overlay learns that we serve these topics — defeating
        // the whole point of registering. With this, a default-configured
        // `@bsv/sdk` LookupResolver client discovers us in `ls_ship` and
        // routes queries to us directly. See `crate::mainnet_fanout` for
        // the SHIPBroadcaster port and parity notes.
        //
        // Runs *after* the engine submits locally (in the caller) AND
        // after we've signed+broadcast via ARC above. Errors swallowed —
        // mainnet trackers may be transient (the sync_advertisements cron
        // retries every 15 minutes anyway).
        crate::mainnet_fanout::fan_out(&tagged, Some(&self.hosting_url)).await;

        Ok(tagged)
    }

    async fn find_all_advertisements(
        &self,
        protocol: Protocol,
    ) -> Result<Vec<Advertisement>, AdvertiserError> {
        // "What have WE advertised?" — authoritative in our own D1; no
        // network query needed. The TS reference uses `LookupResolver`
        // because it has no direct DB access from the Advertiser position;
        // we do, so we take the cheaper + deterministic path.
        let identity = self.identity_key_hex();
        // We filter in-memory against `find_all_records()` rather than
        // adding a new typed query method to the trait — D1 has at most a
        // few thousand SHIP/SLAP rows total (one per topic-advertisement
        // in the whole network), and `sync_advertisements` only runs every
        // 15 minutes. The bandwidth cost is trivial; the trait-surface
        // cost of a new method per storage backend is higher.
        let results = match protocol {
            Protocol::Ship => {
                let records = self
                    .ship_storage
                    .find_all_records()
                    .await
                    .map_err(|e| AdvertiserError::LookupFailed(e.to_string()))?;
                records
                    .into_iter()
                    .filter(|r| r.identity_key == identity && r.domain == self.hosting_url)
                    .map(|r| Advertisement {
                        protocol: Protocol::Ship,
                        identity_key: r.identity_key,
                        domain: r.domain,
                        topic_or_service: r.topic,
                        beef: None,
                        output_index: None,
                    })
                    .collect()
            }
            Protocol::Slap => {
                let records = self
                    .slap_storage
                    .find_all_records()
                    .await
                    .map_err(|e| AdvertiserError::LookupFailed(e.to_string()))?;
                records
                    .into_iter()
                    .filter(|r| r.identity_key == identity && r.domain == self.hosting_url)
                    .map(|r| Advertisement {
                        protocol: Protocol::Slap,
                        identity_key: r.identity_key,
                        domain: r.domain,
                        topic_or_service: r.service,
                        beef: None,
                        output_index: None,
                    })
                    .collect()
            }
        };
        Ok(results)
    }

    async fn revoke_advertisements(
        &self,
        _advertisements: &[Advertisement],
    ) -> Result<TaggedBEEF, AdvertiserError> {
        // v1: no-op. Revoke requires a signed PushDrop unlocker, which we
        // haven't ported yet (see module docs). Returning empty topics
        // keeps `Engine::sync_advertisements` from submitting a bogus
        // BEEF; the stale ads stay on-chain until they naturally age out.
        Ok(TaggedBEEF::new(vec![], vec![]))
    }

    fn parse_advertisement(&self, output_script: &[u8]) -> Option<Advertisement> {
        // Reuse the stock parser — field layout is identical.
        self.inner.parse_advertisement(output_script)
    }
}
