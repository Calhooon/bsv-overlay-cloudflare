//! `tm_pot` / `ls_pot` — LOW pot-spend landing-proof index.
//!
//! Lets a client ask the overlay "is pot outpoint `(txid:vout)` SPENT, and
//! by which txid?" — served from the engine's own spend bookkeeping,
//! replacing a browser → WhatsOnChain `/spent` read. This is the on-chain
//! landing proof LOW requires before crediting a settle/refund/sweep
//! (`app/src/lib/stake.ts`): the target outpoint must be spent *by* the
//! settle txid.
//!
//! # What it indexes — the pot covenant output, NOT its params
//!
//! The live pot lock is the `Poc5TemplatePot` COVENANT (bsv-low #103): a
//! 2-of-3 SETTLE-key multisig PLUS an in-script mandate that any spend pay
//! one of four templates (winner-A / winner-B / tie / height-gated refund).
//! Its compiled locking script is a fixed 122-byte HEAD, then 10 per-game
//! param pushes (`<pubA><pubB><pubTower><payPkhA><payPkhB><rakePkh>
//! <stakeA><stakeB><feeSats><recoveryHeight>`, variable length), then a
//! fixed 2850-byte contract-code TAIL. The topic manager admits an output
//! IFF its locking script IS that covenant — HEAD-prefix + TAIL-suffix
//! match ([`is_pot_covenant_script`]). The 2850-byte tail is collision-free,
//! so a false positive is not a practical concern.
//!
//! # The ONE difference from `reveal` — persist the spender
//!
//! This module is a near-copy of [`super::reveal`] with a single behavioral
//! change. `reveal` indexes a provably-unspendable `OP_RETURN` and treats
//! spend/eviction as no-ops (a reveal is a permanent fact that is never
//! spent). The pot output, by contrast, IS spent — that spend is the whole
//! point. So `ls_pot` opts into spend notifications
//! ([`SpendNotificationMode::WholeTx`](overlay_engine::types::SpendNotificationMode))
//! and, on spend, PERSISTS the spender: it marks the record `spent = true`
//! and records the `spendingTxid` (parsed out of the spending BEEF). The
//! record is NEVER deleted — a landing proof is permanent history the way
//! a reveal is, just with an extra "and here is the tx that spent it" fact.
//!
//! The engine fires the spend notification for the pot input whenever the
//! SPENDING (settle) tx is submitted, regardless of whether the spender
//! admits any new outputs — so submitting the settle to `tm_pot` (its
//! P2PKH outputs admit nothing) still records the spend against the pot.
//!
//! # The durable BEEF store (`pot_beefs`)
//!
//! Both hooks run in whole-tx mode so the service also durably stores the
//! full BEEF of every pot FUNDING tx (on admit) and every pot-SPENDING
//! settle/refund tx (on spend), keyed by each tx's own txid. This exists
//! because the engine's `transactions` table is lifecycle-managed — a BEEF
//! row is only written by `insert_output` (a settle admits no outputs, so it
//! never gets one) and is deleted by the deep-delete when a spent unretained
//! coin is cleaned up. `pot_beefs` is the durable source `low-app-layer`'s
//! `/beef/:txid` serves. See [`storage::PotStorage::store_beef`] for the
//! longer-wins/never-clobber write rule.
//!
//! # Lookup (`ls_pot`)
//!
//! Query JSON (tagged by `type`):
//!
//! ```json
//! {"type": "spentStatus", "outpoints": [{"txid": "<hex>", "vout": 0}, ...]}
//! ```
//!
//! The answer is a freeform, input-ordered JSON array — one entry per
//! requested outpoint:
//!
//! ```json
//! [{"txid": "<hex>", "vout": 0, "known": true, "spent": true,
//!   "spendingTxid": "<hex>"}]
//! ```
//!
//! `known` = a record exists (the output was admitted to `tm_pot`); a
//! missing record is fail-safe: `{"known": false, "spent": null,
//! "spendingTxid": null}` (never assert "unspent" for an output we never
//! saw).

use std::sync::OnceLock;

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;

/// The compiled `Poc5TemplatePot` covenant template, copied byte-for-byte
/// from the canonical source
/// `~/bsv/bsv-low/crates/low-spend/src/poc5_template.hex` (6034 chars).
///
/// It is hex EXCEPT for 10 angle-bracket markers (`<pubA>` … `<recoveryHeight>`)
/// standing in for the variable per-game param pushes. The fixed HEAD is the
/// hex before the first `<`; the fixed TAIL is the hex after the last `>`.
pub const POC5_TEMPLATE_HEX: &str = include_str!("poc5_template.hex");

/// Drift guard: sha256 of [`POC5_TEMPLATE_HEX`]. Pinned so that if the
/// canonical LOW template ever changes, the copied bytes here fail the pin
/// (`template_hex_matches_pin`) and the recognizer must be re-derived rather
/// than silently indexing the wrong lock. Canonical source:
/// `~/bsv/bsv-low/crates/low-spend/src/poc5_template.hex`.
pub const POC5_TEMPLATE_SHA256: &str =
    "8010d9add051b3cf1f6c4f0125c71fe417dc309239f8c4799a49905649608002";

/// Fixed HEAD bytes = the template hex before the first `<` param marker,
/// hex-decoded (122 bytes). Cached after the first decode.
fn head_bytes() -> &'static [u8] {
    static HEAD: OnceLock<Vec<u8>> = OnceLock::new();
    HEAD.get_or_init(|| {
        let i = POC5_TEMPLATE_HEX
            .find('<')
            .expect("poc5 template must contain a '<' param marker");
        hex::decode(&POC5_TEMPLATE_HEX[..i]).expect("poc5 template HEAD must be valid hex")
    })
}

/// Fixed TAIL bytes = the template hex after the last `>` param marker,
/// hex-decoded (2850 bytes). Cached after the first decode.
fn tail_bytes() -> &'static [u8] {
    static TAIL: OnceLock<Vec<u8>> = OnceLock::new();
    TAIL.get_or_init(|| {
        let j = POC5_TEMPLATE_HEX
            .rfind('>')
            .expect("poc5 template must contain a '>' param marker");
        hex::decode(&POC5_TEMPLATE_HEX[j + 1..]).expect("poc5 template TAIL must be valid hex")
    })
}

/// True iff `s` is a `Poc5TemplatePot` covenant locking script.
///
/// A covenant lock is the fixed HEAD, then the 10 variable per-game param
/// pushes, then the fixed contract-code TAIL. We recognize it structurally:
/// `s` must be at least `HEAD.len() + TAIL.len()` bytes and both begin with
/// HEAD and end with TAIL. The variable middle (the param pushes) is not
/// examined — any param values are admitted, exactly as the on-chain
/// contract accepts any committed params. The 2850-byte TAIL makes an
/// accidental collision with a non-covenant script not a practical concern.
pub fn is_pot_covenant_script(s: &[u8]) -> bool {
    let head = head_bytes();
    let tail = tail_bytes();
    s.len() >= head.len() + tail.len() && s.starts_with(head) && s.ends_with(tail)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_hex_matches_pin() {
        // Drift guard: the copied template must be byte-identical to the
        // canonical LOW source. If this fails, re-copy poc5_template.hex and
        // re-derive the recognizer (do NOT just bump the pin).
        let digest = bsv_rs::primitives::hash::sha256(POC5_TEMPLATE_HEX.as_bytes());
        assert_eq!(
            hex::encode(digest),
            POC5_TEMPLATE_SHA256,
            "poc5_template.hex drifted from the pinned canonical template"
        );
    }

    #[test]
    fn head_and_tail_have_expected_lengths() {
        // HEAD = 122 bytes, TAIL = 2850 bytes (per the Poc5TemplatePot layout).
        assert_eq!(head_bytes().len(), 122, "HEAD must decode to 122 bytes");
        assert_eq!(tail_bytes().len(), 2850, "TAIL must decode to 2850 bytes");
    }
}
