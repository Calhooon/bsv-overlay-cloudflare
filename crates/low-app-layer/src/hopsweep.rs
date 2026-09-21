//! bsv-low #469 (2026-09-19, the owed list's decision 3): THE HOP SWEEP IS
//! FILED like the refund backup, so a stranded funding hop is claimable from
//! ANY device the wallet runs on — the fifth `POST /record` family, and the
//! first that was never bought on chain.
//!
//! `LOW/hopsweep/v1` — six canonical pushes: the tag, the identity (33), the
//! game id (32), the hop txid (32), the hop vout (u32 LE), the pre-signed
//! sweep's raw bytes. NO marker signature: the network-verifiable proof is
//! already in hand (`~/bsv/epoch/NETWORK-ENFORCEMENT-RULES.md`, test (4)) —
//! the sweep's own input signature under the hop's P2PKH lock, which only the
//! seat's `[2,'low settle']` key can make, and the hop's admitted `hopparty`
//! marker (identity-signed, bsv-low #315) binds that lock's key to the
//! identity. The bar at the filing ([`bind_hop_sweep`]):
//!
//!   1. the index holds the hop's marker for the POSTER (`hopparty_records`
//!      keyed by the hop outpoint and the identity) and that marker's
//!      identity signature verifies (the latched `markerValid`, else the
//!      replay) — the unforgeable binding of the lock's key to the identity;
//!      no row yet is 425 (the JOIN-time race: the client's outbox re-files);
//!   2. the raw's FIRST input spends the hop outpoint the marker names;
//!   3. its unlock is exactly `[sig ‖ 0x41, seatSettlePubkey]`, the signature
//!      canonical low-S DER, verifying over the BIP-143 `SIGHASH_ALL|FORKID`
//!      digest of the hop's OWN lock (`P2PKH(hash160(seatSettlePubkey))`, the
//!      container's decoded lock when the row carries it) and value (the
//!      container's decoded value when the row carries it, else the marker's);
//!      a stranger cannot produce this (it needs the seat's key).
//!
//! Only a verified sweep is WRITTEN (no rank 0: a stranger must never be able
//! to occupy an identity's filing cap with unverifiable bytes). The row is
//! keyed by CONTENT (the same sweep re-filed is the same row); `/owed` serves
//! the bytes on the hop's row (`facts.sweepRawHex`) and, once the hop is spent
//! by that very sweep, turns the row into a `payout` (the credit BEEF of the
//! sweep). The client re-verifies everything before it broadcasts or credits
//! (`owedPress.ts`): the row steers, the bytes are judged again.
//!
//! What this does NOT decide: the sweep's destination. The seat signed it; the
//! seat chose where its own sats go (the client pays its BRC-29 pot home).

use bsv_rs::primitives::bsv::sighash::{compute_sighash_for_signing, parse_transaction, SighashParams, SIGHASH_ALL, SIGHASH_FORKID};
use overlay_discovery::hopparty::hopparty_identity_challenge;
use overlay_discovery::hopparty::validity::expected_hop_lock_hex;
use overlay_discovery::potparty::validity::{canonical_der, verify_identity_sig};

pub const HOPSWEEP_TAG: &[u8] = b"LOW/hopsweep/v1";
/// Sanity cap on the sweep raw push (a real sweep is ~200 bytes).
pub const HOPSWEEP_RAW_MAX_LEN: usize = 100_000;
/// The pushes of a canonical marker: tag, identity, gameId, hopTxid, hopVout, the raw.
pub const HOPSWEEP_PUSHES: usize = 6;

/// A decoded hop sweep filing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopsweepMarker {
    pub identity: Vec<u8>,
    pub game_id: [u8; 32],
    pub hop_txid: [u8; 32],
    pub hop_vout: u32,
    pub sweep_raw: Vec<u8>,
}

/// Parse the CANONICAL marker (the door already refused a non-canonical script; this re-walks the pushes and
/// checks every field's length). `None` = not a hop sweep marker.
pub fn parse_hopsweep_marker(script: &[u8]) -> Option<HopsweepMarker> {
    let pushes = crate::record_post::canonical_marker_pushes(script)?;
    if pushes.len() != HOPSWEEP_PUSHES || pushes[0] != HOPSWEEP_TAG {
        return None;
    }
    let identity = pushes[1];
    if identity.len() != 33 || !matches!(identity[0], 0x02 | 0x03) {
        return None;
    }
    let game_id: [u8; 32] = pushes[2].try_into().ok()?;
    let hop_txid: [u8; 32] = pushes[3].try_into().ok()?;
    let vout_b: [u8; 4] = pushes[4].try_into().ok()?;
    let sweep_raw = pushes[5];
    if sweep_raw.is_empty() || sweep_raw.len() > HOPSWEEP_RAW_MAX_LEN {
        return None;
    }
    Some(HopsweepMarker {
        identity: identity.to_vec(),
        game_id,
        hop_txid,
        hop_vout: u32::from_le_bytes(vout_b),
        sweep_raw: sweep_raw.to_vec(),
    })
}

/// The row `hopsweep_records` holds (every hex lowercase).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopsweepRecord {
    pub identity: String,
    pub game_id: String,
    pub hop_txid: String,
    pub hop_vout: u32,
    pub sweep_txid: String,
    pub sweep_raw_hex: String,
    /// The content key (`filed:` + 56 hex), the PK with `output_index` 0.
    pub txid: String,
    pub output_index: u32,
    pub created_at: i64,
}

/// A filing's key: identity ‖ game ‖ hop txid ‖ vout ‖ the sweep bytes (there is no signature to exclude).
pub fn hopsweep_content_key(m: &HopsweepMarker) -> String {
    let vout = m.hop_vout.to_le_bytes();
    crate::record_post::content_key(HOPSWEEP_TAG, &[&m.identity, &m.game_id, &m.hop_txid, &vout, &m.sweep_raw])
}

/// The txid of the sweep raw (display order, lowercase), or `None` when the raw does not parse.
pub fn sweep_txid_of(raw: &[u8]) -> Option<String> {
    let tx = bsv_rs::transaction::Transaction::from_binary(raw).ok()?;
    Some(tx.id().to_ascii_lowercase())
}

/// The record a verified marker writes (the txid computed from the bytes; `created_at` stamped by the route).
pub fn record_of(m: &HopsweepMarker) -> Option<HopsweepRecord> {
    Some(HopsweepRecord {
        identity: hex::encode(&m.identity),
        game_id: hex::encode(m.game_id),
        hop_txid: hex::encode(m.hop_txid),
        hop_vout: m.hop_vout,
        sweep_txid: sweep_txid_of(&m.sweep_raw)?,
        sweep_raw_hex: hex::encode(&m.sweep_raw),
        txid: hopsweep_content_key(m),
        output_index: 0,
        created_at: 0,
    })
}

/// Does the raw's FIRST input spend `hop_txid:hop_vout`? The cheap first refusal (the bar is [`bind_hop_sweep`]).
pub fn sweep_spends_hop(raw: &[u8], hop_txid: &[u8; 32], hop_vout: u32) -> bool {
    let Ok(tx) = bsv_rs::transaction::Transaction::from_binary(raw) else {
        return false;
    };
    let Some(input) = tx.inputs.first() else {
        return false;
    };
    let want = hex::encode(hop_txid);
    input.source_txid.as_deref().is_some_and(|t| t.eq_ignore_ascii_case(&want)) && input.source_output_index == hop_vout
}

/// The hop as the index holds it — ONE `hopparty_records` row for (the hop outpoint, the poster), read by the route
/// AFTER the marker parsed (a stranger's post costs no read of a hop it cannot name).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HopContext {
    pub identity: String,
    pub opponent_identity: String,
    pub game_id: String,
    pub hop_vout: u32,
    pub hop_sats: u64,
    pub seat_settle_pubkey: String,
    pub identity_sig_hex: String,
    /// The container's decoded lock (#310), when the row carries it.
    pub hop_lock_hex: Option<String>,
    /// The container's decoded value, when the row carries it.
    pub hop_sats_on_chain: Option<u64>,
    /// The overlay's latched verdict (`markerValid`), when it has run.
    pub marker_valid: Option<bool>,
}

/// Why a hop sweep filing is refused (mapped onto `RecordRefusal` by the door).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepRefusal {
    /// The index holds no hop marker for this outpoint and poster (yet): 425, the client re-files.
    HopNotIndexed,
    /// The hop's marker does not verify under the poster (not latched, and the replay fails): 425 until it does.
    HopMarkerUnverified,
    /// The raw's first input does not spend the hop the marker names.
    SweepDoesNotSpendTheHop,
    /// The raw is not the seat's spend of the hop (the unlock shape, the key, or the signature over the hop's lock
    /// and value fails).
    SweepNotTheSeatsSpend,
}

/// The sighash-type byte every LOW spend signs with.
const SIGHASH_ALL_FORKID_BYTE: u8 = (SIGHASH_ALL | SIGHASH_FORKID) as u8;

/// THE BAR (module docs): the poster's verified hop marker, the raw spending it, the seat's signature over the
/// hop's own lock and value. Pure over its inputs; the route reads the hop row.
pub fn bind_hop_sweep(m: &HopsweepMarker, hop: Option<&HopContext>) -> std::result::Result<(), SweepRefusal> {
    let Some(hop) = hop else {
        return Err(SweepRefusal::HopNotIndexed);
    };
    let identity_hex = hex::encode(&m.identity);
    if !hop.identity.eq_ignore_ascii_case(&identity_hex) || hop.hop_vout != m.hop_vout {
        return Err(SweepRefusal::HopNotIndexed); // the row is another identity's or another output's: not this poster's hop
    }
    // 1. the marker's identity binding: the latched verdict, else the replay
    let (Ok(opponent), Ok(seat_pub), Ok(identity_sig)) = (
        hex::decode(&hop.opponent_identity),
        hex::decode(&hop.seat_settle_pubkey),
        hex::decode(&hop.identity_sig_hex),
    ) else {
        return Err(SweepRefusal::HopMarkerUnverified);
    };
    if hop.marker_valid != Some(true) {
        let Some(challenge) = hopparty_identity_challenge(&m.identity, &opponent, &m.game_id, hop.hop_vout, hop.hop_sats, &seat_pub) else {
            return Err(SweepRefusal::HopMarkerUnverified);
        };
        if !verify_identity_sig(&m.identity, &m.game_id, &challenge, &identity_sig) {
            return Err(SweepRefusal::HopMarkerUnverified);
        }
    }
    // 2. the raw spends the hop
    if !sweep_spends_hop(&m.sweep_raw, &m.hop_txid, m.hop_vout) {
        return Err(SweepRefusal::SweepDoesNotSpendTheHop);
    }
    // 3. the seat's signature over the hop's lock and value
    let Some(expected_lock) = expected_hop_lock_hex(&seat_pub) else {
        return Err(SweepRefusal::SweepNotTheSeatsSpend);
    };
    if hop.hop_lock_hex.as_deref().is_some_and(|l| !l.eq_ignore_ascii_case(&expected_lock)) {
        return Err(SweepRefusal::SweepNotTheSeatsSpend); // the container's lock is not a P2PKH of the marker's key
    }
    let Ok(lock) = hex::decode(&expected_lock) else {
        return Err(SweepRefusal::SweepNotTheSeatsSpend);
    };
    let value = hop.hop_sats_on_chain.unwrap_or(hop.hop_sats);
    let Ok(parsed) = parse_transaction(&m.sweep_raw) else {
        return Err(SweepRefusal::SweepNotTheSeatsSpend);
    };
    let Some(input) = parsed.inputs.first() else {
        return Err(SweepRefusal::SweepNotTheSeatsSpend);
    };
    let Some(pushes) = crate::record_post::unlock_pushes(&input.script) else {
        return Err(SweepRefusal::SweepNotTheSeatsSpend);
    };
    if pushes.len() != 2 || pushes[1] != seat_pub.as_slice() {
        return Err(SweepRefusal::SweepNotTheSeatsSpend);
    }
    let Some((&sighash_byte, der)) = pushes[0].split_last() else {
        return Err(SweepRefusal::SweepNotTheSeatsSpend);
    };
    if sighash_byte != SIGHASH_ALL_FORKID_BYTE {
        return Err(SweepRefusal::SweepNotTheSeatsSpend);
    }
    let Some(sig) = canonical_der(der) else {
        return Err(SweepRefusal::SweepNotTheSeatsSpend);
    };
    let Ok(pk) = bsv_rs::primitives::ec::PublicKey::from_bytes(&seat_pub) else {
        return Err(SweepRefusal::SweepNotTheSeatsSpend);
    };
    let digest = compute_sighash_for_signing(&SighashParams {
        version: parsed.version,
        inputs: &parsed.inputs,
        outputs: &parsed.outputs,
        locktime: parsed.locktime,
        input_index: 0,
        subscript: &lock,
        satoshis: value,
        scope: SIGHASH_ALL | SIGHASH_FORKID,
    });
    if !sig.verify(&digest, &pk) {
        return Err(SweepRefusal::SweepNotTheSeatsSpend);
    }
    Ok(())
}

// ── the owed list's view of a filing ────────────────────────────────────────

/// A filed sweep as the owed derivation sees it (the newest per hop outpoint).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FiledHopSweep {
    pub sweep_txid: String,
    pub raw_hex: String,
    /// What the sweep pays out in all (the seat's one output: its home), when the raw parses.
    pub pays_sats: Option<u64>,
    /// bsv-low #517 (loop 19, pair 11, 2026-09-21): the INDEX holds this sweep with a chaintracks-VERIFIED proof
    /// (`transactions.has_proof`, the latch only the verified stitch sets; the word `/tx-any` serves and
    /// `/credit-beef` assembles the credit from). The owed walk confirms the swept payout on it before any hop row
    /// (never attributed to a sweep once the JOIN's eviction released its pointer) or courier word (an indexer lagging
    /// a big block, memoised five minutes) — false when the read faulted or skipped this pass.
    pub index_proven: bool,
    /// The block the verified bump names (`transactions.proofHeight`), when recorded.
    pub index_proof_height: Option<u64>,
}

/// The sweep's total output value (a sweep is 1-in/1-out: the hop minus the fee).
pub fn sweep_output_sats(raw_hex: &str) -> Option<u64> {
    let raw = hex::decode(raw_hex).ok()?;
    let tx = bsv_rs::transaction::Transaction::from_binary(&raw).ok()?;
    let mut sum = 0u64;
    for o in &tx.outputs {
        sum = sum.checked_add(o.satoshis?)?;
    }
    Some(sum)
}

// ── SQL (prepared against the production schema in `tests/sql_prepares_sqlite.rs`) ──

/// The table (overlay migration 158; the app layer issues the same statement as a catch-up).
pub const HOPSWEEP_CREATE: &str = "CREATE TABLE IF NOT EXISTS hopsweep_records (identity TEXT NOT NULL, gameId TEXT NOT NULL, hopTxid TEXT NOT NULL, hopVout INTEGER NOT NULL, sweepTxid TEXT NOT NULL, sweepRawHex TEXT NOT NULL, txid TEXT NOT NULL, outputIndex INTEGER NOT NULL, createdAt INTEGER, PRIMARY KEY (txid, outputIndex))";
pub const HOPSWEEP_IDX_IDENTITY: &str = "CREATE INDEX IF NOT EXISTS idx_hopsweep_identity ON hopsweep_records(identity)";
pub const HOPSWEEP_IDX_HOP: &str = "CREATE INDEX IF NOT EXISTS idx_hopsweep_hop ON hopsweep_records(hopTxid, hopVout)";

pub const HOPSWEEP_FILE_SQL: &str = "INSERT OR IGNORE INTO hopsweep_records \
     (identity, gameId, hopTxid, hopVout, sweepTxid, sweepRawHex, txid, outputIndex, createdAt) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
/// The per-hop cap count (poster, game, hop txid, the key being filed — excluded).
pub const HOPSWEEP_FILED_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM hopsweep_records \
     WHERE identity = ?1 AND gameId = ?2 AND hopTxid = ?3 AND txid LIKE 'filed:%' AND txid <> ?4";
pub const HOPSWEEP_FILED_TODAY_SQL: &str = "SELECT COUNT(*) AS n FROM hopsweep_records \
     WHERE identity = ?1 AND txid LIKE 'filed:%' AND createdAt >= ?2";
/// The chain no-op count: a sweep filing is never on chain, so this is 0 by construction (kept so the door's
/// ladder has one shape per family).
pub const HOPSWEEP_CHAIN_ROWS_SQL: &str = "SELECT COUNT(*) AS n FROM hopsweep_records \
     WHERE identity = ?1 AND gameId = ?2 AND hopTxid = ?3 AND txid NOT LIKE 'filed:%'";
/// The hop as the index holds it, for (the hop outpoint, the poster): the latched-valid row first.
pub const HOP_CONTEXT_SQL: &str = "SELECT identity, opponentIdentity, gameId, hopVout, hopSats, seatSettlePubkey, identitySigHex, \
            hopLockHex, hopSatsOnChain, markerValid \
     FROM hopparty_records WHERE txid = ?1 AND hopVout = ?2 AND identity = ?3 \
     ORDER BY COALESCE(markerValid, 0) DESC, createdAt ASC, rowid ASC LIMIT 1";
/// Every filed sweep of an identity, newest first (the owed recompute keeps the newest per hop outpoint).
pub const HOPSWEEPS_FOR_IDENTITY_SQL: &str = "SELECT hopTxid, hopVout, sweepTxid, sweepRawHex FROM hopsweep_records \
     WHERE identity = ?1 ORDER BY createdAt DESC, rowid DESC LIMIT 500";

/// bsv-low #517: the filed sweeps the index holds PROVEN, one chunk of txids per ask (the caller closes the `IN (`
/// list with its placeholders). `has_proof = 1` is the latch only the chaintracks-verified stitch sets (the admit
/// path always writes 0; a refuted bump resets it), `proofHeight` the block that bump names.
pub const SWEEP_PROOFS_SQL_HEAD: &str = "SELECT lower(txid) AS txid, proofHeight FROM transactions WHERE has_proof = 1 AND txid IN (";

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use bsv_rs::primitives::ec::PrivateKey;
    use bsv_rs::wallet::{Counterparty, CreateSignatureArgs, GetPublicKeyArgs, ProtoWallet};

    fn skey(scalar: u8) -> PrivateKey {
        let mut b = [0u8; 32];
        b[31] = scalar;
        PrivateKey::from_bytes(&b).expect("nonzero scalar")
    }
    fn wallet(seed: u8) -> ProtoWallet {
        ProtoWallet::new(Some(PrivateKey::from_hex(&hex::encode([seed; 32])).unwrap()))
    }
    fn identity(w: &ProtoWallet) -> Vec<u8> {
        hex::decode(
            w.get_public_key(GetPublicKeyArgs { identity_key: true, protocol_id: None, key_id: None, counterparty: None, for_self: None })
                .unwrap()
                .public_key,
        )
        .unwrap()
    }
    fn push(out: &mut Vec<u8>, data: &[u8]) {
        let n = data.len();
        if n < 0x4c {
            out.push(n as u8);
        } else if n <= 0xff {
            out.push(0x4c);
            out.push(n as u8);
        } else {
            out.push(0x4d);
            out.extend_from_slice(&(n as u16).to_le_bytes());
        }
        out.extend_from_slice(data);
    }
    fn script(pushes: &[&[u8]]) -> Vec<u8> {
        let mut out = vec![0x00, 0x6a];
        for p in pushes {
            push(&mut out, p);
        }
        out
    }
    const GID: [u8; 32] = [0x31; 32];
    const HOP: [u8; 32] = [0x51; 32];
    const HOP_SATS: u64 = 20_190;
    /// A 1-in/1-out raw: input `prev:vout` with `unlock`, one output of `out_sats` to `out_lock`.
    fn raw_1in_1out(prev: &[u8; 32], vout: u32, unlock: &[u8], out_sats: u64, out_lock: &[u8]) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&1i32.to_le_bytes());
        raw.push(1);
        let mut prev_le = *prev;
        prev_le.reverse(); // the wire carries the txid in internal byte order
        raw.extend_from_slice(&prev_le);
        raw.extend_from_slice(&vout.to_le_bytes());
        assert!(unlock.len() < 0xfd);
        raw.push(unlock.len() as u8);
        raw.extend_from_slice(unlock);
        raw.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        raw.push(1);
        raw.extend_from_slice(&out_sats.to_le_bytes());
        assert!(out_lock.len() < 0xfd);
        raw.push(out_lock.len() as u8);
        raw.extend_from_slice(out_lock);
        raw.extend_from_slice(&0u32.to_le_bytes());
        raw
    }
    /// The seat's sweep of `HOP:vout` (value `sats`, lock = P2PKH(seat)) to `dest`, signed by `signer` (the honest
    /// case: `signer` == the seat key).
    fn sweep_raw(seat: &PrivateKey, signer: &PrivateKey, vout: u32, sats: u64, dest: &[u8]) -> Vec<u8> {
        let seat_pub = seat.public_key().to_compressed();
        let lock = hex::decode(expected_hop_lock_hex(&seat_pub).unwrap()).unwrap();
        let skeleton = raw_1in_1out(&HOP, vout, &[], sats - 190, dest);
        let parsed = parse_transaction(&skeleton).unwrap();
        let digest = compute_sighash_for_signing(&SighashParams {
            version: parsed.version,
            inputs: &parsed.inputs,
            outputs: &parsed.outputs,
            locktime: parsed.locktime,
            input_index: 0,
            subscript: &lock,
            satoshis: sats,
            scope: SIGHASH_ALL | SIGHASH_FORKID,
        });
        let mut sig = signer.sign(&digest).unwrap().to_der();
        sig.push(SIGHASH_ALL_FORKID_BYTE);
        let mut unlock = Vec::new();
        push(&mut unlock, &sig);
        push(&mut unlock, &seat_pub);
        raw_1in_1out(&HOP, vout, &unlock, sats - 190, dest)
    }
    fn marker(id: &[u8], raw: &[u8]) -> HopsweepMarker {
        HopsweepMarker { identity: id.to_vec(), game_id: GID, hop_txid: HOP, hop_vout: 0, sweep_raw: raw.to_vec() }
    }
    /// The hop's admitted marker for the wallet: a REAL identity signature over the hopparty challenge.
    fn hop_ctx(w: &ProtoWallet, seat: &PrivateKey, on_chain: bool) -> HopContext {
        let id = identity(w);
        let opp = vec![0x03; 33];
        let seat_pub = seat.public_key().to_compressed();
        let challenge = hopparty_identity_challenge(&id, &opp, &GID, 0, HOP_SATS, &seat_pub).unwrap();
        let sig = w
            .create_signature(CreateSignatureArgs {
                data: Some(challenge),
                hash_to_directly_sign: None,
                protocol_id: bsv_rs::wallet::Protocol::new(bsv_rs::wallet::SecurityLevel::App, "low potparty"),
                key_id: hex::encode(GID),
                counterparty: Some(Counterparty::Anyone),
            })
            .unwrap()
            .signature;
        HopContext {
            identity: hex::encode(&id),
            opponent_identity: hex::encode(&opp),
            game_id: hex::encode(GID),
            hop_vout: 0,
            hop_sats: HOP_SATS,
            seat_settle_pubkey: hex::encode(seat_pub.as_slice()),
            identity_sig_hex: hex::encode(sig),
            hop_lock_hex: if on_chain { expected_hop_lock_hex(&seat_pub) } else { None },
            hop_sats_on_chain: if on_chain { Some(HOP_SATS) } else { None },
            marker_valid: None,
        }
    }
    const DEST: [u8; 25] = [0x76, 0xa9, 0x14, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x88, 0xac];

    #[test]
    fn the_marker_parses_from_its_six_canonical_pushes_and_nothing_else() {
        let w = wallet(0x21);
        let id = identity(&w);
        let raw = vec![0x01u8; 200];
        let s = script(&[HOPSWEEP_TAG, &id, &GID, &HOP, &7u32.to_le_bytes(), &raw]);
        let m = parse_hopsweep_marker(&s).expect("parses");
        assert_eq!((m.identity.as_slice(), m.game_id, m.hop_txid, m.hop_vout, m.sweep_raw.as_slice()), (id.as_slice(), GID, HOP, 7, raw.as_slice()));
        // a PUSHDATA2 raw (past 255 bytes) rides the same grammar
        let big = vec![0x02u8; 300];
        assert!(parse_hopsweep_marker(&script(&[HOPSWEEP_TAG, &id, &GID, &HOP, &0u32.to_le_bytes(), &big])).is_some());
        // wrong tag, a fifth or seventh push, a short identity, a 31-byte game id, an empty raw: not a marker
        assert!(parse_hopsweep_marker(&script(&[b"LOW/potrefund/v1", &id, &GID, &HOP, &0u32.to_le_bytes(), &raw])).is_none());
        assert!(parse_hopsweep_marker(&script(&[HOPSWEEP_TAG, &id, &GID, &HOP, &raw])).is_none());
        assert!(parse_hopsweep_marker(&script(&[HOPSWEEP_TAG, &id, &GID, &HOP, &0u32.to_le_bytes(), &raw, &[1u8]])).is_none());
        assert!(parse_hopsweep_marker(&script(&[HOPSWEEP_TAG, &id[..32], &GID, &HOP, &0u32.to_le_bytes(), &raw])).is_none());
        assert!(parse_hopsweep_marker(&script(&[HOPSWEEP_TAG, &id, &GID[..31], &HOP, &0u32.to_le_bytes(), &raw])).is_none());
        assert!(parse_hopsweep_marker(&script(&[HOPSWEEP_TAG, &id, &GID, &HOP, &0u32.to_le_bytes(), &[]])).is_none());
        // a non-canonical spelling (a PUSHDATA1 of a short push) is refused at the door
        let mut nc = vec![0x00, 0x6a, 0x4c, 15];
        nc.extend_from_slice(HOPSWEEP_TAG);
        assert!(parse_hopsweep_marker(&nc).is_none());
    }

    #[test]
    fn the_content_key_is_never_a_txid_and_follows_the_bytes() {
        let w = wallet(0x22);
        let id = identity(&w);
        let m = marker(&id, &[0x01; 100]);
        let k = hopsweep_content_key(&m);
        assert!(k.starts_with("filed:"));
        assert_eq!(k.len(), 62);
        assert_eq!(hopsweep_content_key(&m.clone()), k);
        assert_ne!(hopsweep_content_key(&marker(&id, &[0x02; 100])), k, "another sweep, another row");
        let other = HopsweepMarker { hop_vout: 1, ..m.clone() };
        assert_ne!(hopsweep_content_key(&other), k);
    }

    #[test]
    fn the_seats_own_sweep_binds_a_strangers_does_not() {
        let w = wallet(0x23);
        let id = identity(&w);
        let seat = skey(7);
        let honest = sweep_raw(&seat, &seat, 0, HOP_SATS, &DEST);
        let ctx = hop_ctx(&w, &seat, true);
        assert_eq!(bind_hop_sweep(&marker(&id, &honest), Some(&ctx)), Ok(()));
        // the record: the txid is the raw's own
        let r = record_of(&marker(&id, &honest)).unwrap();
        assert_eq!(r.sweep_txid, bsv_rs::transaction::Transaction::from_binary(&honest).unwrap().id().to_ascii_lowercase());
        assert_eq!(r.hop_txid, hex::encode(HOP));
        assert_eq!(sweep_output_sats(&r.sweep_raw_hex), Some(HOP_SATS - 190));
        // a stranger's signature under the seat's pubkey push
        let forged = sweep_raw(&seat, &skey(8), 0, HOP_SATS, &DEST);
        assert_eq!(bind_hop_sweep(&marker(&id, &forged), Some(&ctx)), Err(SweepRefusal::SweepNotTheSeatsSpend));
        // signed over another value than the hop's (the digest differs)
        let wrong_value = sweep_raw(&seat, &seat, 0, HOP_SATS + 1, &DEST);
        assert_eq!(bind_hop_sweep(&marker(&id, &wrong_value), Some(&ctx)), Err(SweepRefusal::SweepNotTheSeatsSpend));
        // a raw that spends another output of the hop
        let other_vout = sweep_raw(&seat, &seat, 1, HOP_SATS, &DEST);
        assert_eq!(bind_hop_sweep(&marker(&id, &other_vout), Some(&ctx)), Err(SweepRefusal::SweepDoesNotSpendTheHop));
        // garbage bytes
        assert_eq!(bind_hop_sweep(&marker(&id, &[0xaa; 40]), Some(&ctx)), Err(SweepRefusal::SweepDoesNotSpendTheHop));
        // no hop row: 425, the client re-files
        assert_eq!(bind_hop_sweep(&marker(&id, &honest), None), Err(SweepRefusal::HopNotIndexed));
    }

    #[test]
    fn the_hops_marker_must_verify_under_the_poster_the_latched_verdict_or_the_replay() {
        let w = wallet(0x24);
        let id = identity(&w);
        let seat = skey(9);
        let honest = sweep_raw(&seat, &seat, 0, HOP_SATS, &DEST);
        // the replay (no latch yet), with and without the container's decoded facts
        assert_eq!(bind_hop_sweep(&marker(&id, &honest), Some(&hop_ctx(&w, &seat, true))), Ok(()));
        assert_eq!(bind_hop_sweep(&marker(&id, &honest), Some(&hop_ctx(&w, &seat, false))), Ok(()));
        // a garbled identity signature and no latch: unverified (425)
        let mut garbled = hop_ctx(&w, &seat, true);
        garbled.identity_sig_hex = "30".repeat(35);
        assert_eq!(bind_hop_sweep(&marker(&id, &honest), Some(&garbled)), Err(SweepRefusal::HopMarkerUnverified));
        // the overlay's latched verdict stands in for the replay
        garbled.marker_valid = Some(true);
        assert_eq!(bind_hop_sweep(&marker(&id, &honest), Some(&garbled)), Ok(()));
        // a row naming another identity (the query would not return it; the bind still refuses)
        let mut other = hop_ctx(&w, &seat, true);
        other.identity = hex::encode(identity(&wallet(0x25)));
        assert_eq!(bind_hop_sweep(&marker(&id, &honest), Some(&other)), Err(SweepRefusal::HopNotIndexed));
        // the container's lock is not a P2PKH of the marker's key: nothing this key can sweep
        let mut foreign_lock = hop_ctx(&w, &seat, true);
        foreign_lock.hop_lock_hex = expected_hop_lock_hex(&skey(10).public_key().to_compressed());
        assert_eq!(bind_hop_sweep(&marker(&id, &honest), Some(&foreign_lock)), Err(SweepRefusal::SweepNotTheSeatsSpend));
        // a sweep whose pubkey push is another key (even with a valid signature under it) is not the seat's spend
        let other_seat = skey(11);
        let theirs = sweep_raw(&other_seat, &other_seat, 0, HOP_SATS, &DEST);
        assert_eq!(bind_hop_sweep(&marker(&id, &theirs), Some(&hop_ctx(&w, &seat, true))), Err(SweepRefusal::SweepNotTheSeatsSpend));
    }

    #[test]
    fn the_sql_names_the_tables_columns() {
        for sql in [HOPSWEEP_FILE_SQL, HOPSWEEP_FILED_ROWS_SQL, HOPSWEEP_FILED_TODAY_SQL, HOPSWEEP_CHAIN_ROWS_SQL, HOPSWEEPS_FOR_IDENTITY_SQL] {
            assert!(sql.contains("hopsweep_records"));
        }
        assert!(HOP_CONTEXT_SQL.contains("FROM hopparty_records") && HOP_CONTEXT_SQL.contains("markerValid"));
        // #517: the proof read names the engine's own latch, never the admit path's bytes
        assert!(SWEEP_PROOFS_SQL_HEAD.contains("FROM transactions") && SWEEP_PROOFS_SQL_HEAD.contains("has_proof = 1") && SWEEP_PROOFS_SQL_HEAD.ends_with("IN ("));
        assert_eq!(HOPSWEEP_FILE_SQL.matches('?').count(), 9);
        assert!(HOPSWEEP_CREATE.contains("PRIMARY KEY (txid, outputIndex)"));
    }
}
