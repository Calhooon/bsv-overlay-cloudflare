//! `GET /credit-beef/:txid` — the ancestry a WALLET needs to credit a payout,
//! assembled server-side so the client never has to hold it.
//!
//! ## Why this exists
//!
//! Crediting a LOW payout is `wallet.internalizeAction`, and the wallet
//! VALIDATES the BEEF it is handed. Client-side that BEEF was built by
//! `stake.ts allKnownAncestry()` — our hop's BEEF + the peer hop's BEEF + the
//! JOIN raw + the subject raw — which meant every player's browser had to STORE
//! the ancestry bytes to survive a reload. On 2026-08-12 one player's ante was
//! funded from a coin whose parent transaction is 168,797 bytes (a foreign
//! app's covenant spend), so that record alone was ~340 KB of a ~5 MB
//! localStorage quota: about seven hands to exhaustion, and `storage-full` is
//! the one storage failure that can cost a player money.
//!
//! Every byte of that ancestry is ALREADY ours, server-side: the hops, the
//! JOIN and the settle are all transactions LOW broadcast and submitted, so
//! they sit in `transactions` / `pot_beefs` — INCLUDING the foreign parent,
//! which rode in with the hop's BEEF at submit time. The browser copy was
//! redundant. This route assembles the same set from the index and hands back
//! one blob.
//!
//! ## Why not `/lookup` with `x-history-depth`
//!
//! That surface exists and looks like the right answer, but measured against
//! prod on 2026-08-12 it is BROKEN and dangerous: for `ls_low` gameId
//! `73de6e48…`, no header returns 1,172 B / 1 tx / **1 bump (proven)**, while
//! `x-history-depth: 3` returns 676 B / 1 tx / **0 bumps** — asking for more
//! provenance STRIPS the merkle proof and yields something no wallet can
//! verify. Depths 1/2/3 are byte-identical, so the traversal never runs. The
//! engine's `hydrate_utxo_history` re-serialises from
//! `Transaction::from_beef(beef, None)` (dropping the stored BUMP) and walks
//! `outputs_consumed -> storage.find_output`, i.e. ADMITTED OUTPUTS ONLY — so a
//! foreign parent that was never admitted to a topic is invisible to it by
//! construction. Both faults are exactly what this module must not repeat:
//! **bumps are preserved, and the walk follows the TRANSACTION GRAPH, not the
//! admitted-output graph.**
//!
//! ## The walk
//!
//! Start from the subject's stored BEEF; while any transaction in hand is
//! UNPROVEN and one of its input txids is absent, fetch that parent's stored
//! BEEF and merge it. A transaction carrying a bump TERMINATES its branch —
//! that is the BEEF validity rule, and it is why a mined ancestor costs a
//! merkle path instead of a raw transaction.
//!
//! Bounded on both axes (`MAX_FETCHES`, `MAX_ROUNDS`): this is a public
//! read surface and the walk is driven by attacker-influenceable bytes.
//! Exhausting a bound is reported as `complete: false`, never as success —
//! an incomplete BEEF must be a retry to the caller, never a silent partial
//! that the wallet then rejects for reasons the client cannot see.

use bsv_rs::transaction::Beef;
use std::collections::HashSet;

/// Hard cap on parent BEEFs fetched for one request. A LOW credit chain is
/// subject -> JOIN -> two hops -> their parents: well under ten. The cap is a
/// DoS bound, not a shape assumption.
pub const MAX_FETCHES: usize = 32;

/// Hard cap on walk rounds (each round resolves one generation).
pub const MAX_ROUNDS: usize = 8;

/// What the walk still needs, and whether it finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wanted {
    /// Parent txids present in the BEEF's dependency set but absent from it.
    pub txids: Vec<String>,
    /// True when every unproven transaction's inputs are already in hand.
    pub complete: bool,
}

/// The txids this BEEF still needs before a verifier could walk it to proofs.
///
/// A transaction with a bump is PROVEN and terminates its branch, so its
/// inputs are deliberately NOT requested — chasing them would re-fetch
/// ancestry the merkle path already settles, which is precisely the bloat the
/// client was carrying.
///
/// Pure: no I/O, so the walk's decisions are testable without a database.
pub fn wanted_parents(beef: &Beef) -> Wanted {
    let present: HashSet<String> = beef
        .txs
        .iter()
        .filter(|t| !t.is_txid_only())
        .map(|t| t.txid().to_ascii_lowercase())
        .collect();

    let mut txids: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for t in &beef.txs {
        // Proven → branch terminated. Txid-only → we hold no inputs to chase.
        if t.bump_index().is_some() || t.is_txid_only() {
            continue;
        }
        for parent in &t.input_txids {
            let p = parent.to_ascii_lowercase();
            if present.contains(&p) || !seen.insert(p.clone()) {
                continue;
            }
            txids.push(p);
        }
    }
    Wanted {
        complete: txids.is_empty(),
        txids,
    }
}

/// Merge a fetched parent BEEF into the accumulator, preserving its bumps.
///
/// Returns false when the bytes do not parse — a corrupt/foreign row must be
/// skipped, never allowed to poison the accumulator, and never treated as a
/// resolved parent (so the walk reports `complete: false` rather than serving
/// an ancestry with a silent hole).
pub fn merge_parent(acc: &mut Beef, parent_bytes: &[u8]) -> bool {
    match Beef::from_binary(parent_bytes) {
        Ok(parent) => {
            acc.merge_beef(&parent);
            true
        }
        Err(_) => false,
    }
}

/// The walk as a STATE MACHINE, so the route (async D1) and a unit test
/// (in-memory map) drive the identical decisions.
///
/// This shape exists because the loop could not be proven any other way: every
/// BEEF in the index today is already self-sufficient — the client submits
/// complete ancestry, because it holds the local blob we are removing — so
/// probing prod/beta returns `fetches: 0` for every subject and never exercises
/// a single fetch. Testing the loop against live data would have been testing
/// nothing. Keeping the decisions here, and the I/O in the caller, is what
/// makes the interesting path reachable.
pub struct Walk {
    acc: Beef,
    fetches: usize,
    rounds: usize,
    complete: bool,
}

impl Walk {
    pub fn new(subject: Beef) -> Self {
        Self {
            acc: subject,
            fetches: 0,
            rounds: 0,
            complete: false,
        }
    }

    /// The parents to fetch next, or `None` when the walk is finished or has
    /// hit a bound. Finishing sets `complete`; hitting a bound does NOT — the
    /// difference is the whole contract of the endpoint's `complete` flag.
    pub fn next_wanted(&mut self) -> Option<Vec<String>> {
        if self.rounds >= MAX_ROUNDS || self.fetches >= MAX_FETCHES {
            return None;
        }
        let wanted = wanted_parents(&self.acc);
        if wanted.complete {
            self.complete = true;
            return None;
        }
        self.rounds += 1;
        Some(
            wanted
                .txids
                .into_iter()
                .take(MAX_FETCHES - self.fetches)
                .collect(),
        )
    }

    /// Record the outcome of one fetch. `None` = we hold no such row (a
    /// foreign tx never submitted through us); it still COUNTS as a fetch, so
    /// an absent parent can never spin the loop.
    pub fn absorb(&mut self, bytes: Option<&[u8]>) -> bool {
        self.fetches += 1;
        match bytes {
            Some(b) => merge_parent(&mut self.acc, b),
            None => false,
        }
    }

    pub fn fetches(&self) -> usize {
        self.fetches
    }
    pub fn is_complete(&self) -> bool {
        self.complete
    }
    pub fn into_beef(self) -> Beef {
        self.acc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE 2026-08-12 INCIDENT HOP, verbatim: `69c4102274064cd8…`, the ante
    /// whose single parent `ee5e99de…` is 168,797 bytes. Real bytes, so the
    /// input-txid extraction under test is the real parser's rather than a
    /// fixture's idea of one (Rule 18: a fixture built from the same model as
    /// the code only confirms the model).
    const INCIDENT_HOP_RAW: &str = "0100000001b1a0bb6b4c62bf1bffdbe27d45f2511069061de45e702a4f9a0bc2d0de995eee030000006a47304402200585003c4a6b902062cfcc76a3b47d15d218a1d32d156964888980eddbf053d6022067de00d7ee97f0543a49fe955220c684f3590cd1e140d5eb41ecab35abfdd457412102ee19faf46ce9e22e0a995beae8808018fb38378a5965b05e75e52abc1a3034fbffffffff03de4e0000000000001976a9140dca95d31a829ba0cde9a5f8b8820f7798c14d7288ac0000000000000000fd3601006a0f4c4f572f686f7070617274792f76312103ebc4875c6b93a4a02be26bf28ca25876419e9db70bdfa10716a14f68cf62669c21028fe3444b34832fb0d83f67c9f550189260e165412fc431d6d4bd87ec8aa614a020746875807b50c1893c2eeb2ef19f24cb265dc8f3137475a7ed34be88cfa2358c040000000008de4e000000000000210250e15ab9dc0ffca5609523222c89e163ce02a7d22a5164d3dd40135a02a59be14630440220337d55d598b9551a8078e7b3c05a689b0624c54ae82a214f80542cb96cf46c0a02207e29b49d30b74f7103b699b19ac834092bd378b83bf21cde9e646dfad67d6c9d473045022100d6e479601d854fabfe79bcd757d4ff44aacd412e9dc044189203ff50f20a727602204e8ff4091462d6303bbf16bfdf1d611dd61f0f31bd9f40716369bbb6b11981a143220000000000001976a914532b02700864d959e6c0b4850845cce4d45ae19d88ac00000000";
    /// The parent that started all this — `INCIDENT_HOP_RAW`'s only input.
    const INCIDENT_PARENT: &str =
        "ee5e99ded0c20b9a4f2a705ee41d06691051f2457de2dbff1bbf624c6bbba0b1";

    fn incident_hop_bytes() -> Vec<u8> {
        hex::decode(INCIDENT_HOP_RAW).expect("incident hop fixture hex")
    }

    /// UNPROVEN: the walk must ask for the fat parent by name. This is the
    /// exact request the client used to satisfy out of localStorage.
    #[test]
    fn an_unproven_tx_asks_for_its_absent_parent() {
        let mut beef = Beef::new();
        beef.merge_raw_tx(incident_hop_bytes(), None);

        let w = wanted_parents(&beef);
        assert!(!w.complete, "a lone unproven hop is not credit-ready");
        assert_eq!(
            w.txids,
            vec![INCIDENT_PARENT.to_string()],
            "must ask for exactly the real parent"
        );
    }

    /// PROVEN: a bump terminates the branch, so the same transaction asks for
    /// NOTHING. This is the rule that makes a mined ancestor cost a merkle
    /// path instead of 168,797 bytes — and the one `x-history-depth` breaks by
    /// dropping the bump on re-serialisation.
    #[test]
    fn a_proven_tx_terminates_its_branch_and_asks_for_nothing() {
        let mut beef = Beef::new();
        // bump_index 0 marks it proven for the walk's purposes; the walk reads
        // `bump_index`, exactly as BEEF validity does.
        beef.merge_raw_tx(incident_hop_bytes(), Some(0));

        let w = wanted_parents(&beef);
        assert!(w.complete, "a proven tx needs no ancestry");
        assert!(w.txids.is_empty(), "asked for {:?}", w.txids);
    }

    // ── the fetch loop ─────────────────────────────────────────────────────
    //
    // A REAL beta pair, split into the two rows a post-change index would
    // hold: subject `cf887745…` (unproven, 541 B raw) spending parent
    // `ee219a98…` (PROVEN, carries its bump). Splitting a real BEEF rather
    // than hand-building one keeps the parser under test honest.

    /// Subject-only BEEF: `cf887745…` with no ancestry at all.
    const SPLIT_SUBJECT_BEEF: &str = "0200beef00010001000000015b7368b067708384d7fa31e3d517ff9570230424faef46df6d4c4e43989a21ee010000006a47304402203e23b3ca24a0cf1ed391e7902aea5f51d51fd10ad1f996cb5b977addd01b6c000220235c073dc2d224cc5f6aba82d839b729696a1178c73e92a2c70544692b53ae3741210345e3fcbf54a59eacd9a5458ca618c2746c62708450792d5e00629b944858fc80feffffff020000000000000000fd5301006a0f4c4f572f706f7470617274792f763221020d2811c5c949bab57b35facd753baabf697b1ba14a50469d416fdac0e37fc9b921030ab0a18b1b73fa264a7d27c7932fd5914ac036a18fc846dbf292f1780a9ef7752066b5658af772e0929278436bc48c46d879e1a80da6f7c760052ccfee00a4321e202321c524328840935e0e6b000eac4cd4389f769c77378f89a047f3040caac98304000000000485ae0e0021033e2ae2b2691c8d767508cb59352b7ed192c12c7810ad6749171d630a0106757b46304402200ba04f554b09750a5b460f6b7fc2b947df6746f3fe4f9612e048cca2d08a70e40220029256600c2a9be3c634409156eaa864639b3ce06be64bfc2c6d41928c6576e64730450221008bf01275931bb5fed15a04a1f85e1d7291b3b10541c4738c61268d2c35185cd6022076248deaba353ec618539901d8a5c889b5d8f10473e0f29d502d89b80079f3fbc9130000000000001976a91410a4c67bb3afaff0d0a4af5f5cdaf465271f328688ac00000000";
    const SPLIT_PARENT_TXID: &str =
        "ee219a98434e4c6ddf46effa2404237095ff17d5e331fad784837067b068735b";
    /// The parent row, WITH its merkle proof — the thing that terminates the walk.
    const SPLIT_PARENT_BEEF: &str = "0200beef01fe7fae0e000902fd4e01007c7f8bd29ce05f38a91cbc4110420e89eb78f02c922cf9d79e8b56d7fb5ce666fd4f01025b7368b067708384d7fa31e3d517ff9570230424faef46df6d4c4e43989a21ee01a6009388fcc2ecb0fe3104d27e6cddaa41ba2cb51f0297228b04e761fde2b1ad41aa0152005f6de1c34d63024e5c9e9225bea2244b58bbfa85230076224f3a8e8977f7f5840128009582d131ba4c87427ed5cf99a0d2db9cd4fb1b812fb6bed536b047b1256bc45a011500d56321c3caf1535be0f07a2eb4d1a76d039864812a910f4ccd8c13836bf6d8c1010b008b69220fb2ef400b519bf91a8ab8a94531e68508dffee3c14414c87903f048fe010400e1145fc01ccf9cf129306bd817dec66691d84dad309010cdc650cd81aea34d5401030058ad13189c61e2a093770fa35988b59eeb0b5bbedbefbccb011bc9ab16c982f4010000c94c6d5780544c4878936cd83f504e178cd9b03e01b3b2143985c0b2c78288460202f3ddf37d20dd9052b6335309057cf8b4e15ee0bc7bbc5be6690176f8457788cf0100010000000146284caba2ab96799399c54a62c322e5028df46be42763a608afead542bda325020000006a47304402203275aadb8069b30877bd6c78c53a3f0f93011d8bd3d1c6f36686c6dd41319bda022073ade4dbfd6614f6096d3087e18ae2b49d893081c69b23c8bb9f80bc4ffdeab3412103d804018a14cb2b3512738c66d5429a62c202db517fd985e5e5449d7c0737327ffeffffff020000000000000000ea006a0f4c4f572f706f7470617274792f763121020d2811c5c949bab57b35facd753baabf697b1ba14a50469d416fdac0e37fc9b921030ab0a18b1b73fa264a7d27c7932fd5914ac036a18fc846dbf292f1780a9ef77520f6b8e125557581119a2c6d30e3525f55645e6546ed4c872cc60d80c380d9e2f92066e65cfbd7568b9ed7f92c922cf078eb890e421041bc1ca9385fe09cd28b7f7c04000000000484ae0e00473045022100d5e4e0ebb45dbbf25b315494bd7275f4ebf1b93639f013a53628600fa37258f102207eeb937d931fe98af0e8667010f45faef44c7a35a19908e179f39b568e25eb9600140000000000001976a914f8ab3ba249717fdc080c7d6f474a9d2f9928077388ac00000000";

    /// Drive the walk exactly as the route does, against an in-memory store.
    fn drive(subject_hex: &str, store: &std::collections::HashMap<String, Vec<u8>>) -> Walk {
        let subject = Beef::from_binary(&hex::decode(subject_hex).expect("subject hex"))
            .expect("subject BEEF parses");
        let mut walk = Walk::new(subject);
        while let Some(wanted) = walk.next_wanted() {
            let mut progressed = false;
            for txid in wanted {
                if walk.absorb(store.get(&txid).map(|v| v.as_slice())) {
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }
        walk
    }

    /// THE PATH LIVE DATA CANNOT REACH: a subject stored WITHOUT its ancestry
    /// is walked to completion by fetching the parent, and the parent's PROOF
    /// survives the merge — the exact property `x-history-depth` destroys.
    #[test]
    fn the_walk_fetches_a_missing_parent_and_keeps_its_proof() {
        let mut store = std::collections::HashMap::new();
        store.insert(
            SPLIT_PARENT_TXID.to_string(),
            hex::decode(SPLIT_PARENT_BEEF).expect("parent hex"),
        );

        let walk = drive(SPLIT_SUBJECT_BEEF, &store);
        assert!(
            walk.is_complete(),
            "the walk must finish once the parent lands"
        );
        assert_eq!(walk.fetches(), 1, "exactly one parent was needed");

        let beef = walk.into_beef();
        assert_eq!(beef.txs.len(), 2, "subject + parent");
        assert!(
            !beef.bumps.is_empty(),
            "the parent's merkle proof MUST survive the merge — losing it is the \
             x-history-depth bug, and a wallet cannot verify a proofless BEEF"
        );
        let parent = beef
            .find_txid(SPLIT_PARENT_TXID)
            .expect("parent present in the assembled BEEF");
        assert!(
            parent.bump_index().is_some(),
            "the fetched parent must still be PROVEN after assembly"
        );
    }

    /// A parent we do not hold leaves the walk INCOMPLETE and, critically,
    /// TERMINATES it — an absent row must never spin the loop.
    #[test]
    fn an_absent_parent_ends_the_walk_incomplete() {
        let store = std::collections::HashMap::new(); // hold nothing
        let walk = drive(SPLIT_SUBJECT_BEEF, &store);
        assert!(
            !walk.is_complete(),
            "must not claim completeness it does not have"
        );
        assert_eq!(walk.fetches(), 1, "one attempt, then stop — never a spin");
    }

    /// Garbage parent bytes must not poison the accumulator, and must not be
    /// counted as resolved — an unparseable row leaves the walk INCOMPLETE
    /// rather than serving an ancestry with a silent hole.
    #[test]
    fn a_corrupt_parent_is_skipped_not_merged() {
        let mut acc = Beef::new();
        acc.merge_raw_tx(incident_hop_bytes(), None);
        let before = acc.txs.len();

        assert!(!merge_parent(&mut acc, &[0xde, 0xad, 0xbe, 0xef]));
        assert_eq!(
            acc.txs.len(),
            before,
            "corrupt bytes changed the accumulator"
        );
        assert!(
            !wanted_parents(&acc).complete,
            "a skipped parent must leave the walk incomplete, never complete"
        );
    }
}
