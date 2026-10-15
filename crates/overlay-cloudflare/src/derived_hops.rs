//! bsv-low #638: lookup rows derived transactionally with the source writes.
//! Triggers cover admission, relatching, spend reversal and eviction in either arrival order.

/// The derived table linking an identity to the pots its hop markers name.
pub const LINKS_CREATE: &str = r"CREATE TABLE IF NOT EXISTS hop_pot_links (identity TEXT NOT NULL, createdAt INTEGER, potTxid TEXT NOT NULL, potVout INTEGER NOT NULL, markerRowid INTEGER NOT NULL, PRIMARY KEY (markerRowid,potTxid,potVout))";

/// The links by identity and creation time.
pub const LINKS_INDEX: &str = r"CREATE INDEX IF NOT EXISTS idx_hop_pot_links_identity_created ON hop_pot_links(identity,createdAt)";

/// The links by pot.
pub const LINKS_POT_INDEX: &str =
    r"CREATE INDEX IF NOT EXISTS idx_hop_pot_links_pot ON hop_pot_links(potTxid,potVout,identity)";

/// The derived table of the hop markers seated on each pot.
pub const SEATS_CREATE: &str = r"CREATE TABLE IF NOT EXISTS pot_hop_seats (potTxid TEXT NOT NULL,potVout INTEGER NOT NULL,markerRowid INTEGER NOT NULL,PRIMARY KEY (potTxid,potVout,markerRowid))";

/// The seats by marker.
pub const SEATS_MARKER_INDEX: &str =
    r"CREATE INDEX IF NOT EXISTS idx_pot_hop_seats_marker ON pot_hop_seats(markerRowid)";

/// Pots by seat A's settle key.
pub const POT_A_INDEX: &str = r"CREATE INDEX IF NOT EXISTS idx_pot_pub_a ON pot_records(pubA)";

/// Pots by seat B's settle key.
pub const POT_B_INDEX: &str = r"CREATE INDEX IF NOT EXISTS idx_pot_pub_b ON pot_records(pubB)";

/// The derivation's state: the backfills already run, by name.
pub const STATE_CREATE: &str =
    r"CREATE TABLE IF NOT EXISTS view_derivation_state (name TEXT PRIMARY KEY)";

/// Derive a new hop marker's links and seats.
pub const HOP_INSERT: &str = r"CREATE TRIGGER IF NOT EXISTS derive_hop_insert AFTER INSERT ON hopparty_records BEGIN INSERT OR IGNORE INTO hop_pot_links (identity,createdAt,potTxid,potVout,markerRowid) SELECT hp.identity, hp.createdAt, pot.txid, pot.outputIndex, hp.rowid
FROM hopparty_records hp CROSS JOIN pot_records hop ON hop.txid=hp.txid AND hop.outputIndex=hp.hopVout
CROSS JOIN pot_records pot ON pot.txid=lower(hop.spendingTxid) AND pot.outputIndex=0
WHERE hp.markerValid=1 AND hop.spent=1 AND hop.spendingTxid IS NOT NULL
AND pot.paramsDecoded=1 AND pot.recoveryHeight IS NOT NULL
AND pot.pubA IS NOT NULL AND pot.pubB IS NOT NULL
AND hp.seatSettlePubkey IN (pot.pubA,pot.pubB)
AND NOT EXISTS (SELECT 1 FROM potparty_records x WHERE +x.identity=hp.identity AND x.potTxid=pot.txid AND x.potVout=pot.outputIndex) AND hp.rowid=NEW.rowid; INSERT OR IGNORE INTO pot_hop_seats (potTxid,potVout,markerRowid) SELECT p.txid,p.outputIndex,hp.rowid FROM hopparty_records hp CROSS JOIN pot_records p ON (p.pubA=hp.seatSettlePubkey OR p.pubB=hp.seatSettlePubkey) WHERE p.pubA IS NOT NULL AND p.pubB IS NOT NULL AND hp.rowid=NEW.rowid; END";

/// Derive a new pot's links and seats.
pub const POT_INSERT: &str = r"CREATE TRIGGER IF NOT EXISTS derive_pot_insert AFTER INSERT ON pot_records BEGIN INSERT OR IGNORE INTO hop_pot_links (identity,createdAt,potTxid,potVout,markerRowid) SELECT hp.identity, hp.createdAt, pot.txid, pot.outputIndex, hp.rowid
FROM hopparty_records hp CROSS JOIN pot_records hop ON hop.txid=hp.txid AND hop.outputIndex=hp.hopVout
CROSS JOIN pot_records pot ON pot.txid=lower(hop.spendingTxid) AND pot.outputIndex=0
WHERE hp.markerValid=1 AND hop.spent=1 AND hop.spendingTxid IS NOT NULL
AND pot.paramsDecoded=1 AND pot.recoveryHeight IS NOT NULL
AND pot.pubA IS NOT NULL AND pot.pubB IS NOT NULL
AND hp.seatSettlePubkey IN (pot.pubA,pot.pubB)
AND NOT EXISTS (SELECT 1 FROM potparty_records x WHERE +x.identity=hp.identity AND x.potTxid=pot.txid AND x.potVout=pot.outputIndex) AND hp.txid=NEW.txid AND hp.hopVout=NEW.outputIndex; INSERT OR IGNORE INTO hop_pot_links (identity,createdAt,potTxid,potVout,markerRowid) SELECT hp.identity, hp.createdAt, pot.txid, pot.outputIndex, hp.rowid
FROM pot_records pot CROSS JOIN pot_records hop INDEXED BY idx_pot_spending_lower ON lower(hop.spendingTxid)=pot.txid CROSS JOIN hopparty_records hp ON hp.txid=hop.txid AND hp.hopVout=hop.outputIndex
WHERE pot.outputIndex=0 AND hp.markerValid=1 AND hop.spent=1 AND hop.spendingTxid IS NOT NULL
AND pot.paramsDecoded=1 AND pot.recoveryHeight IS NOT NULL
AND pot.pubA IS NOT NULL AND pot.pubB IS NOT NULL
AND hp.seatSettlePubkey IN (pot.pubA,pot.pubB)
AND NOT EXISTS (SELECT 1 FROM potparty_records x WHERE +x.identity=hp.identity AND x.potTxid=pot.txid AND x.potVout=pot.outputIndex) AND pot.txid=NEW.txid AND pot.outputIndex=NEW.outputIndex AND lower(hop.spendingTxid)=lower(NEW.txid); INSERT OR IGNORE INTO pot_hop_seats (potTxid,potVout,markerRowid) SELECT p.txid,p.outputIndex,hp.rowid FROM pot_records p CROSS JOIN hopparty_records hp ON hp.seatSettlePubkey IN (p.pubA,p.pubB) WHERE p.pubA IS NOT NULL AND p.pubB IS NOT NULL AND p.txid=NEW.txid AND p.outputIndex=NEW.outputIndex; END";

/// Drop the links a new pot-party marker makes redundant.
pub const PARTY_INSERT: &str = r"CREATE TRIGGER IF NOT EXISTS derive_party_insert AFTER INSERT ON potparty_records BEGIN DELETE FROM hop_pot_links WHERE identity=NEW.identity AND potTxid=NEW.potTxid AND potVout=NEW.potVout; END";

/// Re-derive an updated hop marker's links and seats.
pub const HOP_UPDATE: &str = r"CREATE TRIGGER IF NOT EXISTS derive_hop_update AFTER UPDATE ON hopparty_records BEGIN DELETE FROM hop_pot_links WHERE markerRowid=OLD.rowid; DELETE FROM pot_hop_seats WHERE markerRowid=OLD.rowid; INSERT OR IGNORE INTO hop_pot_links (identity,createdAt,potTxid,potVout,markerRowid) SELECT hp.identity, hp.createdAt, pot.txid, pot.outputIndex, hp.rowid
FROM hopparty_records hp CROSS JOIN pot_records hop ON hop.txid=hp.txid AND hop.outputIndex=hp.hopVout
CROSS JOIN pot_records pot ON pot.txid=lower(hop.spendingTxid) AND pot.outputIndex=0
WHERE hp.markerValid=1 AND hop.spent=1 AND hop.spendingTxid IS NOT NULL
AND pot.paramsDecoded=1 AND pot.recoveryHeight IS NOT NULL
AND pot.pubA IS NOT NULL AND pot.pubB IS NOT NULL
AND hp.seatSettlePubkey IN (pot.pubA,pot.pubB)
AND NOT EXISTS (SELECT 1 FROM potparty_records x WHERE +x.identity=hp.identity AND x.potTxid=pot.txid AND x.potVout=pot.outputIndex) AND hp.rowid=NEW.rowid; INSERT OR IGNORE INTO pot_hop_seats (potTxid,potVout,markerRowid) SELECT p.txid,p.outputIndex,hp.rowid FROM hopparty_records hp CROSS JOIN pot_records p ON (p.pubA=hp.seatSettlePubkey OR p.pubB=hp.seatSettlePubkey) WHERE p.pubA IS NOT NULL AND p.pubB IS NOT NULL AND hp.rowid=NEW.rowid; END";

/// Re-derive an updated pot's links and seats.
pub const POT_UPDATE: &str = r"CREATE TRIGGER IF NOT EXISTS derive_pot_update AFTER UPDATE ON pot_records BEGIN DELETE FROM hop_pot_links WHERE potTxid=OLD.txid AND potVout=OLD.outputIndex; DELETE FROM hop_pot_links WHERE markerRowid IN (SELECT rowid FROM hopparty_records WHERE txid=OLD.txid AND hopVout=OLD.outputIndex); DELETE FROM pot_hop_seats WHERE potTxid=OLD.txid AND potVout=OLD.outputIndex; INSERT OR IGNORE INTO hop_pot_links (identity,createdAt,potTxid,potVout,markerRowid) SELECT hp.identity, hp.createdAt, pot.txid, pot.outputIndex, hp.rowid
FROM hopparty_records hp CROSS JOIN pot_records hop ON hop.txid=hp.txid AND hop.outputIndex=hp.hopVout
CROSS JOIN pot_records pot ON pot.txid=lower(hop.spendingTxid) AND pot.outputIndex=0
WHERE hp.markerValid=1 AND hop.spent=1 AND hop.spendingTxid IS NOT NULL
AND pot.paramsDecoded=1 AND pot.recoveryHeight IS NOT NULL
AND pot.pubA IS NOT NULL AND pot.pubB IS NOT NULL
AND hp.seatSettlePubkey IN (pot.pubA,pot.pubB)
AND NOT EXISTS (SELECT 1 FROM potparty_records x WHERE +x.identity=hp.identity AND x.potTxid=pot.txid AND x.potVout=pot.outputIndex) AND hp.txid=NEW.txid AND hp.hopVout=NEW.outputIndex; INSERT OR IGNORE INTO hop_pot_links (identity,createdAt,potTxid,potVout,markerRowid) SELECT hp.identity, hp.createdAt, pot.txid, pot.outputIndex, hp.rowid
FROM pot_records pot CROSS JOIN pot_records hop INDEXED BY idx_pot_spending_lower ON lower(hop.spendingTxid)=pot.txid CROSS JOIN hopparty_records hp ON hp.txid=hop.txid AND hp.hopVout=hop.outputIndex
WHERE pot.outputIndex=0 AND hp.markerValid=1 AND hop.spent=1 AND hop.spendingTxid IS NOT NULL
AND pot.paramsDecoded=1 AND pot.recoveryHeight IS NOT NULL
AND pot.pubA IS NOT NULL AND pot.pubB IS NOT NULL
AND hp.seatSettlePubkey IN (pot.pubA,pot.pubB)
AND NOT EXISTS (SELECT 1 FROM potparty_records x WHERE +x.identity=hp.identity AND x.potTxid=pot.txid AND x.potVout=pot.outputIndex) AND pot.txid=NEW.txid AND pot.outputIndex=NEW.outputIndex AND lower(hop.spendingTxid)=lower(NEW.txid); INSERT OR IGNORE INTO pot_hop_seats (potTxid,potVout,markerRowid) SELECT p.txid,p.outputIndex,hp.rowid FROM pot_records p CROSS JOIN hopparty_records hp ON hp.seatSettlePubkey IN (p.pubA,p.pubB) WHERE p.pubA IS NOT NULL AND p.pubB IS NOT NULL AND p.txid=NEW.txid AND p.outputIndex=NEW.outputIndex; END";

/// Re-derive the links an updated pot-party marker touches.
pub const PARTY_UPDATE: &str = r"CREATE TRIGGER IF NOT EXISTS derive_party_update AFTER UPDATE ON potparty_records BEGIN DELETE FROM hop_pot_links WHERE identity=NEW.identity AND potTxid=NEW.potTxid AND potVout=NEW.potVout; INSERT OR IGNORE INTO hop_pot_links (identity,createdAt,potTxid,potVout,markerRowid) SELECT hp.identity, hp.createdAt, pot.txid, pot.outputIndex, hp.rowid
FROM hopparty_records hp CROSS JOIN pot_records hop ON hop.txid=hp.txid AND hop.outputIndex=hp.hopVout
CROSS JOIN pot_records pot ON pot.txid=lower(hop.spendingTxid) AND pot.outputIndex=0
WHERE hp.markerValid=1 AND hop.spent=1 AND hop.spendingTxid IS NOT NULL
AND pot.paramsDecoded=1 AND pot.recoveryHeight IS NOT NULL
AND pot.pubA IS NOT NULL AND pot.pubB IS NOT NULL
AND hp.seatSettlePubkey IN (pot.pubA,pot.pubB)
AND NOT EXISTS (SELECT 1 FROM potparty_records x WHERE +x.identity=hp.identity AND x.potTxid=pot.txid AND x.potVout=pot.outputIndex) AND hp.identity=OLD.identity AND pot.txid=OLD.potTxid AND pot.outputIndex=OLD.potVout; END";

/// Drop a deleted hop marker's links and seats.
pub const HOP_DELETE: &str = r"CREATE TRIGGER IF NOT EXISTS derive_hop_delete AFTER DELETE ON hopparty_records BEGIN DELETE FROM hop_pot_links WHERE markerRowid=OLD.rowid; DELETE FROM pot_hop_seats WHERE markerRowid=OLD.rowid; END";

/// Drop a deleted pot's links and seats.
pub const POT_DELETE: &str = r"CREATE TRIGGER IF NOT EXISTS derive_pot_delete AFTER DELETE ON pot_records BEGIN DELETE FROM hop_pot_links WHERE potTxid=OLD.txid AND potVout=OLD.outputIndex; DELETE FROM hop_pot_links WHERE markerRowid IN (SELECT rowid FROM hopparty_records WHERE txid=OLD.txid AND hopVout=OLD.outputIndex); DELETE FROM pot_hop_seats WHERE potTxid=OLD.txid AND potVout=OLD.outputIndex; END";

/// Restore the links a deleted pot-party marker had made redundant.
pub const PARTY_DELETE: &str = r"CREATE TRIGGER IF NOT EXISTS derive_party_delete AFTER DELETE ON potparty_records BEGIN INSERT OR IGNORE INTO hop_pot_links (identity,createdAt,potTxid,potVout,markerRowid) SELECT hp.identity, hp.createdAt, pot.txid, pot.outputIndex, hp.rowid
FROM hopparty_records hp CROSS JOIN pot_records hop ON hop.txid=hp.txid AND hop.outputIndex=hp.hopVout
CROSS JOIN pot_records pot ON pot.txid=lower(hop.spendingTxid) AND pot.outputIndex=0
WHERE hp.markerValid=1 AND hop.spent=1 AND hop.spendingTxid IS NOT NULL
AND pot.paramsDecoded=1 AND pot.recoveryHeight IS NOT NULL
AND pot.pubA IS NOT NULL AND pot.pubB IS NOT NULL
AND hp.seatSettlePubkey IN (pot.pubA,pot.pubB)
AND NOT EXISTS (SELECT 1 FROM potparty_records x WHERE +x.identity=hp.identity AND x.potTxid=pot.txid AND x.potVout=pot.outputIndex) AND hp.identity=OLD.identity AND pot.txid=OLD.potTxid AND pot.outputIndex=OLD.potVout; END";

/// Backfill the links once (`hop-views-v1`).
pub const LINKS_BACKFILL: &str = r"INSERT OR IGNORE INTO hop_pot_links (identity,createdAt,potTxid,potVout,markerRowid) SELECT hp.identity, hp.createdAt, pot.txid, pot.outputIndex, hp.rowid
FROM (SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM view_derivation_state WHERE name='hop-views-v1')) g CROSS JOIN hopparty_records hp CROSS JOIN pot_records hop ON hop.txid=hp.txid AND hop.outputIndex=hp.hopVout
CROSS JOIN pot_records pot ON pot.txid=lower(hop.spendingTxid) AND pot.outputIndex=0
WHERE hp.markerValid=1 AND hop.spent=1 AND hop.spendingTxid IS NOT NULL
AND pot.paramsDecoded=1 AND pot.recoveryHeight IS NOT NULL
AND pot.pubA IS NOT NULL AND pot.pubB IS NOT NULL
AND hp.seatSettlePubkey IN (pot.pubA,pot.pubB)
AND NOT EXISTS (SELECT 1 FROM potparty_records x WHERE +x.identity=hp.identity AND x.potTxid=pot.txid AND x.potVout=pot.outputIndex)";

/// Backfill the seats once (`hop-views-v1`).
pub const SEATS_BACKFILL: &str = r"INSERT OR IGNORE INTO pot_hop_seats (potTxid,potVout,markerRowid) SELECT p.txid,p.outputIndex,hp.rowid FROM (SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM view_derivation_state WHERE name='hop-views-v1')) g CROSS JOIN pot_records p CROSS JOIN hopparty_records hp ON hp.seatSettlePubkey IN (p.pubA,p.pubB) WHERE p.pubA IS NOT NULL AND p.pubB IS NOT NULL";

/// Record the backfill as done.
pub const STATE_DONE: &str =
    r"INSERT OR IGNORE INTO view_derivation_state (name) VALUES ('hop-views-v1')";

/// Pots by lowercased spending txid.
pub const SPENDING_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS idx_pot_spending_lower ON pot_records(lower(spendingTxid))";
