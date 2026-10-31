//! D2-ACTOR-F-FOLD (bsv-low #959, the lens's LOW-2): the Worker's own statement for an owed note's publish read
//! (`owed_events::statement_of`). The overlay adds nothing: its events carry their rows (D2-ACTOR-F) and no CLOSURE.
//! The app layer, which holds the actor's family selections, adds the closure to each carrier's statement
//! (`low-app-layer` `owed_closure`, over `owed_actor::closure_column`); the overlay's carried filings (the SEEN latch,
//! a word, a refusal) name no txid their own key does not, so a warm actor's apply of them makes no miss.

/// The overlay's statement of `source`: none of its own ([`crate::owed_events::read_sql`]'s).
pub fn statement(_source: &str) -> Option<&'static str> {
    None
}
