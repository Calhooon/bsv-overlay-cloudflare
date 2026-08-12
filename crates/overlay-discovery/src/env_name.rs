//! Environment-suffixed topic / lookup-service name matching.
//!
//! A deployment may register its topics under an environment suffix
//! (`TOPIC_SUFFIX`, applied in `overlay-cloudflare`'s worker entry) so that a
//! beta stack and prod can never share an index row: prod registers `tm_low`,
//! beta registers `tm_low_beta`. See the `TOPIC_SUFFIX` block in
//! `overlay-cloudflare/src/lib.rs`.
//!
//! Every lookup service ALSO re-checks its own name internally — a
//! defence-in-depth guard behind the engine's exact-key dispatch. Those guards
//! must accept the suffixed form, or a suffixed deployment breaks in the worst
//! possible way: `output_admitted_by_topic` returns `Ok(())` on a name
//! mismatch, so `/submit` answers **200 while indexing nothing**, and the
//! lookup that a player's money recovery depends on stays empty forever.
//!
//! SHIP and SLAP are deliberately NOT suffixed anywhere (their names are
//! hardcoded in the engine's tracker bootstrap, ad suppression and peer
//! discovery), so their guards keep using plain equality.

/// True when `actual` is exactly `base`, or `base` followed by a non-empty
/// environment suffix (`tm_low` matches `tm_low` and `tm_low_beta`).
///
/// The separating underscore is load-bearing and must NOT be dropped in favour
/// of a plain `starts_with(base)`: `tm_pot` would then swallow `tm_potparty`
/// and `tm_potrefund`, and `tm_low` would swallow `tm_lowfund` — silently
/// filing one protocol's outputs into another protocol's store. Those three
/// pairs exist in this crate today, so the naive form is a live data-corruption
/// bug, not a hypothetical one.
pub fn name_matches(actual: &str, base: &str) -> bool {
    match actual.strip_prefix(base) {
        Some("") => true,
        Some(rest) => rest.starts_with('_') && rest.len() > 1,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::name_matches;

    #[test]
    fn exact_name_matches() {
        assert!(name_matches("tm_low", "tm_low"));
        assert!(name_matches("ls_pot", "ls_pot"));
    }

    #[test]
    fn suffixed_name_matches() {
        assert!(name_matches("tm_low_beta", "tm_low"));
        assert!(name_matches("ls_pot_beta", "ls_pot"));
        assert!(name_matches("tm_dm_delegation_beta", "tm_dm_delegation"));
    }

    /// The whole reason the separating underscore is required: these are real
    /// sibling topics in this crate, and a `starts_with` match would file
    /// `tm_potparty` outputs into the pot landing-proof store.
    #[test]
    fn sibling_protocols_never_match() {
        assert!(!name_matches("tm_potparty", "tm_pot"));
        assert!(!name_matches("tm_potrefund", "tm_pot"));
        assert!(!name_matches("tm_lowfund", "tm_low"));
        assert!(!name_matches("ls_potparty", "ls_pot"));
        assert!(!name_matches("ls_potrefund", "ls_pot"));
        assert!(!name_matches("ls_hopparty", "ls_hand"));
    }

    /// A suffixed sibling must not match a shorter base either — `tm_pot_beta`
    /// and `tm_potparty_beta` are different stores in the same deployment.
    #[test]
    fn suffixed_siblings_stay_distinct() {
        assert!(name_matches("tm_pot_beta", "tm_pot"));
        assert!(!name_matches("tm_potparty_beta", "tm_pot"));
        assert!(!name_matches("tm_pot_beta", "tm_potparty"));
    }

    #[test]
    fn unrelated_and_truncated_names_never_match() {
        assert!(!name_matches("tm_ship", "tm_low"));
        assert!(!name_matches("tm_lo", "tm_low"));
        assert!(!name_matches("", "tm_low"));
        // A bare trailing underscore is not a suffix.
        assert!(!name_matches("tm_low_", "tm_low"));
    }
}
