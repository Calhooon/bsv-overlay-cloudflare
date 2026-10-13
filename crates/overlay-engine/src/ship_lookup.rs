//! Platform transport for SHIP and SLAP discovery during a GASP tick.

use crate::types::{LookupQuestion, OutputListItem};
use async_trait::async_trait;

/// Maximum additional `ls_ship` providers queried in one sync invocation.
/// Configured trackers are always queried; advertised providers share this
/// allowance across topics. This caps lookup hosts, never advertised peers.
pub const SHIP_DISCOVERY_MAX_HOSTS_PER_TICK: usize = 4;

/// A platform's bounded `POST /lookup` client. Non-output answers are empty.
/// The Worker supplies fetch and its deadline; native tests supply records.
#[async_trait(?Send)]
pub trait ShipLookupClient {
    async fn lookup(
        &self,
        host: &str,
        question: &LookupQuestion,
    ) -> Result<Vec<OutputListItem>, String>;
}
