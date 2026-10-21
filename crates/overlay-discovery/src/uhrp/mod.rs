//! UHRP (Universal Hash Resolution Protocol) overlay discovery plugins.
//!
//! Provides `UHRPTopicManager` (tm_uhrp — validates PushDrop advertisements)
//! and `UHRPLookupService` (ls_uhrp — indexes + queries by outpoint, uhrpUrl,
//! expiryTime or hostIdentityKey, behind the reference's door, bsv-low #819).
//!
//! See `docs/uhrp_topic.md` for the PushDrop field layout, validation rules,
//! and an important note on expiry policy (strict reject).

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;
