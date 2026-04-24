//! UHRP (Universal Hash Resolution Protocol) overlay discovery plugins.
//!
//! Provides `UHRPTopicManager` (tm_uhrp — validates PushDrop advertisements)
//! and `UHRPLookupService` (ls_uhrp — indexes + queries by uhrp_url /
//! identity_key).
//!
//! See `docs/uhrp_topic.md` for the PushDrop field layout, validation rules,
//! and an important note on expiry policy (strict reject).

pub mod lookup_service;
pub mod storage;
pub mod topic_manager;
