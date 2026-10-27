//! Topic managers and lookup services for the BSV overlay network: SHIP,
//! SLAP, UHRP, the agent registry, delegation revocations and the LOW
//! topics, each a `TopicManager` and a `LookupService` of
//! `bsv-overlay-engine` over a storage trait with an in-memory store.

#![forbid(unsafe_code)]

pub mod advertiser;
/// Agent Registry: the topic manager, lookup service and storage of agent registrations.
pub mod agent;
pub mod arcade_words;
pub mod collected;
pub mod dm_delegation;
pub mod env_name;
pub mod hand;
/// bsv-low #848 (FAST-C): the served `batch` of a row the app layer's handfile family exploded.
pub mod handfile;
pub mod hopparty;
pub mod low;
pub mod owed_event;
pub mod pot;
pub mod potparty;
pub mod potrefund;
pub mod proof;
pub mod result;
pub mod reveal;
/// SHIP (Service Host Interconnect Protocol): the topic manager, lookup service and storage of SHIP advertisements.
pub mod ship;
/// SLAP (Service Lookup Availability Protocol): the topic manager, lookup service and storage of SLAP advertisements.
pub mod slap;
pub mod tx_facts;
pub mod uhrp;
pub mod validation;

pub use env_name::name_matches;
