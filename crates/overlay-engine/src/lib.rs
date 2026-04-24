//! BSV Overlay Services Engine — core library.
//!
//! Provides the [`Engine`](engine::Engine) orchestrator, storage/topic/lookup traits,
//! GASP sync protocol, and all shared types for the overlay network.
//!
//! This crate is framework-agnostic — it contains zero HTTP or deployment logic.
//! Deployment targets (Cloudflare Workers, Axum, etc.) wrap this crate with HTTP routing.

#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::doc_markdown)] // backtick noise on BRC-XX, GASP, etc.
#![allow(clippy::wildcard_imports)] // common in test modules
#![allow(clippy::too_many_lines)] // engine.rs submit() is necessarily long
#![allow(clippy::cast_precision_loss)] // u64→f64 for scores is intentional
#![allow(clippy::cast_possible_truncation)] // u64→usize is fine on 64-bit
#![allow(clippy::items_after_statements)] // struct-in-function pattern in engine
#![allow(clippy::return_self_not_must_use)] // builder pattern methods
#![allow(clippy::match_same_arms)] // spend notification mode dispatch
#![allow(clippy::needless_bool_assign)] // clarity over brevity
#![allow(clippy::cast_sign_loss)] // f64→u64 for scores, negatives not meaningful
#![allow(clippy::let_and_return)] // sometimes clearer

pub mod advertiser;
pub mod broadcaster;
pub mod builder;
pub mod engine;
pub mod gasp;
pub mod gasp_overlay;
pub mod health_checker;
pub mod lookup_service;
pub mod storage;
pub mod topic_manager;
pub mod types;
