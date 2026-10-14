//! Frozen words from keeper main de5cda7, over the unchanged parity submit
//! payloads and the production wire corpus. Every mode and a duplicate replay.
use bsv_overlay_discovery::ship::topic_manager::SHIPTopicManager;
use bsv_overlay_discovery::slap::topic_manager::SLAPTopicManager;
use bsv_overlay_engine::engine::{Engine, EngineConfig};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::topic_manager::TopicManager;
use bsv_overlay_engine::types::{SubmitMode, TaggedBEEF};
use std::collections::HashMap;

#[tokio::test]
async fn ordinary_submit_words_are_byte_identical_to_keeper_main() {
    let inputs: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/atomic_legacy_inputs.json")).unwrap();
    let mut words = Vec::new();
    for input in inputs {
        for mode in [
            SubmitMode::CurrentTx,
            SubmitMode::HistoricalTx,
            SubmitMode::HistoricalTxNoSpv,
        ] {
            let managers: HashMap<String, Box<dyn TopicManager>> = HashMap::from([
                (
                    "tm_ship".into(),
                    Box::new(SHIPTopicManager::new()) as Box<dyn TopicManager>,
                ),
                (
                    "tm_slap".into(),
                    Box::new(SLAPTopicManager::new()) as Box<dyn TopicManager>,
                ),
            ]);
            let engine = Engine::new(
                managers,
                HashMap::new(),
                Box::new(MemoryStorage::new()),
                None,
                EngineConfig::default(),
            );
            let beef = TaggedBEEF::new(
                hex::decode(input["beef"].as_str().unwrap()).unwrap(),
                serde_json::from_value(input["topics"].clone()).unwrap(),
            );
            for replay in 0..2 {
                let word = match engine.submit(&beef, mode).await {
                    Ok(steak) => serde_json::json!({"ok": steak}),
                    Err(error) => serde_json::json!({"error": error.to_string()}),
                };
                words.push(serde_json::json!({"name": input["name"], "mode": mode, "replay": replay, "word": word}));
            }
        }
    }
    let bytes = serde_json::to_vec_pretty(&words).unwrap();
    if let Ok(path) = std::env::var("ATOMIC_PARITY_CAPTURE") {
        std::fs::write(path, bytes).unwrap();
        return;
    }
    assert_eq!(
        bytes,
        include_bytes!("fixtures/atomic_legacy_words.json"),
        "ordinary Engine::submit changed a corpus word"
    );
}

#[tokio::test]
async fn log_corpus_completes_in_one_poll_with_the_same_words() {
    use bsv_overlay_engine::admission::NoEvidence;
    use bsv_overlay_engine::engine::WalkBreachPolicy;
    use std::rc::Rc;

    let inputs: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/atomic_legacy_inputs.json")).unwrap();
    let mut words = Vec::new();
    for input in inputs {
        for mode in [
            SubmitMode::CurrentTx,
            SubmitMode::HistoricalTx,
            SubmitMode::HistoricalTxNoSpv,
        ] {
            let managers: HashMap<String, Box<dyn TopicManager>> = HashMap::from([
                (
                    "tm_ship".into(),
                    Box::new(SHIPTopicManager::new()) as Box<dyn TopicManager>,
                ),
                (
                    "tm_slap".into(),
                    Box::new(SLAPTopicManager::new()) as Box<dyn TopicManager>,
                ),
            ]);
            let store = Rc::new(MemoryStorage::new());
            let engine = Engine::new(
                managers,
                HashMap::new(),
                Box::new(store.clone()),
                None,
                EngineConfig::default(),
            );
            let beef = TaggedBEEF::new(
                hex::decode(input["beef"].as_str().unwrap()).unwrap(),
                serde_json::from_value(input["topics"].clone()).unwrap(),
            );
            for replay in 0..2 {
                let word = match engine
                    .walk_with_mode(&beef, mode, WalkBreachPolicy::NotNow, &NoEvidence)
                    .await
                {
                    Ok(walked) => {
                        let start = std::time::Instant::now();
                        let admitted = store.admit(&engine, &walked).unwrap();
                        eprintln!(
                            "A1 corpus {} {mode:?} replay={replay}: {} us",
                            input["name"],
                            start.elapsed().as_micros()
                        );
                        serde_json::json!({"ok": admitted.steak})
                    }
                    Err(error) => serde_json::json!({"error": error.to_string()}),
                };
                assert_eq!(store.awaited_io(), 0);
                words.push(serde_json::json!({"name": input["name"], "mode": mode, "replay": replay, "word": word}));
            }
        }
    }
    assert_eq!(
        serde_json::to_vec_pretty(&words).unwrap(),
        include_bytes!("fixtures/atomic_legacy_words.json")
    );
}
