//! The heap of the GASP finalize under `on` (bsv-low #657, the host seam
//! lens's M1). Under `off` the engine's finalize holds the finalized graph's
//! BEEFs (the sink) and one submit's parse; under `on` it must hold no more
//! than that and one body's copy (the body handed to the host). The lens
//! measured the seam as built holding every body a second time (the walked
//! list) for the whole gate: +3,183,983 B at the first finalize judgement of
//! a 3.15 MB graph. This binary owns a counting global allocator, so the
//! live heap is sampled at each finalize judgement (the manager's non-dry
//! call, the same manager in both modes).

#[path = "support/gasp_shadow_world.rs"]
mod world;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bsv_overlay_engine::admission::{gasp_unit, AdmitRefusal, Committed, UnitKind, UnitRunner};
use bsv_overlay_engine::engine::{Engine, EngineConfig};
use bsv_overlay_engine::gasp_shadow::{AdmissionMode, FinalizeBody, GaspFinalizeHost};
use bsv_overlay_engine::storage::memory::MemoryStorage;
use bsv_overlay_engine::topic_manager::{TopicManager, TopicManagerError};
use bsv_overlay_engine::types::*;
use bsv_rs::transaction::{ChainTracker, Transaction};
use world::*;

static LIVE: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = System.alloc(layout);
        if !p.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        p
    }

    unsafe fn dealloc(&self, p: *mut u8, layout: Layout) {
        System.dealloc(p, layout);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The world's manager, sampling the live heap at every call that is not a
/// dry run: a finalize judgement (the engine's own submit under `off`, the
/// host's unit under `on`).
struct Sampling {
    inner: AdmitAll,
    samples: Rc<RefCell<Vec<usize>>>,
}

#[async_trait(?Send)]
impl TopicManager for Sampling {
    fn reads_off_chain_values(&self) -> bool {
        false
    }
    async fn identify_admissible_outputs(
        &self,
        tx: &Transaction,
        previous_coins: &[u8],
        off_chain_values: Option<&[u8]>,
        mode: SubmitMode,
        context: &TopicAdmittanceContext,
    ) -> Result<AdmittanceInstructions, TopicManagerError> {
        if !context.dry_run {
            let live = LIVE.load(Ordering::Relaxed);
            self.samples.borrow_mut().push(live);
        }
        self.inner
            .identify_admissible_outputs(tx, previous_coins, off_chain_values, mode, context)
            .await
    }
    async fn identify_needed_inputs(
        &self,
        beef: &[u8],
        off_chain_values: Option<&[u8]>,
    ) -> Result<Vec<Outpoint>, TopicManagerError> {
        self.inner
            .identify_needed_inputs(beef, off_chain_values)
            .await
    }
    async fn get_documentation(&self) -> String {
        String::new()
    }
    async fn get_metadata(&self) -> ServiceMetadata {
        ServiceMetadata::default()
    }
}

fn sampling(samples: &Rc<RefCell<Vec<usize>>>) -> HashMap<String, Box<dyn TopicManager>> {
    HashMap::from([(
        TOPIC.to_string(),
        Box::new(Sampling {
            inner: AdmitAll {
                fails_at_finalize: None,
            },
            samples: samples.clone(),
        }) as Box<dyn TopicManager>,
    )])
}

/// The host: each body one unit of the memory log over the served store,
/// judged by an engine whose manager samples like the sync engine's.
struct LogHost {
    store: Rc<MemoryStorage>,
    engine: Engine,
}

#[async_trait(?Send)]
impl GaspFinalizeHost for LogHost {
    async fn admit(
        &self,
        body: &FinalizeBody<'_>,
        resolved: Option<Vec<u8>>,
    ) -> Result<Committed, AdmitRefusal> {
        if let Some(bytes) = resolved {
            self.store.cache_admission_beef(&body.walked.beef, bytes)?;
        }
        self.store.run_unit(
            UnitKind::Admit,
            &body.walked.evidence,
            Box::pin(gasp_unit(&self.engine, body.walked)),
        )
    }
}

/// `tx` with a second output carrying `bytes` of data.
fn heavy(mut tx: Transaction, bytes: usize) -> Transaction {
    let mut data = vec![0x00, 0x6a, 0x4e];
    data.extend(u32::try_from(bytes).unwrap().to_le_bytes());
    data.resize(data.len() + bytes, 0xcd);
    tx.outputs.push(bsv_rs::transaction::TransactionOutput::new(
        1,
        bsv_rs::script::LockingScript::from_binary(&data).unwrap(),
    ));
    tx
}

/// The lens's graph: a proven genesis and 12 proven 256 KiB links, the peer
/// listing the tip. Its finalize's BEEFs are about 3.15 MB (13 bodies).
fn chain() -> (Vec<Transaction>, Vec<GASPNode>) {
    let mut txs = vec![genesis(90, 1)];
    for _ in 0..12 {
        let link = heavy(child(txs.last().unwrap(), 0, 1), 256 << 10);
        txs.push(link);
    }
    let nodes = txs
        .iter()
        .enumerate()
        .map(|(i, tx)| node(tx, honest_proof(&tx.id(), 100 + i as u32)))
        .collect();
    (txs, nodes)
}

/// The live heap at each finalize judgement of one sync of the chain.
async fn judgements(mode: AdmissionMode) -> Vec<usize> {
    let (txs, nodes) = chain();
    let events = Events::default();
    let remote = ChaosRemote::new(&nodes, &[(txs.last().unwrap(), 0, 1.0)], &events);
    let store = Rc::new(MemoryStorage::new());
    let samples = Rc::default();
    let mut engine = Engine::with_chain_tracker(
        sampling(&samples),
        HashMap::new(),
        Box::new(store.clone()),
        None,
        None,
        None::<Box<dyn ChainTracker>>,
        EngineConfig {
            sync_configuration: HashMap::from([(
                TOPIC.to_string(),
                SyncTarget::Peers(vec![PEER.to_string()]),
            )]),
            ..Default::default()
        },
    );
    engine.set_gasp_remote_factory(Box::new(remote.clone()));
    if mode == AdmissionMode::On {
        engine.set_gasp_finalize_host(Rc::new(LogHost {
            store: store.clone(),
            engine: Engine::new(
                sampling(&samples),
                HashMap::new(),
                Box::new(store.clone()),
                None,
                EngineConfig::default(),
            ),
        }));
        engine.set_admission_mode(AdmissionMode::On).unwrap();
    }
    engine.start_gasp_sync().await.unwrap();
    drop(engine);
    let samples = samples.borrow().clone();
    assert_eq!(samples.len(), txs.len(), "one judgement per body");
    samples
}

/// M1. At the first finalize judgement `on` holds at most one body's bytes
/// more than `off` (the body handed to the host): the graph's bytes live in
/// the finalize's sink once, never a second time in the walked list.
#[tokio::test]
async fn m1_on_holds_the_finalized_graph_once_at_every_judgement() {
    let (txs, nodes) = chain();
    let largest = nodes
        .iter()
        .map(|n| n.raw_tx.len() / 2)
        .max()
        .unwrap_or_default();
    drop((txs, nodes));
    let off = judgements(AdmissionMode::Off).await;
    let on = judgements(AdmissionMode::On).await;
    println!("M1 off: {off:?}");
    println!("M1 on:  {on:?}");
    let over = on[0].saturating_sub(off[0]);
    println!(
        "M1 at the first judgement: on {} B, off {} B, on over off {over} B (one body {largest} B)",
        on[0], off[0]
    );
    assert!(
        over <= largest,
        "on holds at most one body over off at the first judgement: on {} B, off {} B, over {over} B, one body {largest} B",
        on[0],
        off[0]
    );
}
