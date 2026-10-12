//! One rotating courier ladder, with circuits shared by the bounded owed tick.
use std::{cell::RefCell, collections::BTreeMap};
#[derive(Default)]
pub struct Pacing {
    pub tick: u64,
    pub start: usize,
    pub faults: [u8; 3],
}
impl Pacing {
    pub fn begin_tick(&mut self, tick: u64) {
        if self.tick != tick {
            self.tick = tick;
            self.faults = [0; 3];
        }
    }
    pub fn order(&mut self) -> [usize; 3] {
        let n = self.start % 3;
        self.start += 1;
        [n, (n + 1) % 3, (n + 2) % 3]
    }
    pub fn allowed(&self, rung: usize) -> bool {
        self.faults[rung] < 3
    }
    pub fn fault(&mut self, rung: usize) {
        self.faults[rung] = self.faults[rung].saturating_add(1);
    }
}
thread_local! {
    static PACING:RefCell<Pacing>=RefCell::new(Pacing::default());
    static COUNTS:RefCell<BTreeMap<String,u64>>=const {RefCell::new(BTreeMap::new())};
}
pub fn begin_tick(tick: u64) {
    PACING.with(|p| p.borrow_mut().begin_tick(tick));
}
pub fn order() -> [usize; 3] {
    PACING.with(|p| p.borrow_mut().order())
}
pub fn note(source: &str, outcome: &str) {
    let name = format!("tracker_reask_{source}_{outcome}_total");
    COUNTS.with(|c| *c.borrow_mut().entry(name.clone()).or_default() += 1);
    crate::courier::note_presence(&name);
}
pub fn allowed(rung: usize, source: &str) -> bool {
    let yes = PACING.with(|p| p.borrow().allowed(rung));
    if !yes {
        note(source, "skipped");
    }
    yes
}
pub fn fault(rung: usize, source: &str) {
    PACING.with(|p| p.borrow_mut().fault(rung));
    note(source, "faulted");
}
pub fn health_json() -> serde_json::Value {
    COUNTS.with(|c| serde_json::json!(*c.borrow()))
}
