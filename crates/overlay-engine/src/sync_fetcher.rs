//! The definitive-absence memo belongs to one sync invocation, across all topics.
use crate::gasp::{AncestorFetcher, FetchedAncestor, GASPError};
use async_trait::async_trait;
use std::{cell::RefCell, collections::HashSet, rc::Rc};

pub(crate) struct SyncFetcher {
    inner: Rc<dyn AncestorFetcher>,
    absent: RefCell<HashSet<String>>,
}
impl SyncFetcher {
    pub fn new(inner: Rc<dyn AncestorFetcher>) -> Self {
        inner.begin_sync();
        Self {
            inner,
            absent: RefCell::default(),
        }
    }
    fn absent(&self, txid: &str) -> bool {
        let found = self.absent.borrow().contains(&txid.to_ascii_lowercase());
        if found {
            crate::meter::not_found_memo_hit();
        }
        found
    }
}
#[async_trait(?Send)]
impl AncestorFetcher for SyncFetcher {
    async fn fetch_ancestor(&self, txid: &str) -> Result<FetchedAncestor, GASPError> {
        if self.absent(txid) {
            return Err(GASPError::DefinitiveNotFound(txid.into()));
        }
        let result = self.inner.fetch_ancestor(txid).await;
        if matches!(result, Err(GASPError::DefinitiveNotFound(_))) {
            self.absent.borrow_mut().insert(txid.to_ascii_lowercase());
        }
        result
    }
    async fn verified_proof_for(&self, txid: &str) -> Option<String> {
        if self.absent(txid) {
            None
        } else {
            self.inner.verified_proof_for(txid).await
        }
    }
    fn budget_remaining(&self) -> Option<u32> {
        self.inner.budget_remaining()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Fault(std::cell::Cell<u32>);
    #[async_trait(?Send)]
    impl AncestorFetcher for Fault {
        async fn fetch_ancestor(&self, _: &str) -> Result<FetchedAncestor, GASPError> {
            self.0.set(self.0.get() + 1);
            Err(GASPError::RemoteError("503".into()))
        }
    }
    #[tokio::test]
    async fn load_0112_provider_fault_is_not_a_definitive_memo_entry() {
        let inner = Rc::new(Fault(std::cell::Cell::new(0)));
        let fetcher = SyncFetcher::new(inner.clone());
        let meter = crate::meter::LoadMeter::default();
        meter
            .scope(async {
                assert!(fetcher.fetch_ancestor("aa").await.is_err());
                assert!(fetcher.fetch_ancestor("aa").await.is_err());
            })
            .await;
        assert_eq!(inner.0.get(), 2);
        assert_eq!(meter.snapshot().courier_not_found_memo_hits, 0);
    }
}
