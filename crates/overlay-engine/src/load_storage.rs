//! Count each storage operation at the trait boundary and forward its result.
//! While a sync's page is open (engine 0.2.2, `crate::page_batch`) the
//! page's admission writes are queued and written in one batch, its reads
//! answered from the page where it knows them; every other write, and every
//! read of the tables the queue writes, flushes the queue before it.
use crate::{page_batch, storage::*, types::*};
use async_trait::async_trait;
pub(crate) struct MeteredStorage(pub Box<dyn Storage>);

impl MeteredStorage {
    /// Before a call the page does not answer: apply what the page queued,
    /// so the store has every earlier write. `rows` names a write that
    /// changes output or applied rows, after which the page's answers are
    /// forgotten.
    async fn barrier(&self, rows: bool) -> Result<(), StorageError> {
        let Some(page) = page_batch::current() else {
            return Ok(());
        };
        if page.pending() > 0 {
            page.flush(self).await?;
        }
        if rows {
            page.forget_answers();
        }
        Ok(())
    }
}
#[async_trait(?Send)]
impl Storage for MeteredStorage {
    async fn insert_output_with_origin(
        &self,
        output: &Output,
        gasp: bool,
    ) -> Result<(), StorageError> {
        if let Some(page) = page_batch::current() {
            page.queue_output(output, gasp);
            return Ok(());
        }
        crate::meter::storage_write();
        self.0.insert_output_with_origin(output, gasp).await
    }
    async fn read_fault_memos(
        &self,
        host: &str,
        topic: &str,
        outpoints: Option<&[String]>,
    ) -> Result<crate::fault_memo::FaultMemoBatch, StorageError> {
        crate::meter::storage_read();
        self.0.read_fault_memos(host, topic, outpoints).await
    }

    async fn get_peer_cadence(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<crate::sync_load::PeerCadence, StorageError> {
        crate::meter::storage_read();
        self.0.get_peer_cadence(host, topic).await
    }
    async fn set_peer_cadence(
        &self,
        host: &str,
        topic: &str,
        state: &crate::sync_load::PeerCadence,
    ) -> Result<(), StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_write();
        self.0.set_peer_cadence(host, topic, state).await
    }
    async fn insert_output(&self, output: &Output) -> Result<(), StorageError> {
        if let Some(page) = page_batch::current() {
            page.queue_output(output, false);
            return Ok(());
        }
        crate::meter::storage_write();
        self.0.insert_output(output).await
    }
    async fn delete_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
        self.barrier(true).await?;
        crate::meter::storage_write();
        self.0.delete_output(txid, output_index, topic).await
    }
    async fn mark_utxo_as_spent(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
        self.barrier(true).await?;
        crate::meter::storage_write();
        self.0.mark_utxo_as_spent(txid, output_index, topic).await
    }
    async fn update_consumed_by(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        consumed_by: &[Outpoint],
    ) -> Result<(), StorageError> {
        self.barrier(true).await?;
        crate::meter::storage_write();
        self.0
            .update_consumed_by(txid, output_index, topic, consumed_by)
            .await
    }
    async fn update_transaction_beef(&self, txid: &str, beef: &[u8]) -> Result<(), StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_write();
        self.0.update_transaction_beef(txid, beef).await
    }
    async fn mark_transaction_proven(&self, txid: &str) -> Result<(), StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_write();
        self.0.mark_transaction_proven(txid).await
    }
    async fn mark_transaction_proven_at(
        &self,
        txid: &str,
        height: Option<u64>,
    ) -> Result<(), StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_write();

        self.0.mark_transaction_proven_at(txid, height).await
    }
    async fn update_output_block_height(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
        block_height: u32,
    ) -> Result<(), StorageError> {
        self.barrier(true).await?;
        crate::meter::storage_write();
        self.0
            .update_output_block_height(txid, output_index, topic, block_height)
            .await
    }
    async fn insert_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
        if let Some(page) = page_batch::current() {
            page.queue_applied(tx);
            return Ok(());
        }
        crate::meter::storage_write();
        self.0.insert_applied_transaction(tx).await
    }
    async fn does_applied_transaction_exist(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<bool, StorageError> {
        let page = page_batch::current();
        if let Some(held) = page.as_ref().and_then(|p| p.applied(tx)) {
            return Ok(held);
        }
        crate::meter::storage_read();
        let held = self.0.does_applied_transaction_exist(tx).await?;
        if let Some(page) = page {
            page.remember_applied(tx, held);
        }
        Ok(held)
    }
    async fn delete_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
        self.barrier(true).await?;
        crate::meter::storage_write();
        self.0.delete_applied_transaction(tx).await
    }
    async fn find_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: Option<&str>,
        spent: Option<bool>,
        include_beef: bool,
    ) -> Result<Option<Output>, StorageError> {
        // The page answers the admissions' question (a topic's output, no
        // spent filter, no BEEF); any other shape reads the store whole.
        let page = if let (Some(topic), None, false) = (topic, spent, include_beef) {
            page_batch::current().map(|p| (p, topic))
        } else {
            self.barrier(false).await?;
            None
        };
        if let Some(known) = page
            .as_ref()
            .and_then(|(p, topic)| p.output(txid, output_index, topic))
        {
            return Ok(known.0);
        }
        crate::meter::storage_read();
        let answer = self
            .0
            .find_output(txid, output_index, topic, spent, include_beef)
            .await?;
        if let Some((page, topic)) = page {
            page.remember_output(txid, output_index, topic, answer.clone());
        }
        Ok(answer)
    }
    async fn find_served_outputs(
        &self,
        outpoints: &[Outpoint],
    ) -> Result<Vec<Output>, StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_read();
        self.0.find_served_outputs(outpoints).await
    }

    async fn find_outputs_for_transaction(
        &self,
        txid: &str,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_read();
        self.0
            .find_outputs_for_transaction(txid, include_beef)
            .await
    }
    async fn find_utxos_for_topic(
        &self,
        topic: &str,
        since: Option<f64>,
        limit: Option<u64>,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_read();
        self.0
            .find_utxos_for_topic(topic, since, limit, include_beef)
            .await
    }
    async fn find_transactions_for_proof_check(
        &self,
        limit: u64,
        min_age_secs: u64,
    ) -> Result<Vec<TransactionBeef>, StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_read();
        self.0
            .find_transactions_for_proof_check(limit, min_age_secs)
            .await
    }
    async fn update_last_interaction(
        &self,
        host: &str,
        topic: &str,
        since: u64,
    ) -> Result<(), StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_write();
        self.0.update_last_interaction(host, topic, since).await
    }
    async fn get_last_interaction(&self, host: &str, topic: &str) -> Result<u64, StorageError> {
        crate::meter::storage_read();
        self.0.get_last_interaction(host, topic).await
    }
    async fn record_peer_sync_outcome(
        &self,
        host: &str,
        topic: &str,
        success: bool,
    ) -> Result<(), StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_write();
        self.0.record_peer_sync_outcome(host, topic, success).await
    }
    async fn get_peer_sync_health(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<PeerSyncHealth, StorageError> {
        crate::meter::storage_read();
        self.0.get_peer_sync_health(host, topic).await
    }
    async fn record_peer_sync_yield(
        &self,
        host: &str,
        topic: &str,
        yielded: bool,
    ) -> Result<PeerYieldStreak, StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_write();
        self.0.record_peer_sync_yield(host, topic, yielded).await
    }
    async fn put_deferred_graph(
        &self,
        record: &crate::gasp::DeferredGraph,
    ) -> Result<crate::gasp::DeferredGraphSave, StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_write();
        self.0.put_deferred_graph(record).await
    }
    async fn reserve_unanchorable_retry(
        &self,
        record: &crate::gasp::DeferredGraph,
    ) -> Result<bool, StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_write();
        self.0.reserve_unanchorable_retry(record).await
    }
    async fn find_deferred_graphs(
        &self,
        host: &str,
        topic: &str,
    ) -> Result<Vec<crate::gasp::DeferredGraphKey>, StorageError> {
        crate::meter::storage_read();
        self.0.find_deferred_graphs(host, topic).await
    }
    async fn get_deferred_graph(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<Option<crate::gasp::DeferredGraph>, StorageError> {
        crate::meter::storage_read();
        self.0.get_deferred_graph(host, topic, outpoint).await
    }
    async fn delete_deferred_graph(
        &self,
        host: &str,
        topic: &str,
        outpoint: &str,
    ) -> Result<(), StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_write();
        self.0.delete_deferred_graph(host, topic, outpoint).await
    }
    async fn remember_expired_outpoint(
        &self,
        topic: &str,
        outpoint: &str,
        expiry: u64,
    ) -> Result<(), StorageError> {
        self.barrier(false).await?;
        crate::meter::storage_write();
        self.0
            .remember_expired_outpoint(topic, outpoint, expiry)
            .await
    }
    async fn find_expired_outpoints(
        &self,
        topic: &str,
        outpoints: &[String],
    ) -> Result<Vec<String>, StorageError> {
        crate::meter::storage_read();
        self.0.find_expired_outpoints(topic, outpoints).await
    }
    async fn write_batch(&self, writes: &[page_batch::PageWrite]) -> Result<u64, StorageError> {
        crate::meter::storage_write();
        let statements = self.0.write_batch(writes).await?;
        crate::meter::storage_batch(statements);
        Ok(statements)
    }
    async fn find_topic_outputs(
        &self,
        topic: &str,
        outpoints: &[(String, u32)],
    ) -> Result<Vec<Output>, StorageError> {
        crate::meter::storage_read();
        self.0.find_topic_outputs(topic, outpoints).await
    }
    async fn find_applied_transactions(
        &self,
        topic: &str,
        txids: &[String],
    ) -> Result<Vec<String>, StorageError> {
        crate::meter::storage_read();
        self.0.find_applied_transactions(topic, txids).await
    }
}
