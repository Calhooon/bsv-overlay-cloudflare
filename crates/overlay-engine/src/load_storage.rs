//! Count each storage operation at the trait boundary and forward its result.
use crate::{storage::*, types::*};
use async_trait::async_trait;
pub(crate) struct MeteredStorage(pub Box<dyn Storage>);
#[async_trait(?Send)]
impl Storage for MeteredStorage {
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
        crate::meter::storage_write();
        self.0.set_peer_cadence(host, topic, state).await
    }
    async fn insert_output(&self, output: &Output) -> Result<(), StorageError> {
        crate::meter::storage_write();
        self.0.insert_output(output).await
    }
    async fn delete_output(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
        crate::meter::storage_write();
        self.0.delete_output(txid, output_index, topic).await
    }
    async fn mark_utxo_as_spent(
        &self,
        txid: &str,
        output_index: u32,
        topic: &str,
    ) -> Result<(), StorageError> {
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
        crate::meter::storage_write();
        self.0
            .update_consumed_by(txid, output_index, topic, consumed_by)
            .await
    }
    async fn update_transaction_beef(&self, txid: &str, beef: &[u8]) -> Result<(), StorageError> {
        crate::meter::storage_write();
        self.0.update_transaction_beef(txid, beef).await
    }
    async fn mark_transaction_proven(&self, txid: &str) -> Result<(), StorageError> {
        crate::meter::storage_write();
        self.0.mark_transaction_proven(txid).await
    }
    async fn mark_transaction_proven_at(
        &self,
        txid: &str,
        height: Option<u64>,
    ) -> Result<(), StorageError> {
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
        crate::meter::storage_write();
        self.0
            .update_output_block_height(txid, output_index, topic, block_height)
            .await
    }
    async fn insert_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
        crate::meter::storage_write();
        self.0.insert_applied_transaction(tx).await
    }
    async fn does_applied_transaction_exist(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<bool, StorageError> {
        crate::meter::storage_read();
        self.0.does_applied_transaction_exist(tx).await
    }
    async fn delete_applied_transaction(
        &self,
        tx: &AppliedTransaction,
    ) -> Result<(), StorageError> {
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
        crate::meter::storage_read();
        self.0
            .find_output(txid, output_index, topic, spent, include_beef)
            .await
    }
    async fn find_served_outputs(
        &self,
        outpoints: &[Outpoint],
    ) -> Result<Vec<Output>, StorageError> {
        crate::meter::storage_read();
        self.0.find_served_outputs(outpoints).await
    }

    async fn find_outputs_for_transaction(
        &self,
        txid: &str,
        include_beef: bool,
    ) -> Result<Vec<Output>, StorageError> {
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
        crate::meter::storage_write();
        self.0.record_peer_sync_yield(host, topic, yielded).await
    }
    async fn put_deferred_graph(
        &self,
        record: &crate::gasp::DeferredGraph,
    ) -> Result<crate::gasp::DeferredGraphSave, StorageError> {
        crate::meter::storage_write();
        self.0.put_deferred_graph(record).await
    }
    async fn reserve_unanchorable_retry(
        &self,
        record: &crate::gasp::DeferredGraph,
    ) -> Result<bool, StorageError> {
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
        crate::meter::storage_write();
        self.0.delete_deferred_graph(host, topic, outpoint).await
    }
    async fn remember_expired_outpoint(
        &self,
        topic: &str,
        outpoint: &str,
        expiry: u64,
    ) -> Result<(), StorageError> {
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
}
