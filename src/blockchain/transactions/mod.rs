use std::sync::Arc;

use crate::prelude::*;

mod transactions_file;

pub use transactions_file::*;

#[async_trait::async_trait]
/// This trait implementation should manage information
/// about transactions from the blocks index.
pub trait TransactionsIndex {
    type BlocksIndex: BlocksIndex + Send + Sync;
    type Error: std::error::Error + Send + Sync;

    /// Get atomic reference to the blocks index
    /// that will be used to index the transactions.
    fn blocks_index(&self) -> Arc<Self::BlocksIndex>;

    /// Try searching for transaction in the index.
    async fn get_transaction(&self, transaction: &Hash) -> Result<Option<(Transaction, Block)>, Self::Error>;

    /// Check if transaction with given hash is indexed.
    async fn has_transaction(&self, transaction: &Hash) -> Result<bool, Self::Error> {
        Ok(self.get_transaction(transaction).await?.is_some())
    }
}
