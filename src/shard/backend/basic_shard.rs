use std::collections::HashMap;

use crate::prelude::*;

#[derive(Debug, thiserror::Error)]
pub enum BasicShardBackendError<A, B, C> {
    #[error("Authorities index failure: {0}")]
    AuthoritiesIndex(A),

    #[error("Blocks index failure: {0}")]
    BlocksIndex(B),

    #[error("Transactions index failure: {0}")]
    TransactionsIndex(C)
}

/// Shard backend for automatic data processing.
///
/// This backend will automatically handle incoming
/// blocks and transactions and update your blockchain.
///
/// Staged transactions are stored in the RAM.
/// It is recommended to write your own better implementation
/// for high load applications.
pub struct BasicShardBackend<T> {
    /// Blockchain instance controlled by the shard's backend.
    blockchain: T,

    /// Set of transactions that are not yet stabilized
    /// in the blockchain.
    staged_transactions: HashMap<Hash, Transaction>,

    /// This function is used to validate blocks before handling them.
    block_validator: Option<Validator<Block>>,

    /// This function is used to validate transactions before handling them.
    transaction_validator: Option<Validator<Transaction>>,

    /// This function is called after the block is handled by the backend.
    block_handler: Option<Handler<Block>>,

    /// This function is called after the transaction is handled by the backend.
    transaction_handler: Option<Handler<Transaction>>
}

impl<T: Blockchain> BasicShardBackend<T> {
    #[inline]
    pub fn new(blockchain: T) -> Self {
        Self {
            blockchain,
            staged_transactions: HashMap::new(),
            block_validator: None,
            transaction_validator: None,
            block_handler: None,
            transaction_handler: None
        }
    }
}

#[async_trait::async_trait]
impl<T: Blockchain + Send + Sync> ShardBackend for BasicShardBackend<T> {
    type Error = BasicShardBackendError<
        <T::AuthoritiesIndex as AuthoritiesIndex>::Error,
        <T::BlocksIndex as BlocksIndex>::Error,
        <T::TransactionsIndex as TransactionsIndex>::Error
    >;

    async fn get_head_block(&mut self) -> Result<Option<Block>, Self::Error> {
        self.blockchain.blocks_index_ref()
            .get_head_block().await
            .map_err(BasicShardBackendError::BlocksIndex)
    }

    async fn get_tail_block(&mut self) -> Result<Option<Block>, Self::Error> {
        self.blockchain.blocks_index_ref()
            .get_tail_block().await
            .map_err(BasicShardBackendError::BlocksIndex)
    }

    async fn get_staged_transactions(&mut self) -> Result<Vec<Hash>, Self::Error> {
        Ok(self.staged_transactions.keys().copied().collect())
    }

    async fn get_staged_transaction(&mut self, hash: &Hash) -> Result<Option<Transaction>, Self::Error> {
        Ok(self.staged_transactions.get(hash).cloned())
    }

    async fn get_block(&mut self, number: u64) -> Result<Option<Block>, Self::Error> {
        self.blockchain.blocks_index_ref()
            .get_block(number).await
            .map_err(BasicShardBackendError::BlocksIndex)
    }

    async fn get_next_block(&mut self, block: &Block) -> Result<Option<Block>, Self::Error> {
        self.blockchain.blocks_index_ref()
            .get_next_block(block).await
            .map_err(BasicShardBackendError::BlocksIndex)
    }

    async fn get_transaction(&mut self, hash: &Hash) -> Result<Option<(Transaction, Block)>, Self::Error> {
        self.blockchain.transactions_index_ref()
            .get_transaction(hash).await
            .map_err(BasicShardBackendError::TransactionsIndex)
    }

    async fn handle_block(&mut self, block: Block) -> Result<bool, Self::Error> {
        // Validate block's authority before processing it.
        let is_authority = self.blockchain.authorities_index_ref()
            .is_authority(block.validator()).await
            .map_err(BasicShardBackendError::AuthoritiesIndex)?;

        if !is_authority {
            return Ok(false);
        }

        // Validate it if callback is specified.
        if let Some(validator) = &self.block_validator {
            if !validator(&block).await {
                return Ok(false);
            }
        }

        // Try inserting the block to the index.
        let result = self.blockchain.blocks_index_ref()
            .insert_block(block.clone()).await
            .map_err(BasicShardBackendError::BlocksIndex)?;

        // Handle block if the callback is specified.
        if result {
            if let Some(handler) = &self.block_handler {
                handler(&block).await;
            }
        }

        // If block has been indexed - remove transactions
        // which were stabilized by it.
        if result {
            let mut filtered_transactions = HashMap::with_capacity(self.staged_transactions.len());

            for (hash, transaction) in self.staged_transactions.drain() {
                let is_stabilized = self.blockchain.transactions_index_ref()
                    .has_transaction(&hash).await
                    .map_err(BasicShardBackendError::TransactionsIndex)?;

                if !is_stabilized {
                    filtered_transactions.insert(hash, transaction);
                }
            }

            self.staged_transactions = filtered_transactions;
        }

        Ok(result)
    }

    async fn handle_transaction(&mut self, transaction: Transaction) -> Result<bool, Self::Error> {
        // Check if transaction is already stabilized.
        let is_stabilized = self.blockchain.transactions_index_ref()
            .has_transaction(&transaction.get_hash()).await
            .map_err(BasicShardBackendError::TransactionsIndex)?;

        if is_stabilized {
            return Ok(false);
        }

        // Validate transaction if callback is specified.
        if let Some(validator) = &self.transaction_validator {
            if !validator(&transaction).await {
                return Ok(false);
            }
        }

        // Stage the transaction.
        let result = self.staged_transactions.insert(
            transaction.get_hash(),
            transaction.clone()
        );

        // Handle transaction if the callback is specified.
        if result.is_some() {
            if let Some(handler) = &self.transaction_handler {
                handler(&transaction).await;
            }

            return Ok(true);
        }

        Ok(false)
    }
}

impl<T> ValidatableShardBackend for BasicShardBackend<T> {
    #[inline]
    fn set_block_validator(&mut self, validator: Validator<Block>) {
        self.block_validator = Some(validator);
    }

    #[inline]
    fn set_transaction_validator(&mut self, validator: Validator<Transaction>) {
        self.transaction_validator = Some(validator);
    }
}

impl<T> HandlableShardBackend for BasicShardBackend<T> {
    #[inline]
    fn set_block_handler(&mut self, handler: Handler<Block>) {
        self.block_handler = Some(handler);
    }

    #[inline]
    fn set_transaction_handler(&mut self, handler: Handler<Transaction>) {
        self.transaction_handler = Some(handler);
    }
}
