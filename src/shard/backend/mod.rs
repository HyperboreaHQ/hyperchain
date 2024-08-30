use crate::block::prelude::*;

mod basic_shard;

pub use basic_shard::*;

#[async_trait::async_trait]
pub trait ShardBackend {
    type Error: std::error::Error + Send + Sync;

    /// Get head block of the blockchain.
    async fn get_head_block(&mut self) -> Result<Option<Block>, Self::Error>;

    /// Get tail block of the blockchain.
    async fn get_tail_block(&mut self) -> Result<Option<Block>, Self::Error>;

    /// Get list of staged transactions' hashes.
    async fn get_staged_transactions(&mut self) -> Result<Vec<Hash>, Self::Error>;

    /// Try to get staged transaction with a given hash.
    async fn get_staged_transaction(&mut self, hash: &Hash) -> Result<Option<Transaction>, Self::Error>;

    /// Try to get block with given number.
    async fn get_block(&mut self, number: u64) -> Result<Option<Block>, Self::Error>;

    /// Try to get block next to the given one.
    ///
    /// This method should implement the fastest possible
    /// way of doing this operation.
    async fn get_next_block(&mut self, block: &Block) -> Result<Option<Block>, Self::Error> {
        self.get_block(block.number() + 1).await
    }

    /// Try to get stable transaction with given hash.
    async fn get_transaction(&mut self, hash: &Hash) -> Result<Option<(Transaction, Block)>, Self::Error>;

    /// Handle blockchain block.
    ///
    /// This is not necessary a new block, so you
    /// need to implement a validation method too.
    ///
    /// Return true if the block was accepted.
    async fn handle_block(&mut self, block: Block) -> Result<bool, Self::Error>;

    /// Handle blockchain transaction.
    ///
    /// This is not necessary a new transaction, so you
    /// need to implement a validation method too.
    ///
    /// Return true if the transaction was accepted.
    async fn handle_transaction(&mut self, transaction: Transaction) -> Result<bool, Self::Error>;
}

pub(crate) type Validator<T> = Box<dyn Fn(&T) -> bool + Send + Sync>;

/// Shard backend that implements arbitrary functions
/// to validate blocks and transactions before handling.
pub trait ValidatableShardBackend: Sized {
    /// Change blocks validator.
    fn set_block_validator(&mut self, validator: Validator<Block>);

    /// Change transactions validator.
    fn set_transaction_validator(&mut self, validator: Validator<Transaction>);

    #[inline]
    /// Get backend with changed blocks validator.
    fn with_block_validator(mut self, validator: Validator<Block>) -> Self {
        self.set_block_validator(validator);

        self
    }

    #[inline]
    /// Get backend with changed transactions validator.
    fn with_transaction_validator(mut self, validator: Validator<Transaction>) -> Self {
        self.set_transaction_validator(validator);

        self
    }
}

pub(crate) type Handler<T> = Box<dyn Fn(&T) + Send + Sync>;

/// Shard backend that implements methods
/// to handle new blocks and transactions
/// in the blockchain.
pub trait HandlableShardBackend: Sized {
    /// Change blocks handler.
    fn set_block_handler(&mut self, handler: Handler<Block>);

    /// Change transactions handler.
    fn set_transaction_handler(&mut self, handler: Handler<Transaction>);

    #[inline]
    /// Get backend with changed blocks handler.
    fn with_block_handler(mut self, handler: Handler<Block>) -> Self {
        self.set_block_handler(handler);

        self
    }

    #[inline]
    /// Get backend with changed transactions handler.
    fn with_transaction_handler(mut self, handler: Handler<Transaction>) -> Self {
        self.set_transaction_handler(handler);

        self
    }
}
