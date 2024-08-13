use crate::block::prelude::*;

mod shard_subscriber;
mod shard_automaton;

pub use shard_subscriber::ShardSubscriberBackend;
pub use shard_automaton::ShardAutomatonBackend;

#[async_trait::async_trait]
pub trait ShardBackend {
    type Error: std::error::Error + Send + Sync;

    /// Handle blockchain's block.
    ///
    /// This is not necessary a new block, so you
    /// need to implement a validation method too.
    async fn handle_block(&mut self, block: Block) -> Result<(), Self::Error>;

    /// Handle blockchain's transaction.
    ///
    /// This is not necessary a new transaction, so you
    /// need to implement a validation method too.
    async fn handle_transaction(&mut self, transaction: Transaction) -> Result<(), Self::Error>;

    /// Get list of known blocks in given period.
    async fn get_blocks(&mut self, from_number: u64, max_amount: Option<u64>) -> Result<Vec<Block>, Self::Error>;

    /// Get list of known staged transactions excluding
    /// ones with known hashes.
    async fn get_transactions(&mut self, known: Vec<Hash>) -> Result<Vec<Transaction>, Self::Error>;
}
