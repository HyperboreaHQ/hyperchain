use std::future::Future;
use std::pin::Pin;

use super::*;

type Handler<T, F, E> = Box<
    dyn FnMut(T) -> Pin<Box<
        dyn Future<
            Output = Result<F, E>
        > + Send + Sync + 'static
    >> + Send + Sync + 'static
>;

type BlockHandler<E> = Handler<Block, (), E>;
type TransactionHandler<E> = Handler<Transaction, (), E>;
type GetBlocksHandler<E> = Handler<(u64, Option<u64>), Vec<Block>, E>;
type GetTransactionsHandler<E> = Handler<Vec<Hash>, Vec<Transaction>, E>;

#[derive(Default)]
/// Shard backend for manual data processing.
///
/// In this backend you have to specify handlers
/// for incoming blocks and transactions and write
/// their processing logic.
pub struct ShardSubscriberBackend<E> {
    pub block_handler: Option<BlockHandler<E>>,
    pub transaction_handler: Option<TransactionHandler<E>>,
    pub get_blocks_handler: Option<GetBlocksHandler<E>>,
    pub get_transactions_handler: Option<GetTransactionsHandler<E>>
}

impl<E> ShardSubscriberBackend<E> {
    #[inline]
    /// Change block updates handler.
    pub fn with_block_handler(mut self, handler: BlockHandler<E>) -> Self {
        self.block_handler = Some(handler);

        self
    }

    #[inline]
    /// Change transactions updates handler.
    pub fn with_transaction_handler(mut self, handler: TransactionHandler<E>) -> Self {
        self.transaction_handler = Some(handler);

        self
    }

    #[inline]
    /// Change get blocks handler.
    pub fn with_get_blocks_handler(mut self, handler: GetBlocksHandler<E>) -> Self {
        self.get_blocks_handler = Some(handler);

        self
    }

    #[inline]
    /// Change get transactions handler.
    pub fn with_get_transactions_handler(mut self, handler: GetTransactionsHandler<E>) -> Self {
        self.get_transactions_handler = Some(handler);

        self
    }
}

#[async_trait::async_trait]
impl<E> ShardBackend for ShardSubscriberBackend<E>
where E: std::error::Error + Send + Sync
{
    type Error = E;

    async fn handle_block(&mut self, block: Block) -> Result<(), Self::Error> {
        if let Some(handler) = &mut self.block_handler {
            handler(block).await?;
        }

        Ok(())
    }

    async fn handle_transaction(&mut self, transaction: Transaction) -> Result<(), Self::Error> {
        if let Some(handler) = &mut self.transaction_handler {
            handler(transaction).await?;
        }

        Ok(())
    }

    async fn get_blocks(&mut self, from_number: u64, max_amount: Option<u64>) -> Result<Vec<Block>, Self::Error> {
        match self.get_blocks_handler.as_mut() {
            Some(handler) => handler((from_number, max_amount)).await,
            None => Ok(vec![])
        }
    }

    async fn get_transactions(&mut self, known: Vec<Hash>) -> Result<Vec<Transaction>, Self::Error> {
        match self.get_transactions_handler.as_mut() {
            Some(handler) => handler(known).await,
            None => Ok(vec![])
        }
    }
}
