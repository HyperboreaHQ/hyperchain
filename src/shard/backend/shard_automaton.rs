use std::collections::HashSet;

use crate::prelude::*;

/// Shard backend for automatic data processing.
///
/// This backend will automatically handle incoming
/// blocks and transactions and update your blockchain.
///
/// Temporary blocks and transactions are stored in the RAM.
/// It is recommended to write your own better implementation
/// for high load applications.
pub struct ShardAutomatonBackend<T> {
    blockchain: T,
    blocks_pool: HashSet<Block>,
    transactions_pool: HashSet<Transaction>
}

impl<T: Blockchain> ShardAutomatonBackend<T> {
    #[inline]
    pub fn new(blockchain: T) -> Self {
        Self {
            blockchain,
            blocks_pool: HashSet::new(),
            transactions_pool: HashSet::new()
        }
    }

    /// Try to push stored blocks to the blockchain.
    ///
    /// Returns amount of drained blocks.
    pub async fn drain_blocks_pool(&mut self) -> Result<u64, <T::BlocksIndex as BlocksIndex>::Error> {
        let mut total_drained = 0;

        let blocks_index = self.blockchain.blocks_index();

        let mut tail = blocks_index.get_tail_block().await?
            .as_ref()
            .map(Block::get_hash);

        loop {
            let mut drained = HashSet::with_capacity(self.blocks_pool.len());
            let mut drained_count = 0;

            for block in self.blocks_pool.drain() {
                // If the stored block is not stored in the blockchain.
                if blocks_index.get_block(block.number()).await?.is_none() {
                    // If the tail block is previous to the current one.
                    if block.previous_block() == tail {
                        // Update tail hash.
                        tail = Some(block.get_hash());

                        // Remove staged transactions contained by this block.
                        for transaction in block.transactions() {
                            if self.transactions_pool.contains(transaction) {
                                self.transactions_pool.remove(transaction);
                            }
                        }

                        // Push block to the blockchain.
                        blocks_index.push_block(block).await?;

                        drained_count += 1;
                    }

                    // Otherwise keep it in the pool.
                    else {
                        drained.insert(block);
                    }
                }
            }

            self.blocks_pool = drained;

            if drained_count == 0 {
                break;
            }

            total_drained += drained_count;
        }

        Ok(total_drained)
    }
}

#[async_trait::async_trait]
impl<T: Blockchain + Send + Sync> ShardBackend for ShardAutomatonBackend<T> {
    type Error = <T::BlocksIndex as BlocksIndex>::Error;

    async fn handle_block(&mut self, block: Block) -> Result<(), Self::Error> {
        let authorities = self.blockchain.authorities_index();

        // Validate block's authority before processing it.
        match authorities.is_authority(block.validator()).await {
            Ok(is_authority) if !is_authority => return Ok(()),
            Err(_) => return Ok(()),
            _ => ()
        }

        let blocks = self.blockchain.blocks_index();

        match blocks.get_tail_block().await? {
            Some(tail) if block.previous_block() == Some(tail.get_hash()) => {
                for transaction in block.transactions() {
                    self.transactions_pool.retain(|known| known.get_hash() != transaction.get_hash());
                }

                blocks.push_block(block).await?;

                self.drain_blocks_pool().await?;
            }

            None if block.is_root() => {
                for transaction in block.transactions() {
                    self.transactions_pool.retain(|known| known.get_hash() != transaction.get_hash());
                }

                blocks.push_block(block).await?;

                self.drain_blocks_pool().await?;
            }

            _ => {
                self.blocks_pool.insert(block);
            }
        }

        Ok(())
    }

    async fn handle_transaction(&mut self, transaction: Transaction) -> Result<(), Self::Error> {
        match self.blockchain.blocks_index_ref().get_tail_block().await? {
            Some(tail) if !tail.transactions().contains(&transaction) => {
                self.transactions_pool.insert(transaction);
            }

            None => {
                self.transactions_pool.insert(transaction);
            }

            _ => ()
        }

        Ok(())
    }

    async fn get_blocks(&mut self, mut from_number: u64, max_amount: Option<u64>) -> Result<Vec<Block>, Self::Error> {
        let to_number = match max_amount {
            Some(max_amount) => from_number + max_amount,
            None => {
                let tail = self.blockchain.blocks_index()
                    .get_tail_block().await?;

                match tail {
                    Some(tail) => {
                        if tail.number < from_number {
                            from_number
                        } else {
                            tail.number
                        }
                    }

                    None => from_number
                }
            }
        };

        let blocks_index = self.blockchain.blocks_index();

        let mut blocks = Vec::with_capacity((to_number - from_number + 1) as usize);

        let Some(block) = blocks_index.get_block(from_number).await? else {
            return Ok(blocks);
        };

        while let Some(block) = blocks_index.get_next_block(&block).await? {
            blocks.push(block);

            from_number += 1;

            if from_number > to_number {
                break;
            }
        }

        Ok(blocks)
    }

    async fn get_transactions(&mut self, known: Vec<Hash>) -> Result<Vec<Transaction>, Self::Error> {
        let mut transactions = Vec::new();

        for transaction in &self.transactions_pool {
            if !known.contains(&transaction.get_hash()) {
                transactions.push(transaction.clone());
            }
        }

        Ok(transactions)
    }
}
