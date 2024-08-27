use crate::block::Block;

mod chunked_blocks;

pub use chunked_blocks::*;

#[async_trait::async_trait]
/// This trait implementation should manage information
/// about the blocks.
pub trait BlocksIndex {
    type Error: std::error::Error + Send + Sync;

    /// Try to get a block by its number.
    async fn get_block(&self, number: u64) -> Result<Option<Block>, Self::Error>;

    /// Try to insert a block to the index.
    ///
    /// This method mustn't replace already indexed
    /// blocks and return `false` if it failed to index
    /// given block. Otherwise return `true`.
    async fn insert_block(&self, block: Block) -> Result<bool, Self::Error>;

    /// Try to get a block next to the given one.
    ///
    /// This method should have the fastest next block lookup implementation.
    async fn get_next_block(&self, block: &Block) -> Result<Option<Block>, Self::Error> {
        self.get_block(block.number + 1).await
    }

    /// Try to get the head block.
    ///
    /// Head block is a block that doesn't
    /// reference any other indexed block
    /// and has the minimal number.
    ///
    /// ```text
    /// [0] <- [1] <- [2] <- ??? <- ... <- ??? <- [6] <- [7] <- [8] <- [9]
    /// ^^^ head      ^^^ tail                    ^^^^^^^^^^^^^^^^^^^^^^^^ floating blocks
    /// ```
    async fn get_head_block(&self) -> Result<Option<Block>, Self::Error> {
        self.get_block(0).await
    }

    /// Try to get the tail block.
    ///
    /// Tail block is a block which hash
    /// is not referenced by any other indexed block
    /// and is connected with a head block by other blocks.
    ///
    /// ```text
    /// [0] <- [1] <- [2] <- ??? <- ... <- ??? <- [6] <- [7] <- [8] <- [9]
    /// ^^^ head      ^^^ tail                    ^^^^^^^^^^^^^^^^^^^^^^^^ floating blocks
    /// ```
    async fn get_tail_block(&self) -> Result<Option<Block>, Self::Error> {
        let Some(mut block) = self.get_head_block().await? else {
            return Ok(None);
        };

        loop {
            match self.get_next_block(&block).await? {
                Some(next_block) => block = next_block,

                None => return Ok(Some(block))
            }
        }
    }

    /// Check if the blocks index is empty.
    async fn is_empty(&self) -> Result<bool, Self::Error> {
        Ok(self.get_head_block().await?.is_none())
    }

    /// Check if the blocks index is truncated.
    ///
    /// Truncated blocks index's head block references some
    /// another block which is not stored in this index.
    ///
    /// !!! Truncated indexes cannot be fully validated. !!!
    async fn is_truncated(&self) -> Result<bool, Self::Error> {
        match self.get_head_block().await? {
            Some(head) => Ok(!head.is_root()),
            None => Ok(false)
        }
    }
}
