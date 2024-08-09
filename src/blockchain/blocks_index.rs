use crate::block::{
    Block,
    Hash
};

#[async_trait::async_trait]
pub trait BlocksIndex {
    type Error: std::error::Error + Send + Sync;

    /// Try to get a block by its number.
    async fn get_block_by_number(&self, number: u64) -> Result<Option<Block>, Self::Error>;

    /// Try to get a block by its hash.
    async fn get_block_by_hash(&self, hash: Hash) -> Result<Option<Block>, Self::Error>;

    /// Try to push a new block to the index.
    /// 
    /// This method should verify the block before adding it
    /// and return `false` if it wasn't added. Otherwise `true`.
    async fn push_block(&self, block: Block) -> Result<bool, Self::Error>;

    /// Try to get a block next to the given one.
    /// 
    /// This method should have the fastest next block lookup implementation.
    async fn get_next_block(&self, block: &Block) -> Result<Option<Block>, Self::Error> {
        self.get_block_by_number(block.number + 1).await
    }

    /// Try to get the root block.
    /// 
    /// This method must return the same value
    /// as `get_block_by_number(0)`.
    async fn get_root_block(&self) -> Result<Option<Block>, Self::Error> {
        self.get_block_by_number(0).await
    }

    /// Try to get the tail (latest) block.
    async fn get_tail_block(&self) -> Result<Option<Block>, Self::Error> {
        let Some(mut block) = self.get_root_block().await? else {
            return Ok(None);
        };

        loop {
            match self.get_block_by_number(block.number + 1).await? {
                Some(next_block) => block = next_block,

                None => return Ok(Some(block))
            }
        }
    }

    /// Check if the blocks index is empty.
    async fn is_empty(&self) -> Result<bool, Self::Error> {
        Ok(self.get_root_block().await?.is_none())
    }

    /// Check if the blocks index is truncated.
    /// 
    /// Truncated blocks index's root block references some
    /// another block which is not stored in this index.
    /// 
    /// Truncated indexes can't be fully validated.
    async fn is_truncated(&self) -> Result<bool, Self::Error> {
        match self.get_root_block().await? {
            Some(root) => Ok(root.previous_block.is_some()),
            None => Ok(false)
        }
    }
}
