use std::path::PathBuf;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value as Json;

use hyperborealib::exports::tokio;

use hyperborealib::rest_api::{
    AsJson,
    AsJsonError
};

use super::*;

#[derive(Debug, thiserror::Error)]
pub enum ChunkedBlocksIndexError {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Json(#[from] AsJsonError),

    #[error(transparent)]
    Serialize(#[from] serde_json::Error)
}

/// Basic blocks index implementation.
///
/// This struct will squash several blocks
/// in a single chunk file and store in
/// in the given folder.
///
/// This should be enough for small scale applications.
pub struct ChunkedBlocksIndex {
    folder: PathBuf,
    chunk_size: u64,
    tail_block: AtomicU64
}

impl ChunkedBlocksIndex {
    /// Create or open blocks index
    /// stored in a given folder.
    ///
    /// Chunk size specifies amount of blocks
    /// that needs to be squashed into a single file.
    pub async fn open(folder: impl Into<PathBuf>, chunk_size: u64) -> Result<Self, ChunkedBlocksIndexError> {
        let folder: PathBuf = folder.into();

        if !folder.exists() {
            tokio::fs::create_dir_all(&folder).await?;
        }

        Ok(Self {
            folder,
            chunk_size,
            tail_block: AtomicU64::new(0)
        })
    }
}

#[async_trait::async_trait]
impl BlocksIndex for ChunkedBlocksIndex {
    type Error = ChunkedBlocksIndexError;

    async fn get_block(&self, number: u64) -> Result<Option<Block>, Self::Error> {
        let chunk_number = number / self.chunk_size;

        let chunk_path = self.folder.join(format!("chunk-{chunk_number}.json"));

        // Block doesn't exist if the chunk doesn't exist
        if !chunk_path.exists() {
            return Ok(None);
        }

        // Read chunk where the block should be stored
        let chunk = tokio::fs::read(&chunk_path).await?;
        let chunk = serde_json::from_slice::<HashSet<Json>>(&chunk)?;

        // Search for the block
        let block = chunk.iter()
            .flat_map(Block::from_json)
            .find(|block| block.number() == number);

        Ok(block)
    }

    async fn insert_block(&self, block: Block) -> Result<bool, Self::Error> {
        let chunk_number = block.number() / self.chunk_size;

        let chunk_path = self.folder.join(format!("chunk-{chunk_number}.json"));

        // Create new chunk file if one doesn't exist already
        if !chunk_path.exists() {
            tokio::fs::write(&chunk_path, serde_json::to_string_pretty(&[
                block.to_json()?
            ])?).await?;

            return Ok(true);
        }

        // Otherwise update existing chunk file
        let chunk = tokio::fs::read(&chunk_path).await?;
        let mut chunk = serde_json::from_slice::<HashSet<Json>>(&chunk)?;

        // Do not update the file if block wasn't added
        if !chunk.insert(block.to_json()?) {
            return Ok(false);
        }

        tokio::fs::write(&chunk_path, serde_json::to_string_pretty(&chunk)?).await?;

        Ok(true)
    }

    async fn get_head_block(&self) -> Result<Option<Block>, Self::Error> {
        let mut chunks = tokio::fs::read_dir(&self.folder).await?;
        let mut head_chunk = None;

        // Search for the lowest chunk number.
        while let Some(entry) = chunks.next_entry().await? {
            let name = entry.file_name()
                .to_string_lossy()
                .to_string();

            if let Some(tail) = name.strip_prefix("chunk-") {
                if let Some(number) = tail.strip_suffix(".json") {
                    if let Ok(number) = number.parse::<u64>() {
                        head_chunk = match head_chunk {
                            Some(head_chunk) if number < head_chunk => Some(number),
                            None => Some(number),
                            _ => head_chunk
                        };
                    }
                }
            }
        }

        // Return None if no chunks found.
        let Some(head_chunk) = head_chunk else {
            return Ok(None);
        };

        // Read the first chunk file.
        let head_chunk = self.folder.join(format!("chunk-{head_chunk}.json"));

        let chunk = tokio::fs::read(&head_chunk).await?;
        let chunk = serde_json::from_slice::<HashSet<Json>>(&chunk)?;

        // Search for the block with lowest number.
        let block = chunk.iter()
            .flat_map(Block::from_json)
            .min_by(|a, b| {
                a.number().cmp(&b.number())
            });

        Ok(block)
    }

    async fn get_tail_block(&self) -> Result<Option<Block>, Self::Error> {
        // Lookup the head block.
        let Some(mut tail_block) = self.get_head_block().await? else {
            return Ok(None);
        };

        // Load the latest cached tail block number.
        let mut tail_block_number = self.tail_block.load(Ordering::Relaxed);

        match tail_block_number.cmp(&tail_block.number()) {
            // Set it to the head block's number if it's lower that it.
            std::cmp::Ordering::Less => {
                tail_block_number = tail_block.number();
            }

            // Otherwise, if the cached value is greated than the head block's
            // number - try to lookup this block and if failed - replace it
            // back to the head block's number.
            std::cmp::Ordering::Greater => {
                match self.get_block(tail_block_number).await? {
                    Some(block) => tail_block = block,
                    None => tail_block_number = tail_block.number()
                }
            }

            _ => ()
        }

        // Force-update the stored tail block's number in case above happened.
        self.tail_block.store(tail_block_number, Ordering::Relaxed);

        let mut chunk_number = tail_block_number / self.chunk_size;

        // Go through all the following chunks.
        loop {
            // Build path to the chunk file.
            let chunk_path = self.folder.join(format!("chunk-{chunk_number}.json"));

            // Stop the search if this file doesn't exist.
            if !chunk_path.exists() {
                break;
            }

            // Read the tail block's chunk.
            let chunk = tokio::fs::read(&chunk_path).await?;
            let chunk = serde_json::from_slice::<HashSet<Json>>(&chunk)?;

            // List all the blocks from this chunk.
            let mut blocks = chunk.iter()
                .flat_map(Block::from_json)
                .filter(|block| block.number() > tail_block_number)
                .collect::<Vec<_>>();

            // Sort them in ascending order.
            blocks.sort_by_key(|block| block.number());

            // Iterate over the blocks in chunk.
            for block in blocks {
                // If it's connected to the tail block - update the tail.
                if block.previous_block() == Some(tail_block.get_hash()) {
                    tail_block = block;
                    tail_block_number = tail_block.number();
                }

                // Otherwise return currently stored tail block.
                else {
                    self.tail_block.store(tail_block_number, Ordering::Release);

                    return Ok(Some(tail_block));
                }
            }

            chunk_number += 1;
        }

        self.tail_block.store(tail_block_number, Ordering::Release);

        Ok(Some(tail_block))
    }

    async fn is_empty(&self) -> Result<bool, Self::Error> {
        let has_entries = tokio::fs::read_dir(&self.folder).await?
            .next_entry().await?
            .is_some();

        Ok(!has_entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn index() -> Result<(), ChunkedBlocksIndexError> {
        use hyperborealib::crypto::asymmetric::SecretKey;

        use crate::block::prelude::*;

        let path = std::env::temp_dir()
            .join(".hyperchain.chunked-blocks-test");

        if path.exists() {
            tokio::fs::remove_dir_all(&path).await?;
        }

        // Prepare blocks
        let validator = SecretKey::random();

        let block_a = BlockBuilder::build_root(&validator);
        let block_b = BlockBuilder::chained(&block_a).sign(&validator);
        let block_c = BlockBuilder::chained(&block_b).sign(&validator);
        let block_d = BlockBuilder::chained(&block_c).sign(&validator);

        // Run the tests
        let index = ChunkedBlocksIndex::open(path, 2).await?;

        assert!(index.get_block(0).await?.is_none());
        assert!(index.get_block(1).await?.is_none());
        assert!(index.get_block(2).await?.is_none());

        assert!(index.get_head_block().await?.is_none());
        assert!(index.get_tail_block().await?.is_none());

        // Push A
        assert!(index.insert_block(block_a.clone()).await?);

        assert_eq!(index.get_block(0).await?, Some(block_a.clone()));
        assert!(index.get_block(1).await?.is_none());
        assert!(index.get_block(2).await?.is_none());

        assert_eq!(index.get_head_block().await?, Some(block_a.clone()));
        assert_eq!(index.get_tail_block().await?, Some(block_a.clone()));

        assert!(index.get_next_block(&block_a).await?.is_none());

        // Push C
        assert!(index.insert_block(block_c.clone()).await?);

        assert_eq!(index.get_block(0).await?, Some(block_a.clone()));
        assert!(index.get_block(1).await?.is_none());
        assert_eq!(index.get_block(2).await?, Some(block_c.clone()));

        assert_eq!(index.get_head_block().await?, Some(block_a.clone()));
        assert_eq!(index.get_tail_block().await?, Some(block_a.clone()));

        // Push B
        assert!(index.insert_block(block_b.clone()).await?);

        assert_eq!(index.get_block(0).await?, Some(block_a.clone()));
        assert_eq!(index.get_block(1).await?, Some(block_b.clone()));
        assert_eq!(index.get_block(2).await?, Some(block_c.clone()));

        assert_eq!(index.get_head_block().await?, Some(block_a.clone()));
        assert_eq!(index.get_tail_block().await?, Some(block_c.clone()));

        // Push D
        assert!(index.insert_block(block_d.clone()).await?);

        assert_eq!(index.get_block(0).await?, Some(block_a.clone()));
        assert_eq!(index.get_block(1).await?, Some(block_b.clone()));
        assert_eq!(index.get_block(2).await?, Some(block_c.clone()));
        assert_eq!(index.get_block(3).await?, Some(block_d.clone()));

        assert_eq!(index.get_head_block().await?, Some(block_a.clone()));
        assert_eq!(index.get_tail_block().await?, Some(block_d.clone()));

        assert_eq!(index.get_next_block(&block_a).await?, Some(block_b.clone()));
        assert_eq!(index.get_next_block(&block_b).await?, Some(block_c.clone()));
        assert_eq!(index.get_next_block(&block_c).await?, Some(block_d.clone()));
        assert!(index.get_next_block(&block_d).await?.is_none());

        Ok(())
    }
}
